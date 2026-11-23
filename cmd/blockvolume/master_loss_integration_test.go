//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Coverage cells A2.24, A2.27 and A2.35: the holder loses its master while it
// is alive, two ways: its connections are closed (renewals fail at once), or
// they stay open and every byte is dropped (each renewal hangs until its own
// 2 s timeout: the guard's stalled-renewal path). Either way the holder exits
// 75 for lease_expired before a forced move is admitted, and the target then
// serves the cut block. Administrative calls still reach the real master.
func TestProcedureMasterUnreachableFencesBeforeForcedMove(t *testing.T) {
	t.Run("connections closed", func(t *testing.T) { masterLoss(t, false) })
	t.Run("silent drop", func(t *testing.T) { masterLoss(t, true) })
}

func masterLoss(t *testing.T, silent bool) {
	env := startHandoffEnv(t, 2)
	proxy, disconnect, drop := holderMasterProxy(t, env.h.masterAddr)
	args := env.volArgs("s1")
	for i := range args {
		if args[i] == "--master" {
			args[i+1] = proxy
		}
	}
	old := env.h.start(t, "blockvolume-s1-partition", env.h.volume, args...)
	e := env.adopt(t, old, "s1")
	block := pattern("master-loss-kept")
	env.writeBlock(t, "s1", 0, block)
	// Install a known remote point before losing the master, so the target's
	// read proves data preservation without promising unpublished WAL data.
	env.h.command(t, "ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status, "--timeout", "120s")
	env.requireHeadHolder(t, e, "s1")
	if silent {
		drop()
	} else {
		disconnect()
	}
	b := env.hosts["s2"]
	move := []string{"ops", "workspace", "force-move", "--master", env.h.masterAddr, "--volume", "v1",
		"--target-host", b.id, "--target-data", b.data, "--target-ctrl", b.ctrl, "--request-id", "master-loss-move",
		"--confirm-forced", fmt.Sprintf("v1@%d", e)}
	refusedWhileAlive := false
	poll(t, 90*time.Second, "forced move never admitted after holder lost master", func() bool {
		exitedBeforePreview := false
		select {
		case <-old.done:
			exitedBeforePreview = true
		default:
		}
		out, _ := env.h.command(t, append(append([]string{}, move...), "--dry-run")...)
		if previewAdmitted(out) {
			if !exitedBeforePreview {
				t.Fatal("forced move admitted before the live holder's exit was observed")
			}
			return true
		}
		if !exitedBeforePreview {
			refusedWhileAlive = true
		}
		return false
	})
	if !refusedWhileAlive {
		t.Fatal("no forced-move refusal observed while the disconnected holder was alive")
	}
	if err := old.wait(time.Second); exitCode(err) != 75 {
		t.Fatalf("master-lost holder exited %v; want75", err)
	}
	if !old.has("reason=lease_expired") {
		t.Fatal("holder exit lacks affirmative lease_expired reason")
	}
	if silent {
		// The stalled-renewal path: a renewal ran to its own timeout, then the
		// guard fenced, then the process exited for the expired lease.
		requireLogOrder(t, old, "the call reached the 2s renewal timeout", "workspace lease: fenced", "reason=lease_expired")
	}
	if listening(env.hosts["s1"].nvme) {
		t.Fatal("exited holder still has a frontend listener")
	}
	out, _ := env.h.command(t, move...)
	_, mustU := fatalOn(t)
	e2 := mustU(forcedEpoch(out, e, "s1", "s2"))
	nw := env.start(t, "blockvolume-s2-after-partition", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("target epoch%d; want%d", got, e2)
	}
	if !bytes.Equal(env.readBlock(t, "s2", 0), block) {
		t.Fatal("target lost the confirmed-cut block")
	}
}

// requireLogOrder requires each of lines to appear in d's log, each after the
// one before it.
func requireLogOrder(t *testing.T, d *daemon, lines ...string) {
	t.Helper()
	data, _ := os.ReadFile(d.log)
	at := 0
	for _, l := range lines {
		i := strings.Index(string(data)[at:], l)
		if i < 0 {
			t.Fatalf("%s: %q missing, or not after the lines before it (%q)", d.name, l, lines)
		}
		at += i + len(l)
	}
}

// holderMasterProxy forwards real TCP bytes until disconnect closes every
// accepted connection and the listener, or until drop makes it a silent
// black hole: every connection, open or new, stays open and its bytes are
// read and discarded in both directions, so a call sent through it gets no
// answer until its own timeout. Closing a port or dropping bytes is the
// injected failure, never the evidence used to pass the lease/exit
// assertion.
func holderMasterProxy(t *testing.T, target string) (string, func(), func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var once sync.Once
	closed := false
	var dropping atomic.Bool
	conns := map[net.Conn]bool{}
	// forward copies src to dst until src ends, discarding the bytes while
	// dropping.
	forward := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 && !dropping.Load() {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	acceptDone := make(chan struct{})
	stop := func() {
		once.Do(func() {
			mu.Lock()
			closed = true
			_ = ln.Close()
			for c := range conns {
				_ = c.Close()
			}
			mu.Unlock()
		})
	}
	t.Cleanup(func() {
		stop()
		select {
		case <-acceptDone:
		case <-time.After(5 * time.Second):
			t.Error("proxy accept did not end")
		}
		joined := make(chan struct{})
		go func() { wg.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("proxy connections did not end")
		}
	})
	go func() {
		defer close(acceptDone)
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				_ = client.Close()
				continue
			}
			conns[client] = true
			wg.Add(1)
			mu.Unlock()
			go func() {
				defer wg.Done()
				defer func() { mu.Lock(); delete(conns, client); mu.Unlock(); _ = client.Close() }()
				if dropping.Load() { // a new connection into the black hole: held open, never answered
					_, _ = io.Copy(io.Discard, client)
					return
				}
				server, err := net.DialTimeout("tcp", target, 2*time.Second)
				if err != nil {
					return
				}
				mu.Lock()
				if closed {
					mu.Unlock()
					_ = server.Close()
					return
				}
				conns[server] = true
				mu.Unlock()
				defer func() { mu.Lock(); delete(conns, server); mu.Unlock(); _ = server.Close() }()
				copied := make(chan struct{})
				go func() { forward(server, client); _ = server.Close(); close(copied) }()
				forward(client, server)
				_ = client.Close()
				_ = server.Close()
				<-copied
			}()
		}
	}()
	return ln.Addr().String(), stop, func() { dropping.Store(true) }
}
