//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Coverage cells A2.24, A2.27 and A2.35: real master dial failure while
// the holder is alive. Administrative calls still reach the real master.
func TestProcedureMasterUnreachableFencesBeforeForcedMove(t *testing.T) {
	env := startHandoffEnv(t, 2)
	proxy, disconnect := holderMasterProxy(t, env.h.masterAddr)
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
	disconnect()
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

// holderMasterProxy forwards real TCP bytes until disconnect closes every
// accepted connection and the listener. Closing a port is the injected
// failure, never the evidence used to pass the lease/exit assertion.
func holderMasterProxy(t *testing.T, target string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var once sync.Once
	closed := false
	conns := map[net.Conn]bool{}
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
				go func() { _, _ = io.Copy(server, client); _ = server.Close(); close(copied) }()
				_, _ = io.Copy(client, server)
				_ = client.Close()
				_ = server.Close()
				<-copied
			}()
		}
	}()
	return ln.Addr().String(), stop
}
