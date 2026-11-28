//go:build subprocess && chunkstore_integration

package main_test

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Controls of the sibling-volume harness itself (no product process).

// TestDialNVMeClosesItsSocketOnAFailedHandshake: the server answers the
// initialization request with a PDU of the wrong type, so dialNVMe fails
// after it opened its socket. The failure (a recovered softTB panic, as the
// sibling writer sees it) must close that socket: the server reads EOF.
func TestDialNVMeClosesItsSocketOnAFailedHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	eof := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			eof <- err
			return
		}
		defer conn.Close()
		req := make([]byte, 128) // the client's ICReq PDU
		if _, err := io.ReadFull(conn, req); err != nil {
			eof <- err
			return
		}
		// A valid common header of type 0x5 (not ICResp 0x1): hlen 8, pdo 0, plen 8.
		if _, err := conn.Write([]byte{0x5, 0, 8, 0, 8, 0, 0, 0}); err != nil {
			eof <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		eof <- err
	}()
	err = softly(t, func(st testing.TB) { dialNVMe(st, ln.Addr().String(), plannedHandoffNQN) })
	if err == nil {
		t.Fatal("dialNVMe against a wrong-type ICResp succeeded")
	}
	select {
	case got := <-eof:
		if got != io.EOF {
			t.Fatalf("server side after the failed handshake: %v; want EOF (the client closed its socket)", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the client never closed its socket after the failed handshake")
	}
}

// deadAddress is a TCP address nothing listens on.
func deadAddress(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := ln.Addr().String()
	ln.Close()
	return a
}

func failingWriter(t *testing.T, v string) *siblingWriter {
	w := &siblingWriter{volume: v, nvme: deadAddress(t), blocks: 1 << 20, stop: make(chan struct{}), done: make(chan struct{}), acked: map[uint64][32]byte{}}
	go w.run(t)
	return w
}

// TestSiblingTeardownOfAPartialStart: a sibling start that did not complete
// (a later sibling's start failed) still stops and joins every writer that
// was running, and judges none of them; a complete start judges each one.
func TestSiblingTeardownOfAPartialStart(t *testing.T) {
	env := &handoffEnv{}
	for _, admitted := range []bool{false, true} {
		ws := []*siblingWriter{failingWriter(t, "v1s2"), failingWriter(t, "v1s3")}
		time.Sleep(3 * siblingWriteEvery) // both are retrying against a dead address
		judged := 0
		start := time.Now()
		env.siblingTeardown(t, ws, admitted, func(testing.TB, *siblingWriter) { judged++ })
		for _, w := range ws {
			select {
			case <-w.done:
			default:
				t.Fatalf("admitted=%v: writer %s still running after the teardown", admitted, w.volume)
			}
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("admitted=%v: teardown took %v", admitted, d)
		}
		if want := map[bool]int{false: 0, true: 2}[admitted]; judged != want {
			t.Fatalf("admitted=%v: %d writers judged; want %d", admitted, judged, want)
		}
		if ws[0].unknown == 0 {
			t.Fatal("control: the writer against a dead address recorded no failed write")
		}
	}
}

func TestExpectedSiblingExit(t *testing.T) {
	for line, want := range map[string]bool{
		"blockvolume: exiting status=75 reason=lease_expired":          true,
		"blockvolume: exiting status=75 reason=revoke_ack_unconfirmed": true,
		"blockvolume: exiting status=75 reason=assignment_timeout":     false,
		"blockvolume: exiting status=78 reason=deleted":                false,
		"blockvolume: exiting status=2 reason=lease_expired":           false,
		"exit signal: killed": false,
		"exit exit status 2":  false,
	} {
		if got := expectedSiblingExit(line); got != want {
			t.Errorf("expectedSiblingExit(%q) = %v; want %v", line, got, want)
		}
	}
}

func TestSiblingEarlyFailures(t *testing.T) {
	t0 := time.Date(2026, 11, 27, 4, 0, 0, 0, time.Local)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	for _, c := range []struct {
		name     string
		failed   []time.Time
		exited   time.Time
		fenced   time.Time
		isFenced bool
		want     int
	}{
		{"no failure", nil, at(10), at(8), true, 0},
		{"all after the stamp's second", []time.Time{at(9), at(10)}, at(12), at(8), true, 0},
		{"in the second before the stamp", []time.Time{at(7).Add(500 * time.Millisecond)}, at(12), at(8), true, 1},
		{"at the stamp", []time.Time{at(8)}, at(12), at(8), true, 1},
		{"in the stamp's second, before an actual fence at 8.9", []time.Time{at(8).Add(100 * time.Millisecond)}, at(12), at(8), true, 1},
		{"one before the fence", []time.Time{at(5), at(9)}, at(12), at(8), true, 1},
		{"exit inside the stamp's second", []time.Time{at(8).Add(600 * time.Millisecond)}, at(8).Add(500 * time.Millisecond), at(8), true, 0},
		{"no fence line: from the exit", []time.Time{at(5), at(13)}, at(12), time.Time{}, false, 1},
		{"neither known: all early", []time.Time{at(5)}, time.Time{}, time.Time{}, false, 1},
	} {
		if got := siblingEarlyFailures(c.failed, c.exited, c.fenced, c.isFenced); got != c.want {
			t.Errorf("%s: %d early; want %d", c.name, got, c.want)
		}
	}
}

// TestSiblingTeardownRegisteredBeforeStart drives startSiblings itself: the
// second sibling's start fails (t.Fatalf, so in a child test process), and
// the first sibling's writer must still be stopped and joined by the
// harness's teardown, which runs from the daemon cleanup registered before
// any sibling. Registering the teardown after the start loop fails this.
func TestSiblingTeardownRegisteredBeforeStart(t *testing.T) {
	if os.Getenv("SWBLOCK_PARTIAL_START_CHILD") == "1" {
		h := &plannedHandoffHarness{dir: t.TempDir(), t: t}
		env := &handoffEnv{h: h}
		t.Cleanup(h.runBeforeStop) // the daemon cleanup, registered before the siblings
		siblingStartForTest = func(t *testing.T, v string) *siblingWriter {
			if v == "v1s3" {
				t.Fatalf("injected start failure of %s", v)
			}
			w := &siblingWriter{volume: v, stop: make(chan struct{}), done: make(chan struct{})}
			go func() {
				<-w.stop
				fmt.Println("FAKE-WRITER-JOINED", v)
				close(w.done)
			}()
			return w
		}
		env.startSiblings(t, []string{"v1s2", "v1s3"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run", "^TestSiblingTeardownRegisteredBeforeStart$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "SWBLOCK_PARTIAL_START_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the child passed; want its injected start failure:\n%s", out)
	}
	for _, want := range []string{"injected start failure of v1s3", "FAKE-WRITER-JOINED v1s2", "SIBLING-TEARDOWN 1 writer(s) stopped"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("the child's output lacks %q:\n%s", want, out)
		}
	}
}
