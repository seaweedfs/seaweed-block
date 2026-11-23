//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Single-volume coverage, gap 1 (cell A2.4, V1-SPEC r2.55): kill -9 of the
// holder WITH guest I/O in flight, then a restart inside the lease. Every
// write a Flush acknowledged must read back after the restart
// (INV-WAL-SYNCED-RECOVERED-001); writes not yet acknowledged by a Flush may
// vanish, but what is left of them must be a prefix of the stream in LBA
// order with nothing torn or invented (the writer writes LBAs in order).
//
// The writer is the guest: qd1 NVMe/TCP writes of distinct blocks, a Flush
// every crashIOFlushEvery writes; it records the last LBA a successful
// Flush covered. The holder is killed while the writer is mid-stream.

const (
	crashIOBase       = 2048 // first LBA the writer uses (chunk 2)
	crashIOSpan       = 40000
	crashIOFlushEvery = 8
)

func crashIOBlock(lba uint64) []byte {
	s := fmt.Sprintf("crash-io lba %08d|", lba)
	b := make([]byte, 4096)
	for i := range b {
		b[i] = s[i%len(s)]
	}
	return b
}

// crashIOWriter writes LBAs crashIOBase.. in order until stopped or an I/O
// fails, and reports how far the Flush acknowledgements reached.
type crashIOWriter struct {
	mu      sync.Mutex
	written int // writes completed (acknowledged by the write itself)
	flushed int // writes covered by an acknowledged Flush
	err     error
	done    chan struct{}
}

func startCrashIOWriter(t *testing.T, env *handoffEnv, id string) *crashIOWriter {
	t.Helper()
	w := &crashIOWriter{done: make(chan struct{})}
	c := dialNVMe(t, env.hosts[id].nvme, plannedHandoffNQN)
	go func() {
		defer close(w.done)
		defer c.close()
		for n := 0; n < crashIOSpan; n++ {
			lba := crashIOBase + uint64(n)
			err := c.tryWrite(lba, crashIOBlock(lba))
			if err == nil && n%crashIOFlushEvery == crashIOFlushEvery-1 {
				err = c.tryFlush()
				if err == nil {
					w.mu.Lock()
					w.flushed = n + 1
					w.mu.Unlock()
				}
			}
			w.mu.Lock()
			if err != nil {
				w.err = err
			} else {
				w.written = n + 1
			}
			w.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	return w
}

func (w *crashIOWriter) counts() (written, flushed int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written, w.flushed, w.err
}

func TestProcedureCrashWithIOInFlight(t *testing.T) {
	env := startHandoffEnv(t, 1)
	first := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, first, "s1")

	w := startCrashIOWriter(t, env, "s1")
	// Let the stream run until a good number of Flushes acknowledged, so the
	// kill lands mid-stream with writes in flight.
	poll(t, 60*time.Second, "the writer never got 2000 flushed writes", func() bool {
		_, flushed, err := w.counts()
		return flushed >= 2000 || err != nil
	})
	if _, _, err := w.counts(); err != nil {
		t.Fatalf("a write failed before the kill: %v", err)
	}
	first.kill(t) // SIGKILL while the writer is still writing
	select {
	case <-w.done:
	case <-time.After(70 * time.Second):
		t.Fatal("the writer did not stop after the holder was killed")
	}
	written, flushed, werr := w.counts()
	if werr == nil {
		t.Fatalf("the writer finished all %d writes before the kill: no I/O was in flight", crashIOSpan)
	}
	t.Logf("at the kill: %d writes completed, %d covered by an acknowledged Flush; the writer's error: %v", written, flushed, werr)

	again := env.start(t, "blockvolume-s1-again", "s1")
	if got := env.adopt(t, again, "s1"); got != e {
		t.Fatalf("the restarted holder adopted epoch %d; want %d", got, e)
	}

	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	defer c.close()
	zero := make([]byte, 4096)
	for n := 0; n < flushed; n++ {
		lba := crashIOBase + uint64(n)
		if got := c.read(t, lba, 4096); !bytes.Equal(got, crashIOBlock(lba)) {
			t.Fatalf("flush-acknowledged write %d (LBA %d) is lost or wrong after kill -9 and restart", n, lba)
		}
	}
	// Past the Flush acknowledgements: a prefix of the stream, then zeros.
	kept, ended := flushed, false
	for n := flushed; n < written+crashIOFlushEvery && n < crashIOSpan; n++ {
		lba := crashIOBase + uint64(n)
		got := c.read(t, lba, 4096)
		switch {
		case !ended && bytes.Equal(got, crashIOBlock(lba)):
			kept++
		case bytes.Equal(got, zero):
			ended = true
		default:
			t.Fatalf("LBA %d after the last acknowledged Flush holds data out of the stream's order or torn (kept %d)", lba, kept)
		}
	}
	t.Logf("after the restart: all %d flush-acknowledged writes read back; %d of the %d unflushed completed writes survived as a prefix", flushed, kept-flushed, written-flushed)
}
