//go:build subprocess && chunkstore_integration

package main_test

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Sibling volumes for the 1/2/3-volume cells of the must-pass table: every
// scenario runs with 1, 2 and 3 volumes. SWBLOCK_TEST_VOLUMES=N (1, 2 or 3;
// default 1, no change) makes startHandoffEnvConfigured add N-1 sibling
// volumes v1s2..v1sN beside the test's own volumes, each on a node of its own
// (own blockvolume process, endpoints and local root), created by the cluster
// spec at SWBLOCK_TEST_VOLUME_BYTES (default 100 MiB). Each sibling runs a
// seeded writer for the whole test: every block is written once, and a write
// whose write and flush both succeeded is journaled (LBA, sha256). Before the
// harness stops any of its processes (beforeStop), every journaled
// block must read back with the same bytes; a write that failed is ambiguous
// and is counted, never checked. The cell passes only if the test passes AND
// every sibling reads back every acknowledged write. The test's own volumes
// and assertions are unchanged.

const (
	testVolumesEnv      = "SWBLOCK_TEST_VOLUMES"
	testVolumeBytesEnv  = "SWBLOCK_TEST_VOLUME_BYTES"
	siblingDefaultBytes = 100 << 20
	siblingWriteEvery   = 200 * time.Millisecond
	siblingRedialAfter  = 40 * time.Second
)

// siblingVolumes returns the sibling names for SWBLOCK_TEST_VOLUMES (none
// when unset or 1).
func siblingVolumes(t testing.TB) []string {
	t.Helper()
	v := os.Getenv(testVolumesEnv)
	if v == "" {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 3 {
		t.Fatalf("%s=%q: want 1, 2 or 3", testVolumesEnv, v)
	}
	var out []string
	for i := 2; i <= n; i++ {
		out = append(out, fmt.Sprintf("v1s%d", i))
	}
	return out
}

// siblingVolumeBytes is SWBLOCK_TEST_VOLUME_BYTES: a positive multiple of
// 4 MiB (whole chunks), 100 MiB by default.
func siblingVolumeBytes(t testing.TB) uint64 {
	t.Helper()
	v := os.Getenv(testVolumeBytesEnv)
	if v == "" {
		return siblingDefaultBytes
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 || n%(4<<20) != 0 {
		t.Fatalf("%s=%q: want a positive multiple of 4 MiB", testVolumeBytesEnv, v)
	}
	return n
}

// siblingBlock is the seeded content of a sibling's block lba.
func siblingBlock(volume string, lba uint64) []byte {
	out := make([]byte, 0, 4096)
	var seed [8]byte
	for i := uint64(0); len(out) < 4096; i++ {
		binary.LittleEndian.PutUint64(seed[:], lba<<16|i)
		sum := sha256.Sum256(append([]byte(volume+"/"), seed[:]...))
		out = append(out, sum[:]...)
	}
	return out[:4096]
}

// softTB turns the NVMe helpers' Fatalf into an error the background writer
// recovers (FailNow must not be called off the test goroutine).
type softTB struct{ testing.TB }

type softFailure struct{ msg string }

func (softTB) Helper()                           {}
func (softTB) Fatalf(format string, args ...any) { panic(softFailure{fmt.Sprintf(format, args...)}) }
func (softTB) Fatal(args ...any)                 { panic(softFailure{fmt.Sprint(args...)}) }

func softly(t testing.TB, fn func(testing.TB)) (err error) {
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(softFailure)
			if !ok {
				panic(r)
			}
			err = errors.New(f.msg)
		}
	}()
	fn(softTB{t})
	return nil
}

type siblingWriter struct {
	volume, nvme, status string
	blocks               uint64
	stop, done           chan struct{}
	// d is the sibling's blockvolume and args its command, to restart it
	// for the read-back if it exited during the test (a fenced lease).
	d    *daemon
	args []string

	mu      sync.Mutex
	acked   map[uint64][32]byte
	unknown int
	lastErr string
}

func (w *siblingWriter) run(t testing.TB) {
	defer close(w.done)
	var c *nvmeInitiator
	var dialed time.Time
	defer func() {
		if c != nil {
			c.close()
		}
	}()
	for lba := uint64(0); lba < w.blocks; lba++ {
		select {
		case <-w.stop:
			return
		case <-time.After(siblingWriteEvery):
		}
		// dialNVMe gives each connection an absolute 60 s deadline: redial
		// well before it, so a failed write is the product's, not the
		// connection's age.
		if c != nil && time.Since(dialed) > siblingRedialAfter {
			c.close()
			c = nil
		}
		data := siblingBlock(w.volume, lba)
		err := softly(t, func(st testing.TB) {
			if c == nil {
				c, dialed = dialNVMe(st, w.nvme, plannedHandoffNQN), time.Now()
			}
			c.write(st, lba, data)
			c.flush(st)
		})
		w.mu.Lock()
		if err == nil {
			w.acked[lba] = sha256.Sum256(data)
		} else {
			w.unknown++
			w.lastErr = err.Error()
		}
		w.mu.Unlock()
		if err != nil && c != nil {
			c.close()
			c = nil
		}
	}
}

// startSiblings starts each sibling's blockvolume, waits until it serves as
// primary, starts its writer, and registers the read-back as a harness
// beforeStop hook, which runs before any harness process is stopped.
func (env *handoffEnv) startSiblings(t *testing.T, names []string) {
	t.Helper()
	if len(names) == 0 {
		return
	}
	bytes := siblingVolumeBytes(t)
	_, mustU := fatalOn(t)
	var writers []*siblingWriter
	for _, v := range names {
		b := env.hosts["n-"+v]
		args := append(env.volArgsFor(b), "--durable-blocks", fmt.Sprint(bytes/4096))
		d := env.h.start(t, "blockvolume-"+v, env.h.volume, args...)
		e := mustU(adoptedEpochOf(v, d.waitLog(t, 60*time.Second, "blockvolume: volume "+v+" adopted as PRIMARY at r1@")))
		waitSiblingPrimary(t, b.status, v, e)
		w := &siblingWriter{volume: v, nvme: b.nvme, status: b.status, blocks: bytes / 4096, stop: make(chan struct{}), done: make(chan struct{}),
			d: d, args: args, acked: map[uint64][32]byte{}}
		writers = append(writers, w)
		go w.run(t)
	}
	siblingWritersOf.Store(env, writers)
	// The read-back runs before any harness process stops (beforeStop), not
	// as an ordinary cleanup: a process the test starts after the siblings (a
	// restarted master) registers its cleanup later, so it would stop first
	// and the siblings, their master gone, would fence before being read.
	env.h.beforeStop = append(env.h.beforeStop, func() {
		for _, w := range writers {
			close(w.stop)
		}
		for _, w := range writers {
			select {
			case <-w.done:
			case <-time.After(90 * time.Second):
				t.Errorf("sibling %s: the writer did not stop within 90 s", w.volume)
				continue
			}
			if !env.siblingServing(t, w) {
				continue
			}
			env.readBackSibling(t, w)
		}
	})
}

// siblingServing makes sure the sibling serves before its read-back. A
// sibling whose process exited during the test (a test that keeps the master
// away past the lease fences every volume, which then exits by design) is
// restarted with the same command and local root and must come back as
// primary; its acknowledged writes are then read through the restarted
// process. Reports false (with an error) when it cannot serve.
func (env *handoffEnv) siblingServing(t *testing.T, w *siblingWriter) bool {
	t.Helper()
	select {
	case <-w.d.done:
	default:
		return true
	}
	why := "exit " + fmt.Sprint(w.d.err)
	if l := w.d.line("blockvolume: exiting"); l != "" {
		why = l
	}
	t.Logf("SIBLING-RESTART %s: exited during the test (%s); restarting it for the read-back", w.volume, why)
	w.d = env.h.start(t, "blockvolume-"+w.volume+"-readback", env.h.volume, w.args...)
	l, err := w.d.waitLogErr(60*time.Second, "blockvolume: volume "+w.volume+" adopted as PRIMARY at r1@")
	if err != nil {
		t.Errorf("sibling %s: restarted for the read-back but did not serve: %v", w.volume, err)
		return false
	}
	e, err := adoptedEpochOf(w.volume, l)
	if err != nil {
		t.Errorf("sibling %s: restarted for the read-back: %v", w.volume, err)
		return false
	}
	waitSiblingPrimary(t, w.status, w.volume, e)
	return true
}

// siblingWritersOf maps an env to its siblings' writers, for a test that
// checks the siblings kept serving while it did something else.
var siblingWritersOf sync.Map // *handoffEnv -> []*siblingWriter

// siblingWriteFailures is the number of sibling writes that failed so far,
// per sibling volume, and the volumes' names in order.
func (env *handoffEnv) siblingWriteFailures() (map[string]int, []string) {
	v, _ := siblingWritersOf.Load(env)
	ws, _ := v.([]*siblingWriter)
	out, names := map[string]int{}, []string(nil)
	for _, w := range ws {
		w.mu.Lock()
		out[w.volume] = w.unknown
		w.mu.Unlock()
		names = append(names, w.volume)
	}
	return out, names
}

// waitLogErr is waitLog that returns an error instead of failing the test.
func (d *daemon) waitLogErr(timeout time.Duration, s string) (string, error) {
	end := time.Now().Add(timeout)
	for {
		if l := d.line(s); l != "" {
			return l, nil
		}
		select {
		case <-d.done:
			if l := d.line(s); l != "" {
				return l, nil
			}
			return "", fmt.Errorf("%s exited (%v) before logging %q", d.name, d.err, s)
		default:
		}
		if time.Now().After(end) {
			return "", fmt.Errorf("%s did not log %q within %s", d.name, s, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitSiblingPrimary is waitPrimaryAt for a named volume.
func waitSiblingPrimary(t *testing.T, statusAddr, volume string, epoch uint64) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	poll(t, 60*time.Second, fmt.Sprintf("status at %s never showed %s as a healthy primary at epoch %d", statusAddr, volume, epoch), func() bool {
		resp, err := c.Get("http://" + statusAddr + "/status?volume=" + volume)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return statusPrimary(b, epoch)
	})
}

// readBackSibling checks every acknowledged block of one sibling.
func (env *handoffEnv) readBackSibling(t *testing.T, w *siblingWriter) {
	t.Helper()
	w.mu.Lock()
	acked, unknown, lastErr := w.acked, w.unknown, w.lastErr
	w.mu.Unlock()
	bad, read := 0, 0
	err := softly(t, func(st testing.TB) {
		c, dialed := dialNVMe(st, w.nvme, plannedHandoffNQN), time.Now()
		defer func() { c.close() }()
		for lba, want := range acked {
			if time.Since(dialed) > siblingRedialAfter {
				c.close()
				c, dialed = dialNVMe(st, w.nvme, plannedHandoffNQN), time.Now()
			}
			if sha256.Sum256(c.read(st, lba, 4096)) != want {
				bad++
			}
			read++
		}
	})
	t.Logf("SIBLING %s acked=%d unknown=%d read_back=%d mismatched=%d last_error=%q", w.volume, len(acked), unknown, read, bad, lastErr)
	switch {
	case err != nil:
		t.Errorf("sibling %s: read-back failed after %d of %d blocks: %v", w.volume, read, len(acked), err)
	case len(acked) == 0:
		t.Errorf("sibling %s: no write was acknowledged (%d failed, last: %s)", w.volume, unknown, lastErr)
	case bad > 0:
		t.Errorf("sibling %s: %d of %d acknowledged blocks read back other bytes", w.volume, bad, len(acked))
	}
}
