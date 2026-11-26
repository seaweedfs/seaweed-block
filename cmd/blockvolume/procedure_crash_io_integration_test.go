//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// Guest I/O crash recovery: kill -9 of the
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
	t.Logf("after the restart: all %d flush-acknowledged writes read back; %d writes past the last acknowledged Flush survived as a prefix (%d had completed; one in flight may persist without its reply)", flushed, kept-flushed, written-flushed)
}

// Gap 1, cell A3.5: kill -9 of a holder serving on a FRESH root (after a
// clean move) while chunk fills from HEAD are in flight. s1 writes one block
// in each of the first fillChunks chunks; after the hand-over, s2 starts
// with no local copy. A reader walks those chunks (each read misses and
// fills) while a writer streams Flush-acknowledged writes into chunks s1
// never wrote; s2 is SIGKILLed mid-walk and restarted. Every s1 block must
// read back (the fills are redone from HEAD) and every acknowledged s2 write
// must survive.
const (
	fillChunks     = 60
	fillWriterBase = 60 * ladderChunkBlocks
	fillWriterSpan = 4 * ladderChunkBlocks
	fillFlushEvery = 8
)

func fillSourceBlock(c int) []byte {
	s := fmt.Sprintf("fill-src chunk %03d|", c)
	b := make([]byte, 4096)
	for i := range b {
		b[i] = s[i%len(s)]
	}
	return b
}

func TestProcedureCrashOnAFreshRootWithFillsInFlight(t *testing.T) {
	env := startHandoffEnv(t, 2)
	must, mustU := fatalOn(t)
	h := env.h

	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	for c := 0; c < fillChunks; c++ {
		env.writeBlock(t, "s1", uint64(c*ladderChunkBlocks), fillSourceBlock(c))
	}
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	env.revoke(t, e, g)
	if err := old.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v", err)
	}
	env.waitAck(t, e, g)
	moved, _ := h.command(t, env.moveClean(e, "s2", "move-v1-1")...)
	e2 := mustU(movedEpochTo(moved, e, g, l, "s2"))
	nw := env.start(t, "blockvolume-s2", "s2")
	must(checkClaimDoneBy(nw.waitLog(t, 60*time.Second, "durable: HEAD claim for volume=v1 done in"), e2, "s2"))
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("s2 adopted at epoch %d; want %d", got, e2)
	}

	// The reader: read misses fill each source chunk from HEAD.
	var rmu sync.Mutex
	reads, readErr := 0, error(nil)
	readerDone := make(chan struct{})
	rc := dialNVMe(t, env.hosts["s2"].nvme, plannedHandoffNQN)
	go func() {
		defer close(readerDone)
		defer rc.close()
		for c := 0; c < fillChunks; c++ {
			_, err := rc.tryRead(uint64(c*ladderChunkBlocks), 4096)
			rmu.Lock()
			if err != nil {
				readErr = err
				rmu.Unlock()
				return
			}
			reads = c + 1
			rmu.Unlock()
		}
	}()
	// The writer: acknowledged local writes into chunks s1 never wrote.
	var wmu sync.Mutex
	written, flushed := 0, 0
	writerDone := make(chan struct{})
	wc := dialNVMe(t, env.hosts["s2"].nvme, plannedHandoffNQN)
	go func() {
		defer close(writerDone)
		defer wc.close()
		for n := 0; n < fillWriterSpan; n++ {
			lba := uint64(fillWriterBase + n)
			err := wc.tryWrite(lba, crashIOBlock(lba))
			if err == nil && n%fillFlushEvery == fillFlushEvery-1 {
				if err = wc.tryFlush(); err == nil {
					wmu.Lock()
					flushed = n + 1
					wmu.Unlock()
				}
			}
			if err != nil {
				return
			}
			wmu.Lock()
			written = n + 1
			wmu.Unlock()
		}
	}()
	poll(t, 60*time.Second, "s2 never got its first fills and acknowledged writes", func() bool {
		rmu.Lock()
		r := reads
		rmu.Unlock()
		wmu.Lock()
		f := flushed
		wmu.Unlock()
		return r >= 3 && f >= 8
	})
	nw.kill(t) // SIGKILL with fills and writes in flight
	<-readerDone
	<-writerDone
	rmu.Lock()
	readsAtKill, rerr := reads, readErr
	rmu.Unlock()
	wmu.Lock()
	writtenAtKill, flushedAtKill := written, flushed
	wmu.Unlock()
	if readsAtKill >= fillChunks {
		t.Fatalf("the reader finished all %d fills before the kill: no fill was in flight", fillChunks)
	}
	t.Logf("at the kill: %d of %d source chunks read (filled), reader error %v; %d writes completed, %d covered by a Flush", readsAtKill, fillChunks, rerr, writtenAtKill, flushedAtKill)

	again := env.start(t, "blockvolume-s2-again", "s2")
	if got := env.adopt(t, again, "s2"); got != e2 {
		t.Fatalf("the restarted s2 adopted epoch %d; want %d", got, e2)
	}
	c := dialNVMe(t, env.hosts["s2"].nvme, plannedHandoffNQN)
	defer c.close()
	for k := 0; k < fillChunks; k++ {
		if got := c.read(t, uint64(k*ladderChunkBlocks), 4096); !bytes.Equal(got, fillSourceBlock(k)) {
			t.Fatalf("source chunk %d (filled before the kill: %t) does not read back after the restart", k, k < readsAtKill)
		}
	}
	for n := 0; n < flushedAtKill; n++ {
		lba := uint64(fillWriterBase + n)
		if got := c.read(t, lba, 4096); !bytes.Equal(got, crashIOBlock(lba)) {
			t.Fatalf("flush-acknowledged write %d (LBA %d) on the fresh root is lost after kill -9 and restart", n, lba)
		}
	}
	t.Logf("after the restart: all %d source chunks and all %d flush-acknowledged writes read back", fillChunks, flushedAtKill)
}

// tryRead is read returning the error (for readers off the test goroutine).
func (c *nvmeInitiator) tryRead(lba uint64, n int) ([]byte, error) {
	cmd := c.ioCmd(0x02, lba, n/4096)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return nil, fmt.Errorf("nvme Read: %w", err)
	}
	var data []byte
	for {
		ch, err := c.r.Dequeue()
		if err != nil {
			return nil, fmt.Errorf("nvme Read: %w", err)
		}
		if ch.Type == 0x5 {
			var resp nvme.CapsuleResponse
			if err := c.r.Receive(&resp); err != nil {
				return nil, fmt.Errorf("nvme Read response: %w", err)
			}
			if resp.Status != 0 {
				return nil, fmt.Errorf("nvme Read status 0x%04x", resp.Status)
			}
			break
		}
		if ch.Type != 0x7 {
			return nil, fmt.Errorf("nvme Read: unexpected PDU type 0x%x", ch.Type)
		}
		var dh nvme.C2HDataHeader
		if err := c.r.Receive(&dh); err != nil {
			return nil, fmt.Errorf("nvme C2HData: %w", err)
		}
		part := make([]byte, c.r.Length())
		if err := c.r.ReceiveData(part); err != nil {
			return nil, fmt.Errorf("nvme C2HData payload: %w", err)
		}
		data = append(data, part...)
	}
	if len(data) != n {
		return nil, fmt.Errorf("nvme Read returned %d bytes; want %d", len(data), n)
	}
	return data, nil
}

// Gap 2, procedure part (cells P8.7, P9.4, P11.3): kill -9 of the holder
// during a cut's upload, with real binaries. Image A is cut; every block is
// rewritten and a second cut starts; the holder is SIGKILLed after a delay.
// The kill counts as mid-upload only if chunk objects of the second cut
// reached the filer while HEAD stayed at the first cut's generation; else the
// delay is adapted (shorter if the cut had committed, longer if nothing was
// uploaded) and the step repeats with a new image, up to cutCrashAttempts
// times. After the mid-upload kill and a restart: HEAD verifies whole at
// depth data, every block reads the last image locally, and the next cut,
// read back through a snapshot restored into a new volume, is that image.
const cutCrashAttempts = 6

const (
	cutCrashChunks = 48
	cutCrashPer    = 64 // blocks written per chunk
)

func cutCrashBlock(img byte, lba uint64) []byte {
	s := fmt.Sprintf("cut-crash %c lba %08d|", img, lba)
	b := make([]byte, 4096)
	for i := range b {
		b[i] = s[i%len(s)]
	}
	return b
}

func (env *handoffEnv) writeCutCrashImage(t *testing.T, img byte) {
	t.Helper()
	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	defer c.close()
	for ch := 0; ch < cutCrashChunks; ch++ {
		for i := 0; i < cutCrashPer; i++ {
			lba := uint64(ch*ladderChunkBlocks + i)
			c.write(t, lba, cutCrashBlock(img, lba))
		}
	}
	c.flush(t)
}

func (env *handoffEnv) requireCutCrashImage(t *testing.T, hostID string, img byte) {
	t.Helper()
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	for ch := 0; ch < cutCrashChunks; ch++ {
		for i := 0; i < cutCrashPer; i++ {
			lba := uint64(ch*ladderChunkBlocks + i)
			if got := c.read(t, lba, 4096); !bytes.Equal(got, cutCrashBlock(img, lba)) {
				t.Fatalf("%s: LBA %d is not image %c", hostID, lba, img)
			}
		}
	}
}

func TestProcedureCrashDuringACut(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, holder, "s1")
	cut := []string{"ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status}
	verify := []string{"ops", "chunk", "verify", "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath, "--depth", "data", "--timeout", "5m"}
	headGen := func() uint64 {
		out, _ := env.h.command(t, env.chunkShow()...)
		h, err := parseLayoutHead(out)
		if err != nil {
			t.Fatal(err)
		}
		return h.Gen
	}

	env.writeCutCrashImage(t, 'A')
	cutStart := time.Now()
	env.h.command(t, cut...)
	delay := time.Since(cutStart) / 2
	img, landed := byte('A'), false
	for attempt := 1; attempt <= cutCrashAttempts && !landed; attempt++ {
		img++
		env.writeCutCrashImage(t, img)
		gen, entries := headGen(), f1FilerEntries(t, env)
		done := make(chan int, 1)
		go func() {
			_, _, code := env.h.run(t, cut...)
			done <- code
		}()
		time.Sleep(delay)
		holder.kill(t)
		code := <-done
		uploaded := f1FilerEntries(t, env) - entries
		after := headGen()
		switch {
		case after != gen:
			t.Logf("attempt %d: the cut committed before the kill at %s (HEAD gen %d -> %d, CLI exit %d); shorter", attempt, delay, gen, after, code)
			delay /= 2
		case uploaded <= 0:
			t.Logf("attempt %d: nothing was uploaded before the kill at %s; longer", attempt, delay)
			delay *= 2
		default:
			landed = true
			t.Logf("attempt %d: killed at %s mid-upload: %d new filer entries, HEAD still gen %d (CLI exit %d)", attempt, delay, uploaded, gen, code)
		}
		if out, errOut, rc := env.h.run(t, verify...); rc != 0 {
			t.Fatalf("attempt %d: HEAD is not whole after the kill (verify exit %d):\n%s\n%s", attempt, rc, out, errOut)
		}
		holder = env.start(t, fmt.Sprintf("blockvolume-v1-%d", attempt), "s1")
		if got := env.adopt(t, holder, "s1"); got != e {
			t.Fatalf("attempt %d: the restarted holder adopted epoch %d; want %d", attempt, got, e)
		}
		env.requireCutCrashImage(t, "s1", img)
	}
	if !landed {
		t.Fatalf("no kill landed mid-upload in %d attempts", cutCrashAttempts)
	}
	env.h.command(t, cut...)
	env.snapshot(t, "s1", "v1", "after-crash")
	env.restoreInto(t, "v1", "after-crash", "v2")
	env.requireCutCrashImage(t, "n-v2", img)
}
