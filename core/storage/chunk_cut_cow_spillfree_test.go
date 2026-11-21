package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// The spill cap is min(COWSpillLimit, free/2 - reserve), at least 0, read
// from COWSpillFree on every call. A cap of 0 carries its cause: no room
// (the free space is within the reserve) or a read error, which is not
// reported as a full disk. Without COWSpillFree the configured cap applies.
func TestCOWSpillLimit_FromFreeSpace(t *testing.T) {
	l := &chunkLayerImpl{cfg: ChunkLayerConfig{COWSpillLimit: 4 << 30}}
	if got := l.cowSpillLimit(); got.bytes != 4<<30 || got.cause != nil {
		t.Fatalf("no free-space reader: cap %+v, want the configured 4 GiB", got)
	}
	var free atomic.Int64
	var reads atomic.Int64
	var fail atomic.Bool
	l.cfg.COWSpillFree = func() (int64, error) {
		reads.Add(1)
		if fail.Load() {
			return 1 << 40, errors.New("statfs failed")
		}
		return free.Load(), nil
	}
	for _, c := range []struct {
		free, want int64
		cause      error
	}{
		{100 << 30, 4 << 30, nil},             // plenty: the configured cap
		{6 << 30, 2 << 30, nil},               // 6 GiB free: half is 3, minus the 1 GiB reserve
		{2 << 30, 0, errCutNoRoom},            // half the free space is all reserve
		{1 << 30, 0, errCutNoRoom},            // below the reserve: never negative
		{10<<30 + 2, 4 << 30, nil},            // the configured cap still bounds it
		{3 << 30, (3<<30)/2 - (1 << 30), nil}, // 1.5 GiB - 1 GiB
	} {
		free.Store(c.free)
		if got := l.cowSpillLimit(); got.bytes != c.want || !errors.Is(got.cause, c.cause) || (c.cause == nil) != (got.cause == nil) {
			t.Fatalf("free %d: cap %+v, want %d (cause %v)", c.free, got, c.want, c.cause)
		}
	}
	if reads.Load() != 6 {
		t.Fatalf("free space read %d times for 6 calls; want once per call", reads.Load())
	}
	fail.Store(true)
	if got := l.cowSpillLimit(); got.bytes != 0 || !errors.Is(got.cause, errCutFreeUnreadable) || errors.Is(got.cause, errCutNoRoom) {
		t.Fatalf("free-space read error: cap %+v, want 0 with cause free_space_unreadable (not no room)", got)
	}
}

// Each cut reads the free space once, for both the staging bound and the
// window, and shows the cap in the stats: the configured cap with ample
// free space; 0 with reason no_room_to_cut when the free space is within
// the reserve (the cut still completes by staging before the barrier);
// 0 with reason free_space_unreadable when the read fails.
func TestCutCOWV3_SpillCapIsReadOncePerCutAndShown(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	gate.unblock()
	var free, reads atomic.Int64
	var fail atomic.Bool
	l.cfg.COWSpillFree = func() (int64, error) {
		reads.Add(1)
		if fail.Load() {
			return 0, errors.New("statfs failed")
		}
		return free.Load(), nil
	}
	for i, c := range []struct {
		free   int64
		fail   bool
		cap    int64
		reason string
	}{
		{100 << 30, false, l.cfg.COWSpillLimit, ""},
		{1 << 30, false, 0, "no_room_to_cut"},
		{100 << 30, true, 0, "free_space_unreadable"},
	} {
		free.Store(c.free)
		fail.Store(c.fail)
		if _, err := s.Write(uint32(i*ctChunkBlocks+1), makeBlock(ctBlockSize, byte(0x60+i))); err != nil {
			t.Fatal(err)
		}
		before := reads.Load()
		if _, err := l.Cut(context.Background()); err != nil {
			t.Fatalf("cut %d (free %d, read error %v): %v; want it to complete by staging, not refuse", i, c.free, c.fail, err)
		}
		if st := l.Stats(); len(st.CutsAborted) != 0 {
			t.Fatalf("cut %d: aborts %v; want none", i, st.CutsAborted)
		}
		if n := reads.Load() - before; n != 1 {
			t.Fatalf("cut %d read the free space %d times; want once", i, n)
		}
		if st := l.Stats(); st.COWSpillCap != c.cap || st.COWSpillCapReason != c.reason {
			t.Fatalf("cut %d: stats cap %d reason %q; want %d %q", i, st.COWSpillCap, st.COWSpillCapReason, c.cap, c.reason)
		}
	}
}

// The cap the window holds is the one this cut read: with the free space
// within the reserve (or unreadable) the copy window opens with a cap of 0
// and that cause, straight from the reader. The window is caught open by
// holding its copy of chunk 0: staging runs with the read gate disarmed,
// and the stage hook re-dirties chunk 0 each round and arms the gate after
// the second, so the barrier leaves chunk 0 to the window.
func TestCutCOWV3_WindowGetsTheCutsReadCap(t *testing.T) {
	for _, c := range []struct {
		name  string
		read  func() (int64, error)
		cause error
	}{
		{"no room", func() (int64, error) { return 1 << 30, nil }, errCutNoRoom},
		{"unreadable", func() (int64, error) { return 0, errors.New("statfs failed") }, errCutFreeUnreadable},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, l, _, gate := cowFixture(t)
			gate.mu.Lock()
			gate.armed = false
			gate.mu.Unlock()
			l.cfg.COWSpillFree = c.read
			l.testAfterStageHook = func(round int) {
				if _, err := s.Write(1, makeBlock(ctBlockSize, byte(0x70+round))); err != nil {
					t.Error(err)
				}
				if round == 1 {
					gate.mu.Lock()
					gate.armed = true
					gate.mu.Unlock()
				}
			}
			cutDone, cutErr := cowStartCut(t, l, gate)
			w := l.cowActive.Load()
			if w == nil {
				t.Fatal("no copy window open while its copy is held")
			}
			w.mu.Lock()
			limit, cause := w.spillLimit, w.spillCause
			w.mu.Unlock()
			if limit != 0 || !errors.Is(cause, c.cause) {
				t.Fatalf("window cap %d cause %v; want 0 and %v from the cut's read", limit, cause, c.cause)
			}
			gate.unblock()
			cowWaitSignal(t, cutDone, "cut with the window held")
			if err := <-cutErr; err != nil {
				t.Fatalf("cut: %v; want it to complete (the held copy needs no spill)", err)
			}
		})
	}
}

// A save that meets a zero cap aborts the cut under the cap's own cause:
// no_room_to_cut or free_space_unreadable, not cow_capacity; the guest
// write completes and the next cut is not blocked.
func TestCutCOWV3_ZeroCapAbortIsCountedByItsCause(t *testing.T) {
	for _, c := range []struct {
		cause  error
		reason string
	}{
		{errCutNoRoom, "no_room_to_cut"},
		{fmt.Errorf("%w: %w", errCutFreeUnreadable, errors.New("statfs failed")), "free_space_unreadable"},
	} {
		t.Run(c.reason, func(t *testing.T) {
			s, l, _, gate := cowFixture(t)
			cutDone, cutErr := cowStartCut(t, l, gate)
			w := l.cowActive.Load()
			w.mu.Lock()
			w.memory = cutCOWMemoryLimit // the in-memory budget is used
			w.spillLimit, w.spillCause = 0, c.cause
			w.mu.Unlock()
			writeDone := make(chan struct{})
			writeErr := make(chan error, 1)
			// Registered before the writer starts: release the copier and
			// join the writer (bounded) whatever the test's outcome.
			t.Cleanup(func() { gate.unblock(); cowWaitSignal(t, writeDone, "zero-cap writer cleanup") })
			go func() {
				defer close(writeDone)
				_, err := s.Write(5, makeBlock(ctBlockSize, 0xd7))
				writeErr <- err
			}()
			cowWaitSignal(t, writeDone, "guest write at a zero cap")
			if err := <-writeErr; err != nil {
				t.Fatal("guest write refused at a zero cap:", err)
			}
			gate.unblock()
			cowWaitSignal(t, cutDone, "cut at a zero cap")
			if err := <-cutErr; !errors.Is(err, errCutCOWAborted) || !errors.Is(err, c.cause) {
				t.Fatalf("cut at a zero cap: %v, want an abort carrying %v", err, c.cause)
			}
			if st := l.Stats(); st.CutsAborted[c.reason] != 1 || st.CutsAborted["cow_capacity"] != 0 {
				t.Fatalf("aborts %v; want one %s and no cow_capacity", st.CutsAborted, c.reason)
			}
		})
	}
}

// The staging bound follows the free space read at the cut: with ample
// free space the fixture's two dirty chunks are within the bound and go to
// the barrier unstaged; with free space below the reserve the cap is 0, so
// the cut pre-uploads them in a staging round before the barrier.
func TestCutCOWV3_LowFreeSpaceStagesBeforeTheBarrier(t *testing.T) {
	for _, c := range []struct {
		name       string
		free       int64
		wantStaged bool
	}{
		{"ample free space", 100 << 30, false},
		{"free space below the reserve", 1 << 30, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, l, _, gate := cowFixture(t)
			gate.unblock()
			l.cfg.COWSpillFree = func() (int64, error) { return c.free, nil }
			r, err := l.Cut(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if staged := r.Stats.StageRounds > 0 && r.Stats.StagedChunks >= 2; staged != c.wantStaged {
				t.Fatalf("free %d: %d staging rounds, %d staged chunks; want staged=%v", c.free, r.Stats.StageRounds, r.Stats.StagedChunks, c.wantStaged)
			}
		})
	}
}
