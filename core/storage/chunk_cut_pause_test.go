package storage

import (
	"testing"
	"time"
)

// cutWithBarrierHeld runs one cut while a writer holds the capture barrier
// shared (as a writer parked in WAL admission does) for hold after the cut was
// admitted, and returns the cut's stats.
func cutWithBarrierHeld(t *testing.T, hold time.Duration) chunkCutStats {
	t.Helper()
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	l := layer.(*chunkLayerImpl)
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	admitted := make(chan struct{})
	l.testAfterAdmitHook = func() { close(admitted) }
	s.captureMu.RLock()
	type result struct {
		r   chunkCutResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := layer.Cut(ctTimeout(t))
		done <- result{r, err}
	}()
	cowWaitSignal(t, admitted, "cut admitted")
	time.Sleep(hold) // the cut is now waiting for the shared holder at captureMu.Lock
	s.captureMu.RUnlock()
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	return got.r.Stats
}

// P2: the guest pause starts at the barrier's Lock request (a pending writer
// blocks new readers), so the wait for writers already holding the barrier is
// part of BarrierPauseNs.
func TestCutStats_BarrierPauseIncludesTheLockWait(t *testing.T) {
	const hold = 300 * time.Millisecond
	st := cutWithBarrierHeld(t, hold)
	if st.BarrierPauseNs < (hold - 50*time.Millisecond).Nanoseconds() {
		t.Fatalf("BarrierPauseNs %v does not include the %v wait for a writer holding the barrier", time.Duration(st.BarrierPauseNs), hold)
	}
}

// P2: LockWaitNs reports that wait on its own, as the first part of the pause.
func TestCutStats_LockWaitNsReportsTheWait(t *testing.T) {
	const hold = 300 * time.Millisecond
	st := cutWithBarrierHeld(t, hold)
	if st.LockWaitNs < (hold - 50*time.Millisecond).Nanoseconds() || st.BarrierPauseNs < st.LockWaitNs+st.SyncNs {
		t.Fatalf("LockWaitNs %v (pause %v, sync %v): want >= ~%v and inside the pause before the sync", time.Duration(st.LockWaitNs), time.Duration(st.BarrierPauseNs), time.Duration(st.SyncNs), hold)
	}
}
