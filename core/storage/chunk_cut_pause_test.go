package storage

import (
	"sync"
	"testing"
	"time"
)

// cutWithBarrierHeld runs one cut while a writer holds the capture barrier
// shared (as a writer parked in WAL admission does). The hold ends hold after
// the cut's barrier Lock REQUEST (a handshake, not a guess at the cut's
// progress), so the cut waited at least hold at its Lock. The shared hold is
// released on every path, and the cut is joined within a bound.
func cutWithBarrierHeld(t *testing.T, hold time.Duration) chunkCutStats {
	t.Helper()
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	l := layer.(*chunkLayerImpl)
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	requested := make(chan struct{})
	var reqOnce sync.Once
	l.testBeforeBarrierLockHook = func() { reqOnce.Do(func() { close(requested) }) }
	s.captureMu.RLock()
	var unlockOnce sync.Once
	release := func() { unlockOnce.Do(s.captureMu.RUnlock) }
	type result struct {
		r   chunkCutResult
		err error
	}
	done := make(chan result, 1)
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(ctLocalBound):
			t.Errorf("cut not joined within %v", ctLocalBound)
		}
	})
	go func() {
		r, err := layer.Cut(ctTimeout(t))
		done <- result{r, err}
	}()
	cowWaitSignal(t, requested, "the cut's barrier Lock request")
	time.Sleep(hold) // the cut is waiting at captureMu.Lock for the shared holder
	release()
	var got result
	select {
	case got = <-done:
	case <-time.After(ctLocalBound):
		t.Fatalf("cut did not finish within %v after the shared holder released the barrier", ctLocalBound)
	}
	done <- got // for the cleanup's join
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
	if st.BarrierPauseNs < hold.Nanoseconds() {
		t.Fatalf("BarrierPauseNs %v does not include the >= %v wait for a writer holding the barrier", time.Duration(st.BarrierPauseNs), hold)
	}
}

// P2: LockWaitNs reports that wait on its own, as the first part of the pause.
func TestCutStats_LockWaitNsReportsTheWait(t *testing.T) {
	const hold = 300 * time.Millisecond
	st := cutWithBarrierHeld(t, hold)
	if st.LockWaitNs < hold.Nanoseconds() || st.BarrierPauseNs < st.LockWaitNs+st.SyncNs {
		t.Fatalf("LockWaitNs %v (pause %v, sync %v): want >= %v and inside the pause before the sync", time.Duration(st.LockWaitNs), time.Duration(st.BarrierPauseNs), time.Duration(st.SyncNs), hold)
	}
}
