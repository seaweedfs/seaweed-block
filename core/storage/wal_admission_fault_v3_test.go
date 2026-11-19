package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWALStoreV3AdmissionReturnsFaultLatchedDuringPressureWait(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "write"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			s := admissionPressureV3(t, 15)
			s.admissionTimeout = 40 * time.Millisecond
			parked, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			t.Cleanup(release) // unblock before the helper's Close cleanup
			var sleeps atomic.Int32
			s.admission.sleepFn = func(time.Duration) {
				if sleeps.Add(1) == 1 {
					close(parked)
					<-resume
				} else {
					time.Sleep(time.Millisecond)
				}
			}
			before := s.NextLSN()
			head := s.v3.w.logicalHeadV3()
			done := make(chan error, 1)
			go func() {
				var err error
				if batch {
					_, err = s.WriteBatch(16, [][]byte{blk(16), blk(17)})
				} else {
					_, err = s.Write(16, blk(16))
				}
				done <- err
			}()
			select {
			case <-parked:
			case <-time.After(2 * time.Second):
				t.Fatal("writer did not enter hard-pressure wait")
			}
			fault := errors.New("injected terminal persistence fault while admission waits")
			s.latchV3(fault)
			release()
			select {
			case err := <-done:
				if !errors.Is(err, fault) || errors.Is(err, errWALFull) {
					t.Fatalf("waiting writer lost terminal cause: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("terminal fault did not end wait")
			}
			if sleeps.Load() != 1 {
				t.Fatalf("writer kept polling after terminal fault: sleeps=%d", sleeps.Load())
			}
			if s.NextLSN() != before || s.v3.w.logicalHeadV3() != head {
				t.Fatal("faulted waiting writer appended or advanced LSN")
			}
		})
	}
}

// The same terminal cause must survive a saturated writer semaphore even
// without hard WAL pressure; the store's normal append guard is too late there.
func TestWALStoreV3AdmissionReturnsFaultWhileWaitingForWriterSlot(t *testing.T) {
	s := admissionPressureV3(t, 0)
	s.admissionTimeout = 40 * time.Millisecond
	for i := 0; i < cap(s.admission.sem); i++ {
		s.admission.sem <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(s.admission.sem); i++ {
			<-s.admission.sem
		}
	})
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(release)
	sample := s.admission.walUsed
	s.admission.walUsed = func() float64 {
		used := sample() // its store/descriptor/writer locks have all released
		close(entered)
		<-resume
		return used
	}
	done := make(chan error, 1)
	go func() { _, err := s.Write(0, blk(1)); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("write never reached admission")
	}
	fault := errors.New("terminal persistence fault while writer slots are full")
	s.latchV3(fault)
	release()
	select {
	case err := <-done:
		if !errors.Is(err, fault) || errors.Is(err, errWALFull) {
			t.Fatalf("slot waiter lost cause: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fault did not release slot waiter")
	}
	if s.NextLSN() != 1 {
		t.Fatal("faulted slot waiter appended")
	}
}
