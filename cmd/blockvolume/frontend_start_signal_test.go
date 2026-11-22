package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type servingStub struct {
	first  chan struct{}
	admits atomic.Bool
}

func (s *servingStub) FirstServing() <-chan struct{} { return s.first }
func (s *servingStub) AdmitsIO() error {
	if s.admits.Load() {
		return nil
	}
	return errors.New("stale primary lineage")
}

func closedWithin(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// V1-SPEC r2.37/r2.39: a workspace chunk primary's targets start only once
// AdmitsIO passes after the backend first admitted I/O; the stop ends the
// wait without starting them.
func TestFrontendStartSignal_WorkspacePrimaryWaitsForAdmitsIO(t *testing.T) {
	f := flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v1"}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) }) // ends the re-check goroutine on any path
	b := &servingStub{first: make(chan struct{})}
	sig := frontendStartSignal(f, b, stop)
	if closedWithin(sig, 100*time.Millisecond) {
		t.Fatal("started before the backend first admitted I/O")
	}
	close(b.first) // operational, but the guarded projection does not admit yet
	if closedWithin(sig, 4*admitRecheck) {
		t.Fatal("started while AdmitsIO refuses")
	}
	b.admits.Store(true)
	if !closedWithin(sig, 5*time.Second) {
		t.Fatal("did not start after AdmitsIO passed")
	}

	stop2 := make(chan struct{})
	b2 := &servingStub{first: make(chan struct{})}
	close(b2.first)
	sig2 := frontendStartSignal(f, b2, stop2)
	close(stop2)
	if closedWithin(sig2, 4*admitRecheck) {
		t.Fatal("a stopped wait started the targets")
	}
}

// claude06's #410 design condition: a non-chunk volume or a supporting
// replica starts its frontends right after open, as before: its start
// signal is the backend's first-serving channel itself, not AdmitsIO.
func TestFrontendStartSignal_OtherVolumesStartAfterOpenAsBefore(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	for _, f := range []flags{
		{durableImpl: "walstore", volumeID: "v1"},
		{durableImpl: "walstore-chunk", volumeID: "v1"}, // chunk without the workspace lease
	} {
		b := &servingStub{first: make(chan struct{})}
		close(b.first) // open and recovered; AdmitsIO refuses (not the primary)
		if !closedWithin(frontendStartSignal(f, b, stop), time.Second) {
			t.Fatalf("impl=%s workspace-lease=%v: did not start right after open while AdmitsIO refuses", f.durableImpl, f.workspaceLease)
		}
	}
}
