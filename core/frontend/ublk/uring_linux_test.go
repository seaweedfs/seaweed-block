//go:build linux

package ublk

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestURing_ParamsLayout(t *testing.T) {
	if got := unsafe.Sizeof(uringParams{}); got != uringParamsSize {
		t.Fatalf("sizeof(io_uring_params) = %d, want %d", got, uringParamsSize)
	}
}

// The ring itself, without ublk: NOPs round-trip through submission and
// completion with their user data, across a wrap of the queues.
func TestURing_NopRoundTrip(t *testing.T) {
	r, err := newURing(4)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
		t.Skipf("io_uring unavailable here: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	var got []cqe
	for round := 0; round < 3; round++ {
		for i := 0; i < 4; i++ {
			if err := r.prepare(opNop, -1, 0, nil, uint64(round*10+i)); err != nil {
				t.Fatalf("round %d prepare %d: %v", round, i, err)
			}
		}
		if err := r.prepare(opNop, -1, 0, nil, 99); err == nil {
			t.Fatal("a fifth SQE was accepted into a 4-entry queue")
		}
		if err := r.submitAndWait(4); err != nil {
			t.Fatal(err)
		}
		got = r.reap(got[:0])
		if len(got) != 4 {
			t.Fatalf("round %d: %d completions, want 4", round, len(got))
		}
		seen := map[uint64]bool{}
		for _, c := range got {
			if c.Res != 0 {
				t.Fatalf("NOP result %d", c.Res)
			}
			seen[c.UserData] = true
		}
		for i := 0; i < 4; i++ {
			if !seen[uint64(round*10+i)] {
				t.Fatalf("round %d: completion for %d missing (%v)", round, round*10+i, got)
			}
		}
	}
}

// newTestRing returns a ring, or skips where io_uring is unavailable.
func newTestRing(t *testing.T) *uring {
	t.Helper()
	r, err := newURing(8)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
		t.Skipf("io_uring unavailable here: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// submitWithin runs submitAndWait and reports false if it does not return
// in time (lost SQEs would leave it waiting forever). A ring whose call
// did not return is left open: its goroutine may still be using it.
func submitWithin(r *uring, minComplete uint32, d time.Duration) (error, bool) {
	done := make(chan error, 1)
	go func() { done <- r.submitAndWait(minComplete) }()
	select {
	case err := <-done:
		return err, true
	case <-time.After(d):
		return nil, false
	}
}

// A short submission (the kernel consumed fewer SQEs than offered) must
// not lose the rest: every prepared SQE is submitted before waiting. The
// wrapper lets the kernel take at most one SQE per call and returns
// without waiting, as a short io_uring_enter does.
func TestURing_ShortSubmissionSubmitsTheRest(t *testing.T) {
	r := newTestRing(t)
	var calls atomic.Int32
	r.enter = func(fd int, toSubmit, minComplete uint32, flags uintptr) (int, unix.Errno) {
		calls.Add(1)
		if toSubmit > 1 {
			return sysEnter(fd, 1, 0, 0) // short: one SQE taken, no wait
		}
		return sysEnter(fd, toSubmit, minComplete, flags)
	}
	for i := 0; i < 4; i++ {
		if err := r.prepare(opNop, -1, 0, nil, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	err, returned := submitWithin(r, 4, 5*time.Second)
	if !returned {
		t.Fatalf("submitAndWait(4) did not return within 5s after a short submission: SQEs were lost (%d enter calls)", calls.Load())
	}
	defer r.close()
	if err != nil {
		t.Fatal(err)
	}
	if got := r.reap(nil); len(got) != 4 {
		t.Fatalf("%d completions after a short submission (%d enter calls), want all 4", len(got), calls.Load())
	}
}

// A submission the kernel never consumes is an error, never a silent
// success and never an endless retry.
func TestURing_SubmissionWithNoProgressFails(t *testing.T) {
	r := newTestRing(t)
	r.enter = func(int, uint32, uint32, uintptr) (int, unix.Errno) { return 0, 0 }
	if err := r.prepare(opNop, -1, 0, nil, 9); err != nil {
		t.Fatal(err)
	}
	err, returned := submitWithin(r, 0, 5*time.Second)
	if !returned {
		t.Fatal("submitAndWait did not return within 5s when the kernel consumed nothing (endless retry)")
	}
	defer r.close()
	if err == nil {
		t.Fatal("a submission the kernel never consumed was reported as submitted")
	}
}
