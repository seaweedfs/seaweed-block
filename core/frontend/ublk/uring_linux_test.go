//go:build linux

package ublk

import (
	"errors"
	"testing"
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
