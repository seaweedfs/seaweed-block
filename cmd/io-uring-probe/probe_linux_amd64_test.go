//go:build linux && amd64

package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestLinuxUAPIStructSizes(t *testing.T) {
	if got := unsafe.Sizeof(ioUringParams{}); got != 120 {
		t.Fatalf("io_uring_params size=%d want=120", got)
	}
	if got := unsafe.Sizeof(ioUringSQE{}); got != 64 {
		t.Fatalf("io_uring_sqe size=%d want=64", got)
	}
	if got := unsafe.Sizeof(ioUringCQE{}); got != 16 {
		t.Fatalf("io_uring_cqe size=%d want=16", got)
	}
}

func TestRunProbeCompletesWritesFsyncAndReopen(t *testing.T) {
	report, err := runProbe(8)
	if err != nil {
		t.Fatalf("run probe: %v (report=%+v)", err, report)
	}
	if !report.Supported || !report.WriteOpcodeSupported || !report.FsyncOpcodeSupported {
		t.Fatalf("required io_uring capability absent: %+v", report)
	}
	if report.QueueDepth < 4 {
		t.Fatalf("queue depth=%d want >=4", report.QueueDepth)
	}
	if report.SubmittedOps != 4 {
		t.Fatalf("submitted ops=%d want=4", report.SubmittedOps)
	}
	if report.WriteCompletions != 3 || report.FsyncCompletions != 1 || report.CompletionCount != 4 {
		t.Fatalf("unexpected completion counts: %+v", report)
	}
	if report.VerifiedBytes != 3*probeBlockSize {
		t.Fatalf("verified bytes=%d want=%d", report.VerifiedBytes, 3*probeBlockSize)
	}
}

func TestEnterRetriesInterruptedWait(t *testing.T) {
	calls := 0
	ring := &ioUring{
		fd: -1,
		enterCall: func(toSubmit, minComplete, flags uint32) (int, error) {
			calls++
			if calls == 1 {
				return 0, unix.EINTR
			}
			if toSubmit != 0 || minComplete != 3 || flags != ioUringEnterGetEvents {
				t.Fatalf(
					"retry arguments=(%d,%d,%d) want=(0,3,%d)",
					toSubmit,
					minComplete,
					flags,
					ioUringEnterGetEvents,
				)
			}
			return 0, nil
		},
	}
	if _, err := ring.enter(0, 3, ioUringEnterGetEvents); err != nil {
		t.Fatalf("enter returned interrupted wait: %v", err)
	}
	if calls != 2 || ring.submitSyscalls != 2 {
		t.Fatalf("calls=%d submitSyscalls=%d want=2/2", calls, ring.submitSyscalls)
	}

	ring.enterCall = func(uint32, uint32, uint32) (int, error) {
		return 0, errors.New("terminal test error")
	}
	if _, err := ring.enter(0, 1, ioUringEnterGetEvents); err == nil {
		t.Fatal("non-EINTR error was unexpectedly retried as success")
	}
}

func TestAcceptedWaitDoesNotReturnOnTransientError(t *testing.T) {
	var head uint32
	var tail uint32
	var mask uint32
	calls := 0
	ring := &ioUring{
		fd:     -1,
		cqHead: &head,
		cqTail: &tail,
		cqMask: &mask,
		cqes: []ioUringCQE{{
			UserData: 17,
			Result:   probeBlockSize,
		}},
		enterCall: func(uint32, uint32, uint32) (int, error) {
			calls++
			if calls == 1 {
				return 0, errors.New("transient wait failure")
			}
			atomic.StoreUint32(&tail, 1)
			return 0, nil
		},
	}

	completions := ring.waitForAccepted(1)
	if calls != 2 {
		t.Fatalf("wait calls=%d want=2", calls)
	}
	if len(completions) != 1 || completions[0].UserData != 17 {
		t.Fatalf("completions=%+v want user_data=17", completions)
	}
}
