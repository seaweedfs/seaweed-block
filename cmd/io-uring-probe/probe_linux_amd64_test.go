//go:build linux && amd64

package main

import (
	"testing"
	"unsafe"
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
