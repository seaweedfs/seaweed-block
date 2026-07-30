package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/seaweedfs/seaweed-block/internal/iouring"
)

type probeReport = iouring.Report

func main() {
	report, err := runProbe(8)
	writeProbeReport(os.Stdout, report, err)
	if err != nil {
		os.Exit(2)
	}
}

func writeProbeReport(w io.Writer, report probeReport, probeErr error) {
	status := "ok"
	if probeErr != nil {
		status = "unsupported"
	}
	fmt.Fprintf(w, "io_uring_probe_status=%s\n", status)
	fmt.Fprintf(w, "platform=%s\n", report.Platform)
	fmt.Fprintf(w, "kernel_release=%s\n", report.KernelRelease)
	fmt.Fprintf(w, "io_uring_supported=%t\n", report.Supported)
	fmt.Fprintf(w, "refusal_reason=%s\n", valueOrDash(report.RefusalReason))
	fmt.Fprintf(w, "queue_depth=%d\n", report.QueueDepth)
	fmt.Fprintf(w, "write_opcode_supported=%t\n", report.WriteOpcodeSupported)
	fmt.Fprintf(w, "fsync_opcode_supported=%t\n", report.FsyncOpcodeSupported)
	fmt.Fprintf(w, "submitted_ops=%d\n", report.SubmittedOps)
	fmt.Fprintf(w, "submit_syscalls=%d\n", report.SubmitSyscalls)
	fmt.Fprintf(w, "write_completions=%d\n", report.WriteCompletions)
	fmt.Fprintf(w, "fsync_completions=%d\n", report.FsyncCompletions)
	fmt.Fprintf(w, "completion_count=%d\n", report.CompletionCount)
	fmt.Fprintf(w, "verified_bytes=%d\n", report.VerifiedBytes)
	fmt.Fprintln(w, "implementation=raw_linux_uapi")
	fmt.Fprintln(w, "dependency=golang.org/x/sys/unix")
	fmt.Fprintln(w, "cgo_required=false")
	if probeErr != nil {
		fmt.Fprintf(w, "error=%s\n", oneLine(probeErr.Error()))
	}
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return oneLine(value)
}

func oneLine(value string) string {
	return strings.Join(strings.Fields(value), "_")
}
