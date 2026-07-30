package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestWriteProbeReportIncludesMachineReadableBoundary(t *testing.T) {
	var output bytes.Buffer
	writeProbeReport(&output, probeReport{
		Platform:      "test/arch",
		KernelRelease: "test-kernel",
		RefusalReason: "unsupported platform",
	}, errors.New("unsupported platform"))

	got := output.String()
	for _, want := range []string{
		"io_uring_probe_status=unsupported",
		"platform=test/arch",
		"refusal_reason=unsupported_platform",
		"implementation=raw_linux_uapi",
		"dependency=golang.org/x/sys/unix",
		"cgo_required=false",
		"error=unsupported_platform",
	} {
		if !strings.Contains(got, want+"\n") {
			t.Fatalf("report missing %q:\n%s", want, got)
		}
	}
}
