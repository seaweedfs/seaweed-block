//go:build !linux || !amd64

package main

import "testing"

func TestRunProbeRejectsUnsupportedPlatform(t *testing.T) {
	report, err := runProbe(8)
	if err == nil {
		t.Fatal("unsupported platform probe unexpectedly succeeded")
	}
	if report.Supported || report.RefusalReason == "" {
		t.Fatalf("unsupported report lacks explicit boundary: %+v", report)
	}
}
