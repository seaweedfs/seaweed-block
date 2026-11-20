package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartOptInLoopbackFlagIsAcceptedByBlockmaster(t *testing.T) {
	if os.Getenv("SEAWEED_KNOWN_BUGS") != "1" {
		t.Skip("known bug #186 (open): reproduction runs with SEAWEED_KNOWN_BUGS=1; the fix PR removes this skip")
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "seaweed-block", "templates", "blockmaster.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const flag = "--launcher-reject-loopback-publish-targets"
	if !strings.Contains(string(raw), flag) {
		t.Skip("chart no longer emits this opt-in flag")
	}
	if _, err := parseFlags([]string{"--authority-store", t.TempDir(), flag}); err != nil {
		t.Fatalf("chart opt-in flag rejected by real binary parser: %v", err)
	}
}
