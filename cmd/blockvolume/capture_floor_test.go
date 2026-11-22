package main

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// The capture floor is durable.MinCaptureInterval: 5 minutes by default,
// so 60 s is refused; a program that sets it to 30 s before parsing flags
// accepts 30 s and refuses 29 s. The refusal names the floor in force.
func TestCaptureIntervalFloor(t *testing.T) {
	base := append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1")
	parse := func(iv string) error {
		_, err := parseFlags(append(append([]string{}, base...), "--chunk-capture-interval="+iv))
		return err
	}
	if durable.MinCaptureInterval != 5*time.Minute {
		t.Fatalf("default floor %s, want 5m", durable.MinCaptureInterval)
	}
	if err := parse("60s"); err == nil || !strings.Contains(err.Error(), "at least 5m0s") {
		t.Fatalf("60 s under the default floor: %v", err)
	}
	if err := parse("5m"); err != nil {
		t.Fatalf("5m refused: %v", err)
	}
	old := durable.MinCaptureInterval
	durable.MinCaptureInterval = 30 * time.Second
	defer func() { durable.MinCaptureInterval = old }()
	if err := parse("30s"); err != nil {
		t.Fatalf("30 s under a 30 s floor: %v", err)
	}
	if err := parse("29s"); err == nil || !strings.Contains(err.Error(), "at least 30s") {
		t.Fatalf("29 s under a 30 s floor: %v", err)
	}
}
