//go:build !swb_enterprise

package main

import (
	"strings"
	"testing"
)

// The OSS build keeps the 5-minute capture floor: 60 s is refused, 5m is
// accepted, and the refusal and the flag help name the floor.
func TestOSSCaptureFloor(t *testing.T) {
	base := append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1")
	if _, err := parseFlags(append(base, "--chunk-capture-interval=60s")); err == nil || !strings.Contains(err.Error(), "at least 5m0s") {
		t.Fatalf("60 s: %v; want refused below the 5m floor", err)
	}
	if _, err := parseFlags(append(base, "--chunk-capture-interval=5m")); err != nil {
		t.Fatalf("5m refused: %v", err)
	}
}
