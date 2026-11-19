package main

import (
	"strconv"
	"strings"
	"testing"
)

func TestDurableCreateFlagsValidateOriginalGeometry(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("64-bit CLI uint overflow case")
	}
	for _, blocks := range []string{"4294967297", "18446744073709551615"} {
		args := append(phase150RequiredBlockvolumeArgs(), "--durable-root="+t.TempDir(), "--durable-blocks="+blocks)
		if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "durable-blocks") {
			t.Fatalf("blocks %s: %v", blocks, err)
		}
	}
	args := append(phase150RequiredBlockvolumeArgs(), "--durable-root="+t.TempDir(), "--durable-blocks=4294967295", "--durable-blocksize=4096")
	if _, err := parseFlags(args); err != nil {
		t.Fatal(err)
	}
	args = append(args, "--durable-blocksize=8192")
	if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
		t.Fatalf("oversize geometry: %v", err)
	}
}
