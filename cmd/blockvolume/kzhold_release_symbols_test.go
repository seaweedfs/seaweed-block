package main_test

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestKZHoldNeverInReleaseBuild: the kzhold hold (a test-only stop before the
// fresh marker is retired) exists only in a binary built with -tags kzhold.
// A normal build of blockvolume must contain no symbol of it; the kzhold build
// must (the positive control: a check that finds nothing in either build would
// prove nothing).
func TestKZHoldNeverInReleaseBuild(t *testing.T) {
	dir := t.TempDir()
	syms := func(name string, tags ...string) string {
		t.Helper()
		out := filepath.Join(dir, name)
		args := append([]string{"build"}, tags...)
		args = append(args, "-o", out, "github.com/seaweedfs/seaweed-block/cmd/blockvolume")
		if b, err := exec.Command("go", args...).CombinedOutput(); err != nil {
			t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, b)
		}
		b, err := exec.Command("go", "tool", "nm", out).CombinedOutput()
		if err != nil {
			t.Fatalf("go tool nm %s: %v\n%s", out, err, b)
		}
		return string(b)
	}
	const hold = "HoldBeforeChunkFreshRetireForTest"
	if s := syms("release"); strings.Contains(s, hold) || strings.Contains(strings.ToLower(s), "kzhold") {
		t.Fatalf("a normal build of blockvolume contains the test-only kzhold hold")
	}
	if s := syms("kzhold", "-tags", "kzhold"); !strings.Contains(s, hold) {
		t.Fatalf("the kzhold build lacks %s: this check would pass blindly", hold)
	}
}
