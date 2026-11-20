package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// `sw-block ops volume-unheld` exits 0 only for an unheld v3 volume: 1 while
// a store holds it, 2 for unknown (missing, not v3) and bad usage. The
// usage states the per-host, local-only limit and the second check.
func TestOpsVolumeUnheldExitCodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v.bin")
	s, err := storage.CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = s.Close()
		}
	})
	junk := filepath.Join(t.TempDir(), "junk.bin")
	if err := os.WriteFile(junk, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	runIt := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		code := run(append([]string{"ops", "volume-unheld"}, args...), &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, out, _ := runIt("--path", path); code != 1 || !strings.HasPrefix(out, "held:") {
		t.Fatalf("open store: exit %d %q; want 1 held", code, out)
	}
	closed = true
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := runIt("--path", path); code != 0 || !strings.HasPrefix(out, "unheld:") {
		t.Fatalf("closed store: exit %d %q; want 0 unheld", code, out)
	}
	for name, args := range map[string][]string{
		"missing":   {"--path", filepath.Join(t.TempDir(), "missing.bin")},
		"not v3":    {"--path", junk},
		"no path":   nil,
		"extra arg": {"--path", path, "x"},
	} {
		if code, out, _ := runIt(args...); code != 2 || strings.HasPrefix(out, "unheld") {
			t.Fatalf("%s: exit %d %q; want 2", name, code, out)
		}
	}
	_, _, usage := runIt()
	for _, want := range []string{"per host", "local filesystems only", "network or shared mount", "BOTH", "reserves nothing", "is refused"} {
		if !strings.Contains(usage, want) {
			t.Fatalf("usage lacks %q:\n%s", want, usage)
		}
	}
}
