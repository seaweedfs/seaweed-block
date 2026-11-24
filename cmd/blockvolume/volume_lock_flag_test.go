package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// The test lock directory override is refused under systemd
// (INVOCATION_ID set), so it cannot reach a shipped unit.
func TestVolumeLockDirTestOnlyRefusedUnderSystemd(t *testing.T) {
	args := append(phase150RequiredBlockvolumeArgs(), "--durable-root="+t.TempDir(), "--durable-impl=walstore", "--volume-lock-dir-test-only="+t.TempDir())
	t.Setenv("INVOCATION_ID", "")
	f, err := parseFlags(args)
	if err != nil || f.volumeLockDirTestOnly == "" {
		t.Fatalf("the test override outside systemd: %+v %v", f.volumeLockDirTestOnly, err)
	}
	t.Setenv("INVOCATION_ID", "0123456789abcdef")
	if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "refused under systemd") {
		t.Fatalf("the test override was accepted under systemd: %v", err)
	}
}

// The test override is hidden from --help.
func TestVolumeLockDirTestOnlyHiddenFromHelp(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	t.Cleanup(func() { os.Stderr = saved; _ = w.Close(); _ = r.Close() })
	read := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(r); read <- b }() // usage exceeds a pipe buffer
	os.Stderr = w
	_, parseErr := parseFlags([]string{"-h"})
	os.Stderr = saved
	_ = w.Close() // release the reader before joining it
	var out []byte
	select {
	case out = <-read:
	case <-time.After(10 * time.Second):
		t.Fatal("usage reader did not finish")
	}
	if parseErr == nil || !strings.Contains(string(out), "-durable-root") {
		t.Fatalf("no usage printed: %v %q", parseErr, out)
	}
	if strings.Contains(string(out), volumeLockDirTestOnlyFlag) {
		t.Fatalf("--help lists the test override:\n%s", out)
	}
}
