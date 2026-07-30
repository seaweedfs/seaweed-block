//go:build linux && amd64

package iouring

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestExecutorSubmitsNonContiguousWritesAndFsync(t *testing.T) {
	executor, err := New(8)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()

	path := filepath.Join(t.TempDir(), "executor.dat")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(4 * probeBlockSize); err != nil {
		t.Fatal(err)
	}

	first := bytes.Repeat([]byte{0x19}, probeBlockSize)
	second := bytes.Repeat([]byte{0x73}, probeBlockSize)
	completions, err := executor.SubmitAndWait([]Operation{
		Write(int(file.Fd()), 2*probeBlockSize, first, 1),
		Write(int(file.Fd()), 0, second, 2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(completions) != 2 {
		t.Fatalf("write completions=%d want=2", len(completions))
	}
	completions, err = executor.SubmitAndWait([]Operation{Fsync(int(file.Fd()), 3)})
	if err != nil {
		t.Fatal(err)
	}
	if len(completions) != 1 || completions[0].UserData != 3 || completions[0].Result != 0 {
		t.Fatalf("fsync completions=%+v", completions)
	}

	stats := executor.Stats()
	if stats.QueueDepth < 4 || stats.SubmittedOps != 3 || stats.CompletionCount != 3 {
		t.Fatalf("stats=%+v", stats)
	}
}
