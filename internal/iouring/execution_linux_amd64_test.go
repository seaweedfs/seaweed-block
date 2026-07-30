//go:build linux && amd64

package iouring

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestExecutorPoisonsRingAfterPartialSubmission(t *testing.T) {
	executor, err := New(8)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()

	injected := errors.New("partial submission failure")
	executor.ring.enterCall = func(toSubmit, minComplete, flags uint32) (int, error) {
		if toSubmit == 0 {
			t.Fatalf("unexpected wait after injected CQE: min=%d flags=%d", minComplete, flags)
		}
		sqHead := atomic.LoadUint32(executor.ring.sqHead)
		atomic.StoreUint32(executor.ring.sqHead, sqHead+1)
		cqTail := atomic.LoadUint32(executor.ring.cqTail)
		executor.ring.cqes[cqTail&atomic.LoadUint32(executor.ring.cqMask)] = ioUringCQE{
			UserData: 1,
			Result:   4096,
		}
		atomic.StoreUint32(executor.ring.cqTail, cqTail+1)
		return 0, injected
	}

	_, err = executor.SubmitAndWait([]Operation{
		Write(-1, 0, bytes.Repeat([]byte{0x11}, 4096), 1),
		Write(-1, 4096, bytes.Repeat([]byte{0x22}, 4096), 2),
	})
	if !errors.Is(err, injected) {
		t.Fatalf("partial submission error=%v want=%v", err, injected)
	}
	if executor.ring.fd != -1 {
		t.Fatalf("poisoned executor retained ring fd=%d", executor.ring.fd)
	}
	if _, err := executor.SubmitAndWait([]Operation{
		Write(-1, 0, bytes.Repeat([]byte{0x33}, 4096), 3),
	}); err == nil {
		t.Fatal("poisoned executor accepted a later submission")
	}
}
