//go:build linux && amd64

package iouring

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
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

func TestExecutorPoisonsAfterEventFDWaitErrorDrainsAcceptedCQE(t *testing.T) {
	executor, err := New(4)
	if err != nil {
		t.Fatal(err)
	}
	registeredEventFD := executor.ring.eventFD
	executor.ring.eventFD = 1 << 30
	executor.ring.enterCall = func(toSubmit, _, _ uint32) (int, error) {
		head := atomic.LoadUint32(executor.ring.sqHead)
		atomic.StoreUint32(executor.ring.sqHead, head+toSubmit)
		return int(toSubmit), nil
	}
	t.Cleanup(func() {
		_ = executor.Close()
		_ = unix.Close(registeredEventFD)
	})

	go func() {
		time.Sleep(5 * time.Millisecond)
		tail := atomic.LoadUint32(executor.ring.cqTail)
		executor.ring.cqes[tail&atomic.LoadUint32(executor.ring.cqMask)] = ioUringCQE{
			UserData: 71,
			Result:   probeBlockSize,
		}
		atomic.StoreUint32(executor.ring.cqTail, tail+1)
	}()

	completions, err := executor.SubmitAndWait([]Operation{
		Write(-1, 0, bytes.Repeat([]byte{0x71}, probeBlockSize), 71),
	})
	if err == nil {
		t.Fatal("invalid eventfd wait unexpectedly succeeded")
	}
	if len(completions) != 1 || completions[0].UserData != 71 {
		t.Fatalf("completions=%+v want accepted terminal CQE", completions)
	}
	if executor.ring.fd != -1 {
		t.Fatalf("eventfd-poisoned executor retained ring fd=%d", executor.ring.fd)
	}
	if _, err := executor.SubmitAndWait([]Operation{
		Write(-1, 0, bytes.Repeat([]byte{0x72}, probeBlockSize), 72),
	}); err == nil {
		t.Fatal("eventfd-poisoned executor accepted a later submission")
	}
}

func TestExecutorRejectsOversizedSubmissionWithoutStaleSQEs(t *testing.T) {
	executor, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()

	path := filepath.Join(t.TempDir(), "full-sq.dat")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	depth := int(executor.Stats().QueueDepth)
	if err := file.Truncate(int64(depth * probeBlockSize)); err != nil {
		t.Fatal(err)
	}
	operations := make([]Operation, depth+1)
	for i := range operations {
		operations[i] = Write(
			int(file.Fd()),
			int64(i*probeBlockSize),
			bytes.Repeat([]byte{byte(i + 1)}, probeBlockSize),
			uint64(i+1),
		)
	}
	head := atomic.LoadUint32(executor.ring.sqHead)
	tail := atomic.LoadUint32(executor.ring.sqTail)
	if _, err := executor.SubmitAndWait(operations); err == nil {
		t.Fatal("oversized submission unexpectedly succeeded")
	}
	if got := executor.Stats(); got.SubmittedOps != 0 || got.CompletionCount != 0 {
		t.Fatalf("oversized submission changed stats: %+v", got)
	}
	if atomic.LoadUint32(executor.ring.sqHead) != head ||
		atomic.LoadUint32(executor.ring.sqTail) != tail {
		t.Fatal("oversized submission left stale SQEs")
	}

	completions, err := executor.SubmitAndWait(operations[:depth])
	if err != nil {
		t.Fatal(err)
	}
	if len(completions) != depth {
		t.Fatalf("full-depth completions=%d want=%d", len(completions), depth)
	}
}

func TestAcceptedOperationRetainsBufferThroughForcedGC(t *testing.T) {
	executor, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()

	accepted := make(chan struct{})
	release := make(chan struct{})
	executor.ring.enterCall = func(toSubmit, _, _ uint32) (int, error) {
		head := atomic.LoadUint32(executor.ring.sqHead)
		index := executor.ring.sqArray[head&atomic.LoadUint32(executor.ring.sqMask)]
		sqe := executor.ring.sqeArray[index]
		atomic.StoreUint32(executor.ring.sqHead, head+toSubmit)
		close(accepted)
		<-release

		tail := atomic.LoadUint32(executor.ring.cqTail)
		executor.ring.cqes[tail&atomic.LoadUint32(executor.ring.cqMask)] = ioUringCQE{
			UserData: sqe.UserData,
			Result:   int32(sqe.Length),
		}
		atomic.StoreUint32(executor.ring.cqTail, tail+1)
		return int(toSubmit), nil
	}

	result := make(chan error, 1)
	finalized := make(chan struct{}, 1)
	go func() {
		owner := &finalizableWriteBuffer{}
		for i := range owner.data {
			owner.data[i] = 0x5a
		}
		runtime.SetFinalizer(owner, func(*finalizableWriteBuffer) {
			finalized <- struct{}{}
		})
		completions, submitErr := executor.SubmitAndWait([]Operation{
			Write(-1, 0, owner.data[:], 95),
		})
		if submitErr == nil &&
			(len(completions) != 1 || completions[0].UserData != 95) {
			submitErr = errors.New("accepted buffer completion mismatch")
		}
		result <- submitErr
	}()
	<-accepted
	for i := 0; i < 5; i++ {
		runtime.GC()
	}
	select {
	case <-finalized:
		t.Fatal("accepted operation owner finalized before its CQE")
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		runtime.GC()
		select {
		case <-finalized:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("buffer owner finalizer did not run after completion")
		}
		time.Sleep(time.Millisecond)
	}
}

type finalizableWriteBuffer struct {
	data [probeBlockSize]byte
}
