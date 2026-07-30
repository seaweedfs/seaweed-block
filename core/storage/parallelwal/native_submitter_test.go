package parallelwal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/internal/iouring"
)

func TestPositionedExecutionRemainsDefault(t *testing.T) {
	store, _ := createTestStore(t)
	if stats := store.NativeIOStats(); stats.Enabled {
		t.Fatalf("default store unexpectedly enabled native execution: %+v", stats)
	}
}

type fsyncFailureExecutor struct {
	calls  int
	closed bool
	stats  iouring.ExecutionStats
}

func (executor *fsyncFailureExecutor) SubmitAndWait(
	operations []iouring.Operation,
) ([]iouring.Completion, error) {
	executor.calls++
	executor.stats.SubmittedOps += uint64(len(operations))
	executor.stats.SubmitSyscalls++
	executor.stats.CompletionCount += uint64(len(operations))
	if executor.calls == 1 {
		return []iouring.Completion{{
			UserData: 1,
			Result:   int32(recordHeaderSize + testConfig().BlockSize),
		}}, nil
	}
	return []iouring.Completion{{
		UserData: ^uint64(0),
		Result:   -int32(syscall.EIO),
	}}, nil
}

func (executor *fsyncFailureExecutor) Stats() iouring.ExecutionStats {
	return executor.stats
}

func (executor *fsyncFailureExecutor) Close() error {
	executor.closed = true
	return nil
}

func TestNativeFsyncFailureTerminallyRejectsLaterWrites(t *testing.T) {
	fake := &fsyncFailureExecutor{
		stats: iouring.ExecutionStats{QueueDepth: 4},
	}
	originalFactory := newNativeIOExecutor
	newNativeIOExecutor = func(uint32) (nativeIOExecutor, error) {
		return fake, nil
	}
	t.Cleanup(func() {
		newNativeIOExecutor = originalFactory
	})

	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(filepath.Join(t.TempDir(), "fsync-failure.bin"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(0, testBlock(0x81, cfg.BlockSize)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(); err == nil || !strings.Contains(err.Error(), "durability barrier") {
		t.Fatalf("Sync error=%v want native durability barrier failure", err)
	}
	if _, err := store.Write(1, testBlock(0x82, cfg.BlockSize)); err == nil {
		t.Fatal("terminal store accepted a later write")
	}
	stats := store.NativeIOStats()
	if stats.DurabilityBarriers != 1 || stats.FsyncCompletions != 0 ||
		stats.ShortCompletions != 1 {
		t.Fatalf("failure stats=%+v", stats)
	}
	if err := store.Close(); err == nil {
		t.Fatal("Close hid the terminal durability failure")
	}
	if !fake.closed {
		t.Fatal("Close did not close the native executor")
	}
}

func TestExplicitIOUringDoesNotFallbackWhenUnsupported(t *testing.T) {
	if _, err := iouring.New(1); err == nil {
		t.Skip("io_uring is supported on this platform")
	} else if !errors.Is(err, iouring.ErrUnsupported) {
		t.Fatalf("capability error=%v want ErrUnsupported", err)
	}

	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	path := filepath.Join(t.TempDir(), "unsupported.bin")
	store, err := CreateStoreWithConfig(path, cfg)
	if store != nil || !errors.Is(err, iouring.ErrUnsupported) {
		t.Fatalf("CreateStoreWithConfig=(%v,%v) want=(nil,ErrUnsupported)", store, err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed native create left store file: %v", statErr)
	}
}

type blockingWriteExecutor struct {
	mu      sync.Mutex
	stats   iouring.ExecutionStats
	calls   int
	entered chan struct{}
	release chan struct{}
	closed  bool
}

func newBlockingWriteExecutor() *blockingWriteExecutor {
	return &blockingWriteExecutor{
		stats:   iouring.ExecutionStats{QueueDepth: 1},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (executor *blockingWriteExecutor) SubmitAndWait(
	operations []iouring.Operation,
) ([]iouring.Completion, error) {
	executor.mu.Lock()
	executor.calls++
	call := executor.calls
	executor.stats.SubmittedOps += uint64(len(operations))
	executor.stats.SubmitSyscalls++
	executor.stats.CompletionCount += uint64(len(operations))
	executor.mu.Unlock()

	if call == 1 {
		close(executor.entered)
		<-executor.release
		return []iouring.Completion{{
			UserData: 1,
			Result:   int32(recordHeaderSize + testConfig().BlockSize),
		}}, nil
	}
	return []iouring.Completion{{UserData: ^uint64(0), Result: 0}}, nil
}

func (executor *blockingWriteExecutor) Stats() iouring.ExecutionStats {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.stats
}

func (executor *blockingWriteExecutor) Close() error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	executor.closed = true
	return nil
}

func installNativeExecutor(t *testing.T, executor nativeIOExecutor) {
	t.Helper()
	originalFactory := newNativeIOExecutor
	newNativeIOExecutor = func(uint32) (nativeIOExecutor, error) {
		return executor, nil
	}
	t.Cleanup(func() {
		newNativeIOExecutor = originalFactory
	})
}

func TestNativeQueueSaturationReturnsTypedBackpressure(t *testing.T) {
	fake := newBlockingWriteExecutor()
	installNativeExecutor(t, fake)

	cfg := testConfig()
	cfg.QueueDepth = 1
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(filepath.Join(t.TempDir(), "queue-full.bin"), cfg)
	if err != nil {
		t.Fatal(err)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := store.Write(0, testBlock(0x31, cfg.BlockSize))
		writeDone <- writeErr
	}()
	<-fake.entered

	if _, err := store.Write(4, testBlock(0x32, cfg.BlockSize)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second same-lane write error=%v want ErrQueueFull", err)
	}
	stats := store.NativeIOStats()
	if stats.AdmittedRequests != 1 || stats.QueueFullRejects != 1 {
		t.Fatalf("queue saturation stats=%+v", stats)
	}

	close(fake.release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

type shortWriteExecutor struct {
	stats  iouring.ExecutionStats
	closed bool
}

func (executor *shortWriteExecutor) SubmitAndWait(
	operations []iouring.Operation,
) ([]iouring.Completion, error) {
	executor.stats.SubmittedOps += uint64(len(operations))
	executor.stats.SubmitSyscalls++
	executor.stats.CompletionCount += uint64(len(operations))
	completions := make([]iouring.Completion, len(operations))
	for i := range completions {
		completions[i] = iouring.Completion{
			UserData: uint64(i + 1),
			Result:   int32(recordHeaderSize + testConfig().BlockSize - 1),
		}
	}
	return completions, nil
}

func (executor *shortWriteExecutor) Stats() iouring.ExecutionStats {
	stats := executor.stats
	stats.QueueDepth = 4
	return stats
}

func (executor *shortWriteExecutor) Close() error {
	executor.closed = true
	return nil
}

func TestNativeCountsEveryShortCompletionAndFailsAllRequests(t *testing.T) {
	fake := &shortWriteExecutor{}
	installNativeExecutor(t, fake)

	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(filepath.Join(t.TempDir(), "short-write.bin"), cfg)
	if err != nil {
		t.Fatal(err)
	}

	blocks := [][]byte{
		testBlock(0x41, cfg.BlockSize),
		testBlock(0x42, cfg.BlockSize),
		testBlock(0x43, cfg.BlockSize),
		testBlock(0x44, cfg.BlockSize),
	}
	if _, err := store.WriteBatch(0, blocks); err == nil || !strings.Contains(err.Error(), "short append") {
		t.Fatalf("WriteBatch error=%v want short append", err)
	}
	stats := store.NativeIOStats()
	if stats.AdmittedRequests != 4 || stats.CompletionCount != 4 ||
		stats.ShortCompletions != 4 || stats.FallbackCount != 0 {
		t.Fatalf("short completion stats=%+v", stats)
	}
	if _, err := store.Write(4, testBlock(0x45, cfg.BlockSize)); err == nil {
		t.Fatal("terminal store accepted a later write")
	}
	if err := store.Close(); err == nil {
		t.Fatal("Close hid terminal short-write failure")
	}
	if !fake.closed {
		t.Fatal("Close did not close the short-write executor")
	}
}

func TestNativeCloseWaitsForInflightWriteBeforeClosingExecutor(t *testing.T) {
	fake := newBlockingWriteExecutor()
	installNativeExecutor(t, fake)

	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(filepath.Join(t.TempDir(), "close-inflight.bin"), cfg)
	if err != nil {
		t.Fatal(err)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := store.Write(0, testBlock(0x51, cfg.BlockSize))
		writeDone <- writeErr
	}()
	<-fake.entered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned with a native write in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(fake.release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	closed := fake.closed
	fake.mu.Unlock()
	if !closed {
		t.Fatal("native executor remained open after Close")
	}
}
