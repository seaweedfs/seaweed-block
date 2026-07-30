package parallelwal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

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
