package parallelwal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/internal/iouring"
)

func TestPositionedExecutionRemainsDefault(t *testing.T) {
	store, _ := createTestStore(t)
	if stats := store.NativeIOStats(); stats.Enabled {
		t.Fatalf("default store unexpectedly enabled native execution: %+v", stats)
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
