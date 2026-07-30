//go:build linux && amd64

package parallelwal

import (
	"path/filepath"
	"testing"
)

func TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native.bin")
	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}

	blocks := [][]byte{
		testBlock(0x11, cfg.BlockSize),
		testBlock(0x22, cfg.BlockSize),
		testBlock(0x33, cfg.BlockSize),
		testBlock(0x44, cfg.BlockSize),
	}
	lsns, err := store.WriteBatch(0, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(lsns) != 4 || lsns[0] != 1 || lsns[3] != 4 {
		t.Fatalf("LSNs=%v want=[1 2 3 4]", lsns)
	}
	stats := store.NativeIOStats()
	if !stats.Enabled || stats.SubmissionRounds != 1 || stats.SQEs != 4 ||
		stats.CompletionCount != 4 || stats.InflightHighWater != 4 ||
		stats.FallbackCount != 0 {
		t.Fatalf("native stats=%+v", stats)
	}
	t.Logf(
		"native_owner_stats enabled=%t rounds=%d sqes=%d completions=%d inflight_high_water=%d fallback=%d",
		stats.Enabled,
		stats.SubmissionRounds,
		stats.SQEs,
		stats.CompletionCount,
		stats.InflightHighWater,
		stats.FallbackCount,
	)
	if stable, err := store.Sync(); err != nil || stable != 4 {
		t.Fatalf("Sync=(%d,%v) want=(4,nil)", stable, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if recovered, err := reopened.Recover(); err != nil || recovered != 4 {
		t.Fatalf("Recover=(%d,%v) want=(4,nil)", recovered, err)
	}
	for lba, want := range blocks {
		got, err := reopened.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("LBA %d mismatch after portable reopen", lba)
		}
	}
}
