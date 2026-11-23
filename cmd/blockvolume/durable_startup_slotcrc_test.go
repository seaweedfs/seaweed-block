package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

// Issue #51: a real SmartWAL WAL-slot corruption (the issue's mutation:
// the last byte of a committed record's 32-byte slot, i.e. its record CRC)
// must not let the volume start Ready.

// writeSmartWAL writes the given LBAs in order through the real provider,
// syncs and closes it, and returns the store's path.
func writeSmartWAL(t *testing.T, cfg durable.ProviderConfig, lbas []uint32) string {
	t.Helper()
	dp, err := durable.NewDurableProvider(cfg, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := dp.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	for i, lba := range lbas {
		data := make([]byte, 4096)
		data[0], data[1] = byte(0xA0+i), byte(lba)
		if _, err := st.Write(lba, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := dp.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(cfg.StorageRoot, "v1.bin")
}

// corruptSlotCRC flips the last byte (the record CRC) of the newest slot
// recorded for lba, as the issue's QA mutation did (target_record_offset
// +31).
func corruptSlotCRC(t *testing.T, path string, lba uint32) {
	t.Helper()
	_, records, err := smartwal.InspectRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	var off int64 = -1
	var lsn uint64
	for _, r := range records {
		if r.LBA == lba && r.LSN >= lsn {
			off, lsn = r.Offset, r.LSN
		}
	}
	if off < 0 {
		t.Fatalf("no record for LBA %d", lba)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off+31); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, off+31); err != nil {
		t.Fatal(err)
	}
}

func startAfterCorruption(t *testing.T, cfg durable.ProviderConfig) (durableStartupOutcome, *spyHost) {
	t.Helper()
	dp, err := durable.NewDurableProvider(cfg, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dp.Close() })
	h := &spyHost{}
	outcome, evidence, err := openAndRecoverDurable(dp, h, "v1", 5*time.Second)
	t.Logf("outcome=%v reported=%v evidence=%q err=%v", outcome, h.reported, evidence, err)
	return outcome, h
}

// The issue's schedule: LBA 0 was written twice; the newer record's slot
// is corrupted, so recovery drops it as torn and falls back to the older
// record, whose CRC no longer matches the extent. Current main fails
// closed (85d93753): wal_corrupt, readiness blocked.
func TestDurableStartup_Issue51_CorruptNewerSlotIsWALCorrupt(t *testing.T) {
	cfg := durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 16}
	path := writeSmartWAL(t, cfg, []uint32{0, 1, 2, 0})
	corruptSlotCRC(t, path, 0)
	outcome, h := startAfterCorruption(t, cfg)
	if outcome != durableStartupFaulted || len(h.reported) != 1 || h.reported[0] != volume.UnhealthyWALCorrupt || len(h.blocked) == 0 {
		t.Fatalf("corrupt newer slot: outcome=%v reported=%v blocked=%v; want faulted, wal_corrupt, readiness blocked", outcome, h.reported, h.blocked)
	}
}

// Detection only, for smartwal too. A
// corrupted committed slot with later valid records starts as before
// (ready), and the durable status carries the counted warning.
func TestDurableStartup_Issue51_SmartWALHoleIsAWarningNotAnOutage(t *testing.T) {
	cfg := durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 16}
	path := writeSmartWAL(t, cfg, []uint32{0, 1, 2, 3})
	corruptSlotCRC(t, path, 1) // LSN 2; LSN 3 and 4 stay valid
	dp, err := durable.NewDurableProvider(cfg, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	defer dp.Close()
	h := &spyHost{}
	outcome, evidence, err := openAndRecoverDurable(dp, h, "v1", 5*time.Second)
	if outcome != durableStartupReady || len(h.reported) != 0 {
		t.Fatalf("smartwal hole: outcome=%v reported=%v evidence=%q err=%v; want ready", outcome, h.reported, evidence, err)
	}
	warned := false
	for _, st := range dp.DurableStatuses() {
		for _, w := range st.Warnings {
			if strings.HasPrefix(w, storage.WarningWALHoleSuspected+":") && st.HolesSuspected == 1 {
				warned = true
			}
		}
	}
	if !warned {
		t.Fatalf("durable statuses %+v; want the counted %s warning", dp.DurableStatuses(), storage.WarningWALHoleSuspected)
	}
}

// Control: a torn tail (the newest slot bad, nothing valid after it)
// still recovers: a power loss is not an outage.
func TestDurableStartup_Issue51_SmartWALTornTailRecovers(t *testing.T) {
	cfg := durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 16}
	path := writeSmartWAL(t, cfg, []uint32{0, 1, 2})
	corruptSlotCRC(t, path, 2) // LSN 3, the newest
	outcome, h := startAfterCorruption(t, cfg)
	if outcome != durableStartupReady || len(h.reported) != 0 {
		t.Fatalf("torn tail: outcome=%v reported=%v; want ready", outcome, h.reported)
	}
}

// V3 replaces the legacy warning-only recovery policy with nonce-bound,
// contiguous WAL recovery. Corruption below strict W must block readiness.
func TestDurableStartup_Issue51_WALStoreV3CommittedHoleBlocksReadiness(t *testing.T) {
	const childEnv = "SWBLOCK_V3_STARTUP_CRASH_PATH"
	if path := os.Getenv(childEnv); path != "" {
		s, err := storage.CreateWALStoreWithOptions(path, 16, 4096, storage.WALStoreOptions{WMode: storage.WModeStrict})
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		for i := uint32(0); i < 4; i++ {
			data := make([]byte, 4096)
			data[0] = byte(0xd0 + i)
			if _, err := s.Write(i, data); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // deliberately no Close/checkpoint; OS releases the owner lock
	}
	root := t.TempDir()
	path := filepath.Join(root, "v1.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDurableStartup_Issue51_WALStoreV3CommittedHoleBlocksReadiness$")
	cmd.Env = append(os.Environ(), childEnv+"="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash fixture: %v\n%s", err, out)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	at := int64(8192 + (70 + 4096) + 62 + 100) // v3 second record payload
	b := make([]byte, 1)
	_, re := f.ReadAt(b, at)
	b[0] ^= 0xff
	_, we := f.WriteAt(b, at)
	ce := f.Close()
	if re != nil || we != nil || ce != nil {
		t.Fatal(re, we, ce)
	}
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: root, BlockSize: 4096, NumBlocks: 16}
	outcome, h := startAfterCorruption(t, cfg)
	if outcome != durableStartupFaulted || len(h.reported) != 1 || h.reported[0] != volume.UnhealthyWALCorrupt || len(h.blocked) == 0 {
		t.Fatalf("committed hole: outcome=%v reported=%v blocked=%v; want wal_corrupt and blocked", outcome, h.reported, h.blocked)
	}
}
