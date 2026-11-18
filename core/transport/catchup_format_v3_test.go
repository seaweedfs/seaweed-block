//go:build swblock_format_v3_test

package transport

import (
	"bytes"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestCatchUpV3InstallRetireABARefusesOldScan(t *testing.T) {
	primary, err := storage.CreateMappedWALStoreV3Test(filepath.Join(t.TempDir(), "primary.v3"), 64, v3RepBlock, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	primary.DisableAutoFlushForRecoveryTest()
	for lba := uint32(0); lba < 2; lba++ {
		if _, err := primary.Write(lba, v3RepBlk(0x10+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	parked, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(release)
	var scans atomic.Int32
	storage.SetScanReadV3Test(primary, func() {
		if scans.Add(1) == 1 {
			close(parked)
			<-resume
		}
	})
	replica := v3Rep(t)
	l := v3Listen(t, replica)
	exec := NewBlockExecutor(primary, l.Addr())
	closed := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closed <- r })
	if err := exec.StartCatchUp("r1", 1, 1, 1, 1, 3); err != nil {
		t.Fatal(err)
	}
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("sender did not park after reading the original WAL")
	}
	h, err := primary.BeginBaseInstall(2, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.WriteBase(0, v3RepBlk(0x70)); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(2, v3RepBlk(0x73), 3); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.FlushMappedWALStoreV3Test(primary); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	if state, id := storage.BaseStateV3Test(primary); state != storage.BaseNone || id != [16]byte{} {
		t.Fatalf("setup must finish the BASE ABA: state=%v id=%x", state, id)
	}
	release()
	select {
	case result := <-closed:
		if result.Success || result.FailureKind != engine.RecoveryFailureWALRecycled {
			t.Fatalf("old pre-BASE WAL must not be shipped after retirement: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sender did not finish after scan release")
	}
	if got := v3Read(t, replica, 0); !bytes.Equal(got, make([]byte, v3RepBlock)) {
		t.Fatal("sender published superseded pre-BASE bytes to the replica")
	}
}

func TestCatchUpV3CompletedBASEUsesRebuild(t *testing.T) {
	primary, err := storage.CreateWALStoreV3Test(filepath.Join(t.TempDir(), "primary.v3"), 64, v3RepBlock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	h, err := primary.BeginBaseInstall(100, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.WriteBase(0, v3RepBlk(0x70)); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	replica := v3Rep(t)
	l := v3Listen(t, replica)
	result := runCatchUpFrom(t, NewBlockExecutor(primary, l.Addr()), 3, 101)
	if result.Success || result.FailureKind != engine.RecoveryFailureWALRecycled {
		t.Fatalf("completed BASE without install WAL needs rebuild, not corruption: %+v", result)
	}
}

func TestCatchUpV3InstallingAtHeadRefuses(t *testing.T) {
	primary, err := storage.CreateWALStoreV3Test(filepath.Join(t.TempDir(), "primary.v3"), 64, v3RepBlock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	for lba := uint32(0); lba < 2; lba++ {
		if _, err := primary.Write(lba, v3RepBlk(0x10+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.BeginBaseInstall(2, [16]byte{1}); err != nil {
		t.Fatal(err)
	}
	replica := v3Rep(t)
	l := v3Listen(t, replica)
	result := runCatchUpFrom(t, NewBlockExecutor(primary, l.Addr()), 3, 3)
	if result.Success || !strings.Contains(result.FailReason, storage.ErrBaseInstalling.Error()) {
		t.Fatalf("installing source at head must refuse catch-up: %+v", result)
	}
}

// The actual catch-up sender (BlockExecutor.StartCatchUp -> the primary's
// ScanLBAs -> the replica's ship handler) from a dark v3 primary to an empty
// dark v3 replica: single writes and a batch are shipped and the replica
// matches the primary byte for byte. Mutant: ScanLBAs on v3 using the v2
// scan (emits nothing) leaves the replica empty and fails here.
func TestCatchUpV3PrimaryToV3Replica(t *testing.T) {
	primary, err := storage.CreateWALStoreV3Test(filepath.Join(t.TempDir(), "primary.v3"), 64, v3RepBlock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	for lba := uint32(0); lba < 3; lba++ {
		if _, err := primary.Write(lba, v3RepBlk(0x10+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	batch := [][]byte{v3RepBlk(0x20), v3RepBlk(0x21), v3RepBlk(0x22)}
	if _, err := primary.WriteBatch(5, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	_, _, head := primary.Boundaries()
	replica := v3Rep(t)
	l := v3Listen(t, replica)
	exec := NewBlockExecutor(primary, l.Addr())
	if r := runCatchUp(t, exec, head); !r.Success {
		t.Fatalf("catch-up closed unsuccessfully: %+v", r)
	}
	for lba := uint32(0); lba < 8; lba++ {
		want, err := primary.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if got := v3Read(t, replica, lba); !bytes.Equal(got, want) {
			t.Fatalf("replica LBA %d = %x..., primary %x...", lba, got[0], want[0])
		}
	}
}

// codex01 4caac700 P1: a corrupt middle record on the v3 primary must not
// let the actual catch-up sender complete at the prefix before it: the
// session closes unsuccessfully. Mutant: accepting a short prefix fails
// here.
func TestCatchUpV3CorruptMiddleRecordFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "primary.v3")
	primary, err := storage.CreateWALStoreV3Test(path, 64, v3RepBlock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	for lba := uint32(0); lba < 3; lba++ {
		if _, err := primary.Write(lba, v3RepBlk(0x10+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.DamageV3RecordTest(path, 2); err != nil {
		t.Fatal(err)
	}
	_, _, head := primary.Boundaries()
	replica := v3Rep(t)
	l := v3Listen(t, replica)
	exec := NewBlockExecutor(primary, l.Addr())
	if r := runCatchUp(t, exec, head); r.Success {
		t.Fatalf("catch-up over a corrupt LSN 2 closed successfully: %+v", r)
	}
}
