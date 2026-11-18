//go:build swblock_format_v3_test

package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestMigrateV3RefusesAnOpenV3Volume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := storage.CreateMappedWALStoreV3Test(path, 64, migBlock, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	before := snapshotFiles(t, path)
	code, msg := migrate(t, path)
	if code != 1 || !strings.Contains(msg, "locked") {
		t.Fatalf("migration must refuse an open v3 volume: exit=%d message=%s", code, msg)
	}
	if !sameFiles(before, snapshotFiles(t, path)) {
		t.Fatal("refused migration modified the open volume")
	}
}

// Row 13b through the actual v3 open (tag swblock_format_v3_test).

// A migrated volume opens as v3 and serves the v2 blocks unchanged; its
// map is the initial one (all UNKNOWN, covered = the v2 checkpoint); a
// v3 write and a reopen work. Mutant: an initial map with clean entries
// fails here.
func TestMigrateV3VolumeOpensAsV3(t *testing.T) {
	path := v2Volume(t, true)
	if code, msg := migrate(t, path); code != 0 {
		t.Fatalf("migrate: %s", msg)
	}
	s, err := storage.OpenMappedWALStoreV3Test(path)
	if err != nil {
		t.Fatalf("v3 open of a migrated volume: %v", err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	covered, allUnknown := storage.MapCoverageV3Test(s)
	if !allUnknown || covered != s.CheckpointLSN() {
		t.Fatalf("initial map: covered %d (checkpoint %d), all UNKNOWN %v", covered, s.CheckpointLSN(), allUnknown)
	}
	for lba := uint32(0); lba < 4; lba++ {
		got, err := s.Read(lba)
		if err != nil || !bytes.Equal(got, migBlk(0x10+byte(lba))) {
			t.Fatalf("LBA %d after migration: err %v", lba, err)
		}
	}
	if _, err := s.Write(7, migBlk(0x77)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := storage.OpenMappedWALStoreV3Test(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Read(7); !bytes.Equal(got, migBlk(0x77)) {
		t.Fatal("a v3 write after migration did not survive a reopen")
	}
}

// A prepared-only volume (stopped before the commit, A then damaged) is
// refused by v3 open with ErrMigrationIncomplete, as by the tool.
func TestMigrateV3PreparedOnlyRefusedByV3Open(t *testing.T) {
	path := v2Volume(t, true)
	migrateStepHook = func(s string) error {
		if s == "prepared" {
			return errors.New("stop")
		}
		return nil
	}
	migrate(t, path)
	migrateStepHook = nil
	damageSlotA(t, path)
	if s, err := storage.OpenMappedWALStoreV3Test(path); !errors.Is(err, storage.ErrMigrationIncomplete) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("v3 open of a prepared-only volume: err = %v, want ErrMigrationIncomplete", err)
	}
}
