package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/cmd/sw-block/internal/v2legacy"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Plan row 13b controls (DESIGN r17.4 section 10.1; owner claude03), all
// through the actual CLI entry (run).

const migBlock = 4096

func migBlk(b byte) []byte { return bytes.Repeat([]byte{b}, migBlock) }

// v2Volume creates a real v2/impl1 volume with 4 written blocks; clean
// closes it (flushing), otherwise it is left synced but unflushed.
func v2Volume(t *testing.T, clean bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vol")
	s, err := storage.CreateWALStore(path, 64, migBlock)
	if err != nil {
		t.Fatal(err)
	}
	if !clean {
		s.DisableAutoFlushForRecoveryTest()
	}
	for lba := uint32(0); lba < 4; lba++ {
		if _, err := s.Write(lba, migBlk(0x10+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if clean {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Cleanup(func() { _ = s.Close() })
	}
	return path
}

func migrate(t *testing.T, path string) (int, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run([]string{"migrate-v3", "--chunk-blocks", "4", path}, &out, &errb)
	return code, out.String() + errb.String()
}

// snapshotFiles reads the volume and every sidecar next to it.
func snapshotFiles(t *testing.T, path string) map[string][]byte {
	t.Helper()
	got := map[string][]byte{}
	matches, _ := filepath.Glob(path + "*")
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		got[filepath.Base(m)] = b
	}
	return got
}

func sameFiles(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !bytes.Equal(v, b[k]) {
			return false
		}
	}
	return true
}

// A clean v2 volume migrates: slot A committed (slotGen 2), slot B rolled
// over (slotGen 3), every extent byte unchanged, the backup holds the old
// superblock, a v2 chunk bitmap is retired, the old binary now refuses the
// file, and a rerun changes no byte.
func TestMigrateV3CleanVolume(t *testing.T) {
	path := v2Volume(t, true)
	if err := os.WriteFile(path+".chunkbitmap", []byte("c11"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := v2legacy.ReadSuperblock(before)
	if err != nil {
		t.Fatal(err)
	}
	extentBase := sb.WALOffset + sb.WALSize
	if code, msg := migrate(t, path); code != 0 || !strings.Contains(msg, "descriptor sha256") {
		t.Fatalf("migrate: exit %d: %s", code, msg)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after[extentBase:], before[extentBase:]) {
		t.Fatal("the migration changed extent bytes")
	}
	if st, gen := storage.ClassifyMigrationSlotV3(after[:4096]); st != storage.MigrationSlotCommitted || gen != 2 {
		t.Fatalf("slot A: state %d gen %d, want committed gen 2", st, gen)
	}
	if st, gen := storage.ClassifyMigrationSlotV3(after[4096:8192]); st != storage.MigrationSlotCommitted || gen != 3 {
		t.Fatalf("slot B: state %d gen %d, want committed gen 3", st, gen)
	}
	if bak, err := os.ReadFile(path + ".v2-superblock.bak"); err != nil || !bytes.Equal(bak, before[:4096]) {
		t.Fatalf("backup: %v", err)
	}
	if _, err := os.Stat(path + ".chunkbitmap"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the v2 chunk bitmap was not retired")
	}
	if _, err := os.Stat(path + ".v2-chunkbitmap.bak"); err != nil {
		t.Fatalf("retired bitmap: %v", err)
	}
	if s, err := storage.OpenWALStore(path); err == nil {
		_ = s.Close()
		t.Fatal("the old binary opened a migrated volume")
	}
	snap := snapshotFiles(t, path)
	if code, msg := migrate(t, path); code != 0 {
		t.Fatalf("rerun: exit %d: %s", code, msg)
	}
	if !sameFiles(snap, snapshotFiles(t, path)) {
		t.Fatal("a rerun on a committed volume changed bytes")
	}
}

// Refusals change no byte of the volume or any sidecar: an unflushed WAL
// (ErrMigrateNotClean with the operator remedy), an impl-2 volume, and a
// file with neither a v2 superblock nor a valid v3 slot. Mutant: skipping
// the clean scan migrates the unflushed volume and fails here.
func TestMigrateV3RefusalsChangeNothing(t *testing.T) {
	t.Run("unflushed WAL", func(t *testing.T) {
		path := v2Volume(t, false)
		snap := snapshotFiles(t, path)
		code, msg := migrate(t, path)
		if code == 0 || !strings.Contains(msg, "open with the old binary, flush, close cleanly, retry") {
			t.Fatalf("exit %d: %s", code, msg)
		}
		if !sameFiles(snap, snapshotFiles(t, path)) {
			t.Fatal("a refused migration changed bytes")
		}
	})
	t.Run("impl 2", func(t *testing.T) {
		path := v2Volume(t, true)
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte{2, 0, 0, 0}, 92); err != nil {
			t.Fatal(err)
		}
		f.Close()
		snap := snapshotFiles(t, path)
		if code, _ := migrate(t, path); code == 0 || !sameFiles(snap, snapshotFiles(t, path)) {
			t.Fatal("an impl-2 volume was not refused unchanged")
		}
	})
	t.Run("neither valid", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "junk")
		if err := os.WriteFile(path, bytes.Repeat([]byte{0xEE}, 3*4096), 0o644); err != nil {
			t.Fatal(err)
		}
		snap := snapshotFiles(t, path)
		if code, _ := migrate(t, path); code == 0 || !sameFiles(snap, snapshotFiles(t, path)) {
			t.Fatal("neither-valid was not refused unchanged")
		}
	})
}

// A crash after each step is resumable from the slot bytes alone: before
// the commit the rerun restarts with a full rescan; after it, the rerun
// only finishes the rollover and the bitmap retirement.
func TestMigrateV3CrashAfterEachStepIsResumable(t *testing.T) {
	for _, stop := range []string{"scanned", "backup", "map", "prepared", "committed", "rollover"} {
		t.Run(stop, func(t *testing.T) {
			path := v2Volume(t, true)
			if err := os.WriteFile(path+".chunkbitmap", []byte("c11"), 0o644); err != nil {
				t.Fatal(err)
			}
			migrateStepHook = func(s string) error {
				if s == stop {
					return errors.New("simulated crash after " + s)
				}
				return nil
			}
			code, _ := migrate(t, path)
			migrateStepHook = nil
			if code == 0 {
				t.Fatal("the simulated crash did not stop the tool")
			}
			// Before the commit point (step 5) slot A is still the v2
			// superblock and the v2 bitmap is not retired; after it A is a
			// committed v3 slot.
			crashed, _ := os.ReadFile(path)
			_, v2err := v2legacy.ReadSuperblock(crashed[:4096])
			_, bitmapErr := os.Stat(path + ".chunkbitmap")
			if committed := stop == "committed" || stop == "rollover"; committed {
				if st, _ := storage.ClassifyMigrationSlotV3(crashed[:4096]); st != storage.MigrationSlotCommitted {
					t.Fatalf("crash after %s: slot A state %d, want committed", stop, st)
				}
			} else if v2err != nil || bitmapErr != nil {
				t.Fatalf("crash after %s: A still v2 = %v, bitmap present = %v; want both before the commit", stop, v2err == nil, bitmapErr == nil)
			}
			if code, msg := migrate(t, path); code != 0 {
				t.Fatalf("rerun after a crash at %s: exit %d: %s", stop, code, msg)
			}
			raw, _ := os.ReadFile(path)
			if st, _ := storage.ClassifyMigrationSlotV3(raw[:4096]); st != storage.MigrationSlotCommitted {
				t.Fatalf("after the rerun slot A state %d, want committed", st)
			}
			if st, _ := storage.ClassifyMigrationSlotV3(raw[4096:8192]); st != storage.MigrationSlotCommitted {
				t.Fatalf("after the rerun slot B state %d, want committed", st)
			}
		})
	}
}

// The old binary runs after preparation (A is still v2, so it opens): the
// rerun finds A still v2 and restarts with a full rescan; it refuses while
// the old binary's writes are unflushed and migrates once it flushed and
// closed. A prepared B with a damaged A is ambiguous and refused with
// ErrMigrationIncomplete, writing nothing.
func TestMigrateV3OldBinaryAfterPreparation(t *testing.T) {
	path := v2Volume(t, true)
	migrateStepHook = func(s string) error {
		if s == "prepared" {
			return errors.New("stop after preparation")
		}
		return nil
	}
	migrate(t, path)
	migrateStepHook = nil
	s, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatalf("the old binary refused a prepared-only volume: %v", err)
	}
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Write(9, migBlk(0x99)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	snap := snapshotFiles(t, path)
	if code, msg := migrate(t, path); code == 0 || !strings.Contains(msg, "not cleanly flushed") {
		t.Fatalf("rerun over the old binary's unflushed write: exit %d: %s", code, msg)
	}
	if !sameFiles(snap, snapshotFiles(t, path)) {
		t.Fatal("a refused rerun changed bytes")
	}
	_ = s.Close() // stopped without a flush: still not clean
	if code, _ := migrate(t, path); code == 0 {
		t.Fatal("the rerun migrated a volume the old binary left unflushed")
	}
	// The operator remedy: open with the old binary, flush, close cleanly.
	s2, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if code, msg := migrate(t, path); code != 0 {
		t.Fatalf("rerun after the old binary closed cleanly: exit %d: %s", code, msg)
	}

	t.Run("prepared B with a damaged A", func(t *testing.T) {
		path := v2Volume(t, true)
		migrateStepHook = func(s string) error {
			if s == "prepared" {
				return errors.New("stop after preparation")
			}
			return nil
		}
		migrate(t, path)
		migrateStepHook = nil
		f, _ := os.OpenFile(path, os.O_RDWR, 0)
		_, _ = f.WriteAt(bytes.Repeat([]byte{0xEE}, 4096), 0)
		f.Close()
		snap := snapshotFiles(t, path)
		code, msg := migrate(t, path)
		if code == 0 || !strings.Contains(msg, storage.ErrMigrationIncomplete.Error()) || !sameFiles(snap, snapshotFiles(t, path)) {
			t.Fatalf("exit %d: %s", code, msg)
		}
	})
}

// The tool never reads or restores its backup: a committed volume reruns
// to success with the backup damaged and its slots untouched.
func TestMigrateV3NeverRestoresTheBackup(t *testing.T) {
	path := v2Volume(t, true)
	if code, msg := migrate(t, path); code != 0 {
		t.Fatalf("migrate: %s", msg)
	}
	if err := os.WriteFile(path+".v2-superblock.bak", []byte("damaged"), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if code, msg := migrate(t, path); code != 0 {
		t.Fatalf("rerun: %s", msg)
	}
	now, _ := os.ReadFile(path)
	if !bytes.Equal(raw, now) {
		t.Fatal("a rerun changed the committed volume")
	}
}

// The old v2 WAL starts at offset 4096, over the prepared slot B, so an
// old-binary run can overwrite B partly or wholly (codex03 r8). Either way B
// is an invalid slot, never prepared or committed: with A still v2 the rerun
// restarts and migrates; with A damaged too, neither slot is valid and the
// rerun refuses with no byte changed.
func TestMigrateV3OldWALOverwritesThePreparedSlot(t *testing.T) {
	for _, n := range []int{100, 4096} {
		for _, damageA := range []bool{false, true} {
			name := map[int]string{100: "partial", 4096: "full"}[n]
			if damageA {
				name += " with A damaged"
			}
			t.Run(name, func(t *testing.T) {
				path := v2Volume(t, true)
				migrateStepHook = func(s string) error {
					if s == "prepared" {
						return errors.New("stop after preparation")
					}
					return nil
				}
				migrate(t, path)
				migrateStepHook = nil
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt(bytes.Repeat([]byte{0xA5}, n), 4096); err != nil {
					t.Fatal(err)
				}
				if damageA {
					if _, err := f.WriteAt(bytes.Repeat([]byte{0xEE}, 4096), 0); err != nil {
						t.Fatal(err)
					}
				}
				f.Close()
				raw, _ := os.ReadFile(path)
				if st, _ := storage.ClassifyMigrationSlotV3(raw[4096:8192]); st != storage.MigrationSlotInvalid {
					t.Fatalf("an overwritten B classifies as %d, want invalid", st)
				}
				snap := snapshotFiles(t, path)
				code, msg := migrate(t, path)
				if damageA {
					if code == 0 || !sameFiles(snap, snapshotFiles(t, path)) {
						t.Fatalf("neither valid: exit %d, bytes changed: %s", code, msg)
					}
					return
				}
				if code != 0 {
					t.Fatalf("rerun with A still v2: exit %d: %s", code, msg)
				}
			})
		}
	}
}

func damageSlotA(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xEE}, 4096), 0); err != nil {
		t.Fatal(err)
	}
}

func patchSuperblock(t *testing.T, path string, off int64, v uint64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [8]byte
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

// codex01 row 13b P1 and P2: geometry the v3 slot validation would refuse
// is refused before ANY write (no backup, no map, the volume unchanged):
// an aligned VolumeSize whose extent end overflows, and a v2 WAL size that
// extends past the file end (refused before the scan allocates it).
// Mutants: no checked extent end in ValidateMigrationV3, no file-size
// bound before the WAL read.
func TestMigrateV3BadGeometryRefusedBeforeAnyWrite(t *testing.T) {
	for name, patch := range map[string][2]uint64{
		"extent end overflows":  {24, 1 << 63},           // VolumeSize, block aligned
		"WAL past the file end": {48, 1<<45 + 4096},      // WALSize, v3 size aligned
		"WAL region wraps":      {48, ^uint64(0) - 4095}, // WALSize 2^64-4096: every sum wraps
	} {
		t.Run(name, func(t *testing.T) {
			path := v2Volume(t, true)
			patchSuperblock(t, path, int64(patch[0]), patch[1])
			snap := snapshotFiles(t, path)
			code, msg := migrate(t, path)
			if code == 0 || !strings.Contains(msg, "cannot be migrated in place") {
				t.Fatalf("exit %d: %s", code, msg)
			}
			if !sameFiles(snap, snapshotFiles(t, path)) {
				t.Fatal("a geometry refusal wrote a backup, a map or volume bytes")
			}
		})
	}
}

// codex01 row 13b P2: an earlier run renamed the v2 chunk bitmap but its
// directory fsync failed; the rerun (source gone) still fsyncs the
// directory so the rename is durable. Mutant: treating the missing source
// as done without the fsync fails here.
func TestMigrateV3RetirementRerunFsyncsTheDirectory(t *testing.T) {
	path := v2Volume(t, true)
	if err := os.WriteFile(path+".chunkbitmap", []byte("c11"), 0o644); err != nil {
		t.Fatal(err)
	}
	failed := false
	syncDirHook = func(string) error {
		if _, err := os.Stat(path + ".v2-chunkbitmap.bak"); err == nil && !failed {
			failed = true
			return errors.New("injected directory fsync failure")
		}
		return nil
	}
	defer func() { syncDirHook = nil }()
	if code, _ := migrate(t, path); code == 0 {
		t.Fatal("the injected retirement fsync failure did not fail the tool")
	}
	calls := 0
	syncDirHook = func(string) error { calls++; return nil }
	if code, msg := migrate(t, path); code != 0 {
		t.Fatalf("rerun: exit %d: %s", code, msg)
	}
	if calls == 0 {
		t.Fatal("the rerun did not fsync the directory of the earlier rename")
	}
}
