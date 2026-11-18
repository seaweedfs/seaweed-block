package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/seaweedfs/seaweed-block/cmd/sw-block/internal/v2legacy"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// `sw-block migrate-v3` (format bump v3, DESIGN r17.4 section 10.1; plan row
// 13b): the offline, in-place v2/impl1 -> v3/impl3 descriptor rewrite of a
// cleanly closed volume whose WAL is fully flushed. No record is converted
// and no extent byte is touched. Every step is durable before the next.

// errMigrateNotClean is DESIGN 10.1's ErrMigrateNotClean.
var errMigrateNotClean = errors.New("migrate-v3: the volume is not cleanly flushed: open with the old binary, flush, close cleanly, retry")

// errMigrateNeitherSlot: neither offset 0 nor offset 4096 holds a valid
// slot or v2 superblock; nothing is changed.
var errMigrateNeitherSlot = errors.New("migrate-v3: no valid v2 superblock or v3 descriptor slot; nothing changed")

// migrateStepHook, when set by a test, runs after each durable step with
// its name; returning an error stops the tool there (a simulated crash).
var migrateStepHook func(step string) error

// syncDirHook, when set by a test, runs at every directory fsync; an error
// fails that fsync.
var syncDirHook func(path string) error

func runMigrateV3(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("migrate-v3", flag.ContinueOnError)
	fs.SetOutput(stderr)
	chunkBlocks := fs.Uint64("chunk-blocks", 0, "the provider's chunk size in blocks (the map geometry); required")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 || *chunkBlocks == 0 {
		fmt.Fprintln(stderr, "usage: sw-block migrate-v3 --chunk-blocks N <volume-file>")
		return 2
	}
	if err := migrateV3(fs.Arg(0), *chunkBlocks, stdout); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func migrateV3(path string, chunkBlocks uint64, out io.Writer) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	head := make([]byte, 2*storage.MigrationSlotSize)
	if _, err := f.ReadAt(head, 0); err != nil {
		return fmt.Errorf("migrate-v3: read the descriptor region: %w", err)
	}
	a, b := head[:storage.MigrationSlotSize], head[storage.MigrationSlotSize:]
	sb, v2err := v2legacy.ReadSuperblock(a)
	switch {
	case v2err == nil:
		// Not committed: restart from step 1 with a full rescan (the old
		// binary may have run since; nothing earlier is trusted).
		return migrateFromV2(f, path, sb, chunkBlocks, out)
	case errors.Is(v2err, v2legacy.ErrImplUnsupported):
		return fmt.Errorf("migrate-v3: %w (an impl-2 volume stays on its old binary)", v2err)
	}
	stA, genA := storage.ClassifyMigrationSlotV3(a)
	stB, _ := storage.ClassifyMigrationSlotV3(b)
	switch {
	case stA == storage.MigrationSlotCommitted:
		// Committed. Finish step 6 if B was never rewritten, then step 7.
		if genA == 2 && stB != storage.MigrationSlotCommitted {
			rb, err := storage.RolloverSlotV3(a)
			if err != nil {
				return err
			}
			if err := pwriteSync(f, rb, storage.MigrationSlotBOffset); err != nil {
				return err
			}
		}
		return retireC11(path)
	case stB == storage.MigrationSlotCommitted:
		// Committed and possibly in use: only step 7.
		return retireC11(path)
	case stB == storage.MigrationSlotPrepared:
		// A torn step 5 or an old-binary run after preparation followed by
		// damage to A: the bytes cannot tell; never guess.
		return fmt.Errorf("migrate-v3: %w; nothing changed (an operator decision, using the backup)", storage.ErrMigrationIncomplete)
	default:
		return errMigrateNeitherSlot
	}
}

func migrateFromV2(f *os.File, path string, sb v2legacy.Superblock, chunkBlocks uint64, out io.Writer) error {
	src := storage.MigrationSourceV3{
		UUID: sb.UUID, VolumeSize: sb.VolumeSize, ExtentSize: sb.ExtentSize, BlockSize: sb.BlockSize,
		OldWALOffset: sb.WALOffset, OldWALSize: sb.WALSize, CreatedAt: sb.CreatedAt,
		CheckpointLSN: sb.WALCheckpointLSN, ChunkBlocks: chunkBlocks,
	}
	// Precondition 4 first (read-only): the geometry, including the checked
	// extent end, refuses before the WAL is read or anything is written.
	if err := storage.ValidateMigrationV3(src); err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if size := uint64(fi.Size()); sb.WALOffset > size || sb.WALSize > size-sb.WALOffset { // no wrapping sum
		return fmt.Errorf("%w: the v2 WAL [%d, %d) extends past the file end %d", storage.ErrMigrateGeometry, sb.WALOffset, sb.WALOffset+sb.WALSize, fi.Size())
	}
	// Step 1: precondition 3, read-only and before any write.
	if sb.WALHead != sb.WALTail {
		return fmt.Errorf("%w (WALHead %d != WALTail %d)", errMigrateNotClean, sb.WALHead, sb.WALTail)
	}
	wal := make([]byte, sb.WALSize)
	if _, err := f.ReadAt(wal, int64(sb.WALOffset)); err != nil {
		return fmt.Errorf("migrate-v3: read the v2 WAL: %w", err)
	}
	if found := v2legacy.ScanClean(wal, sb.WALCheckpointLSN); len(found) > 0 {
		return fmt.Errorf("%w (%d unflushed record(s); first at WAL offset %d: %s)", errMigrateNotClean, len(found), found[0].Offset, found[0].Reason)
	}
	if err := step("scanned"); err != nil {
		return err
	}
	// Step 2: the atomic operator backup of the v2 superblock.
	raw := make([]byte, v2legacy.SuperblockSize)
	if _, err := f.ReadAt(raw, 0); err != nil {
		return err
	}
	if err := writeFileAtomic(path+".v2-superblock.bak", raw); err != nil {
		return fmt.Errorf("migrate-v3: backup: %w", err)
	}
	if err := step("backup"); err != nil {
		return err
	}
	// Step 3: the initial map sidecar (all UNKNOWN), durable.
	plan, err := storage.PrepareMigrationV3(path+".chunkmap", src)
	if err != nil {
		return fmt.Errorf("migrate-v3: initial map: %w", err)
	}
	if err := syncDirOf(path); err != nil {
		return err
	}
	if err := step("map"); err != nil {
		return err
	}
	// Step 4: the prepared slot B, over the old WAL's first 4 KiB.
	if err := pwriteSync(f, plan.PreparedB, storage.MigrationSlotBOffset); err != nil {
		return err
	}
	if err := step("prepared"); err != nil {
		return err
	}
	// Step 5: slot A, phase 0: the commit point.
	if err := pwriteSync(f, plan.CommittedA, storage.MigrationSlotAOffset); err != nil {
		return err
	}
	if err := step("committed"); err != nil {
		return err
	}
	// Step 6: slot B rewritten at phase 0 (two-slot redundancy).
	if err := pwriteSync(f, plan.RolloverB, storage.MigrationSlotBOffset); err != nil {
		return err
	}
	if err := step("rollover"); err != nil {
		return err
	}
	// Step 7: retire the v2 C11 bitmap, only after the commit.
	if err := retireC11(path); err != nil {
		return err
	}
	// Step 8.
	fmt.Fprintf(out, "migrated %s to v3: descriptor sha256 %s; backups left in place\n", path, hex.EncodeToString(plan.DescriptorHash[:]))
	return nil
}

func step(name string) error {
	if h := migrateStepHook; h != nil {
		return h(name)
	}
	return nil
}

func pwriteSync(f *os.File, b []byte, off int64) error {
	if _, err := f.WriteAt(b, off); err != nil {
		return fmt.Errorf("migrate-v3: write at %d: %w", off, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("migrate-v3: fsync: %w", err)
	}
	return nil
}

// writeFileAtomic writes b to name via name.tmp, fsync, rename and a
// directory fsync; a leftover .tmp from an earlier run is replaced.
func writeFileAtomic(name string, b []byte) error {
	tmp := name + ".tmp"
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	t, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := t.Write(b); err != nil {
		t.Close()
		return err
	}
	if err := t.Sync(); err != nil {
		t.Close()
		return err
	}
	if err := t.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		return err
	}
	return syncDirOf(name)
}

// retireC11 is step 7: rename a v2 .chunkbitmap sidecar to
// <file>.v2-chunkbitmap.bak and fsync the directory. Absent is done.
func retireC11(path string) error {
	err := os.Rename(path+".chunkbitmap", path+".v2-chunkbitmap.bak")
	if errors.Is(err, os.ErrNotExist) {
		if _, serr := os.Stat(path + ".v2-chunkbitmap.bak"); serr != nil {
			return nil // there was no v2 bitmap
		}
		// An earlier run renamed it; its directory fsync may have failed, so
		// make the rename durable now (codex01 row 13b P2).
		return syncDirOf(path)
	}
	if err != nil {
		return fmt.Errorf("migrate-v3: retire the v2 chunk bitmap: %w", err)
	}
	return syncDirOf(path)
}

// syncDirOf fsyncs the directory holding path. A directory cannot be
// fsynced on Windows; there it is a no-op, as in the provider.
func syncDirOf(path string) error {
	if h := syncDirHook; h != nil {
		if err := h(path); err != nil {
			return err
		}
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
