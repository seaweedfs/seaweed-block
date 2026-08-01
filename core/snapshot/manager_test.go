package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

func TestPhase175ManagerCreateReopenReadDelete(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenManager(root)
	if err != nil {
		t.Fatal(err)
	}
	source, err := smartwal.CreateStore(filepath.Join(t.TempDir(), "source.bin"), 4, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	blockA := testBlock(0x11)
	blockB := testBlock(0x22)
	if _, err := source.Write(0, blockA); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(3, blockB); err != nil {
		t.Fatal(err)
	}

	rec, err := manager.Create(context.Background(), CreateRequest{Name: "daily-a", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != StateReady || rec.Frontier != 2 || rec.RecordCount != 2 || rec.DataBytes != 8192 || rec.SizeBytes != 16384 {
		t.Fatalf("record=%+v", rec)
	}
	retry, err := manager.Create(context.Background(), CreateRequest{Name: "daily-a", SourceVolumeID: "vol-a"}, source)
	if err != nil || retry.SnapshotID != rec.SnapshotID || !retry.CreatedAt.Equal(rec.CreatedAt) {
		t.Fatalf("idempotent retry=%+v err=%v", retry, err)
	}
	if _, err := manager.Create(context.Background(), CreateRequest{Name: "daily-a", SourceVolumeID: "vol-b"}, source); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("name conflict error=%v", err)
	}

	reopened, err := OpenManager(root)
	if err != nil {
		t.Fatal(err)
	}
	listed := reopened.List("vol-a")
	if len(listed) != 1 || listed[0].SnapshotID != rec.SnapshotID {
		t.Fatalf("list=%+v", listed)
	}
	blocks := make(map[uint32][]byte)
	cut, err := reopened.ReadBlocks(context.Background(), rec.SnapshotID, func(lba uint32, data []byte) error {
		blocks[lba] = append([]byte(nil), data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cut.Frontier != 2 || string(blocks[0]) != string(blockA) || string(blocks[3]) != string(blockB) {
		t.Fatalf("cut=%+v blocks=%v", cut, blockKeys(blocks))
	}
	if err := reopened.Delete(rec.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Delete(rec.SnapshotID); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	final, err := OpenManager(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := final.List(""); len(got) != 0 {
		t.Fatalf("snapshots after delete=%+v", got)
	}
}

func TestPhase175ManagerDoesNotPublishPartialCapture(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenManager(root)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Create(context.Background(), CreateRequest{Name: "broken", SourceVolumeID: "vol-a"}, failingSource{})
	if err == nil {
		t.Fatal("partial capture succeeded")
	}
	if got := manager.List(""); len(got) != 0 {
		t.Fatalf("partial capture published records: %+v", got)
	}
	for _, dir := range []string{filepath.Join(root, "archives"), filepath.Join(root, "records")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("partial capture left files in %s: %v", dir, entries)
		}
	}
}

func TestPhase175ManagerRefusesCorruptArchiveOnReadAndRestart(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenManager(root)
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(2, 4096)
	if _, err := source.Write(0, testBlock(0x66)); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "corrupt-me", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "archives", rec.SnapshotID+".sbsnap")
	f, err := os.OpenFile(archive, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, archiveHeaderSize+recordHeaderSize+17); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReadBlocks(context.Background(), rec.SnapshotID, nil); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("read corrupt archive error=%v", err)
	}
	if _, err := OpenManager(root); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("restart corrupt archive error=%v", err)
	}
}

func TestPhase175ManagerCleansOwnedTemporaryAndOrphanFiles(t *testing.T) {
	root := t.TempDir()
	archives := filepath.Join(root, "archives")
	records := filepath.Join(root, "records")
	if err := os.MkdirAll(archives, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(records, 0o755); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		filepath.Join(archives, ".tmp-create-crash"),
		filepath.Join(records, ".tmp-record-crash"),
		filepath.Join(archives, "snap-orphan.sbsnap"),
	}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := OpenManager(root); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned residue not removed %s: %v", path, err)
		}
	}
}

func TestPhase175RestoreToNewVolumeIsDurableAndIsolated(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source, err := smartwal.CreateStore(filepath.Join(t.TempDir(), "source.bin"), 4, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	atCut := testBlock(0x31)
	afterCut := testBlock(0x32)
	other := testBlock(0x41)
	if _, err := source.Write(0, atCut); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(2, other); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "restore-a", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(0, afterCut); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Sync(); err != nil {
		t.Fatal(err)
	}

	targetPath := filepath.Join(t.TempDir(), "restored.bin")
	result, err := manager.RestoreToNew(context.Background(), rec.SnapshotID, targetPath, func(path string, numBlocks uint32, blockSize int) (storage.LogicalStorage, error) {
		return smartwal.CreateStore(path, numBlocks, blockSize)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceFrontier != 2 || result.TargetFrontier != 2 || result.RestoredBlocks != 2 || result.RestoredBytes != 8192 {
		t.Fatalf("restore result=%+v", result)
	}
	restored, err := smartwal.OpenStore(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if frontier, err := restored.Recover(); err != nil || frontier != 2 {
		t.Fatalf("recover frontier=%d err=%v", frontier, err)
	}
	got, err := restored.Read(0)
	if err != nil || string(got) != string(atCut) {
		t.Fatal("restored volume did not preserve cut bytes")
	}
	got, err = restored.Read(2)
	if err != nil || string(got) != string(other) {
		t.Fatal("restored volume lost second block")
	}
	if _, err := restored.Write(0, testBlock(0x51)); err != nil {
		t.Fatal(err)
	}
	liveSource, err := source.Read(0)
	if err != nil || string(liveSource) != string(afterCut) {
		t.Fatal("restored write changed live source")
	}
	snapshotBlocks := make(map[uint32][]byte)
	if _, err := manager.ReadBlocks(context.Background(), rec.SnapshotID, func(lba uint32, data []byte) error {
		snapshotBlocks[lba] = append([]byte(nil), data...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if string(snapshotBlocks[0]) != string(atCut) {
		t.Fatal("restored write changed immutable snapshot")
	}
}

func TestPhase175RestoreFailureLeavesNoPublishedTarget(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenManager(filepath.Join(root, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(3, 4096)
	if _, err := source.Write(0, testBlock(0x61)); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(1, testBlock(0x62)); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "restore-fail", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(root, "target.bin")
	_, err = manager.RestoreToNew(context.Background(), rec.SnapshotID, targetPath, func(path string, numBlocks uint32, blockSize int) (storage.LogicalStorage, error) {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			return nil, err
		}
		return &failSecondWrite{LogicalStorage: storage.NewBlockStore(numBlocks, blockSize)}, nil
	})
	if err == nil {
		t.Fatal("injected restore failure succeeded")
	}
	if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed restore published target: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".tmp-restore-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("failed restore temp residue=%v err=%v", matches, err)
	}
}

func TestPhase175RestoreDoesNotOverwriteExistingTarget(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(1, 4096)
	if _, err := source.Write(0, testBlock(0x63)); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "no-overwrite", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(t.TempDir(), "existing.bin")
	want := []byte("owned-by-another-operation")
	if err := os.WriteFile(targetPath, want, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = manager.RestoreToNew(context.Background(), rec.SnapshotID, targetPath, func(path string, numBlocks uint32, blockSize int) (storage.LogicalStorage, error) {
		return smartwal.CreateStore(path, numBlocks, blockSize)
	})
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing target error=%v", err)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil || string(got) != string(want) {
		t.Fatalf("existing target changed got=%q err=%v", got, err)
	}
}

func TestPhase175RestoreToNewWALStoreSurvivesRecovery(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(2, 4096)
	want := testBlock(0x68)
	if _, err := source.Write(1, want); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "wal-restore", SourceVolumeID: "vol-wal"}, source)
	if err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(t.TempDir(), "restored.bin")
	result, err := manager.RestoreToNew(context.Background(), rec.SnapshotID, targetPath, func(path string, numBlocks uint32, blockSize int) (storage.LogicalStorage, error) {
		return storage.CreateWALStore(path, numBlocks, blockSize)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TargetFrontier != 1 {
		t.Fatalf("target frontier=%d want 1", result.TargetFrontier)
	}
	restored, err := storage.OpenWALStore(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if frontier, err := restored.Recover(); err != nil || frontier != 1 {
		t.Fatalf("recover frontier=%d err=%v", frontier, err)
	}
	got, err := restored.Read(1)
	if err != nil || string(got) != string(want) {
		t.Fatal("WALStore restore did not survive reopen/recovery")
	}
}

func TestPhase175DeleteRefusesActiveArchiveReader(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(1, 4096)
	if _, err := source.Write(0, testBlock(0x71)); err != nil {
		t.Fatal(err)
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "in-use", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		_, err := manager.ReadBlocks(context.Background(), rec.SnapshotID, func(uint32, []byte) error {
			close(entered)
			<-release
			return nil
		})
		readDone <- err
	}()
	<-entered
	if err := manager.Delete(rec.SnapshotID); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete active reader error=%v", err)
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(rec.SnapshotID); err != nil {
		t.Fatal(err)
	}
}

type failingSource struct{}

func (failingSource) CaptureSnapshot(_ context.Context, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	if err := sink(0, testBlock(0x77)); err != nil {
		return storage.SnapshotCut{}, err
	}
	return storage.SnapshotCut{}, errors.New("injected capture failure")
}

type failSecondWrite struct {
	storage.LogicalStorage
	writes int
}

func (s *failSecondWrite) Write(lba uint32, data []byte) (uint64, error) {
	s.writes++
	if s.writes == 2 {
		return 0, errors.New("injected target write failure")
	}
	return s.LogicalStorage.Write(lba, data)
}

func testBlock(value byte) []byte {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = value
	}
	return data
}

func blockKeys(blocks map[uint32][]byte) []uint32 {
	keys := make([]uint32, 0, len(blocks))
	for key := range blocks {
		keys = append(keys, key)
	}
	return keys
}
