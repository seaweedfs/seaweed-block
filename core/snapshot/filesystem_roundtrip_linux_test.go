//go:build linux

package snapshot

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175WALStoreRestorePreservesExt4Image(t *testing.T) {
	mkfs, err := exec.LookPath("mkfs.ext4")
	if err != nil {
		t.Skip("mkfs.ext4 is not installed")
	}
	e2fsck, err := exec.LookPath("e2fsck")
	if err != nil {
		t.Skip("e2fsck is not installed")
	}

	const (
		imageSize = 1 << 20
		blockSize = 4096
		numBlocks = imageSize / blockSize
	)
	root := t.TempDir()
	sourceImage := filepath.Join(root, "source.ext4")
	if err := os.WriteFile(sourceImage, make([]byte, imageSize), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(mkfs, "-F", "-b", "1024", sourceImage).CombinedOutput(); err != nil {
		t.Fatalf("mkfs.ext4: %v: %s", err, output)
	}
	want, err := os.ReadFile(sourceImage)
	if err != nil {
		t.Fatal(err)
	}

	sourceStore, err := storage.CreateWALStore(filepath.Join(root, "source.bin"), numBlocks, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	zeroBlock := make([]byte, blockSize)
	for lba := uint32(0); lba < numBlocks; lba++ {
		block := want[int(lba)*blockSize : int(lba+1)*blockSize]
		if bytes.Equal(block, zeroBlock) {
			continue
		}
		if _, err := sourceStore.Write(lba, block); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sourceStore.Sync(); err != nil {
		t.Fatal(err)
	}

	manager, err := OpenManager(filepath.Join(root, "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.Create(context.Background(), CreateRequest{Name: "ext4-roundtrip", SourceVolumeID: "source-vol"}, sourceStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceStore.Close(); err != nil {
		t.Fatal(err)
	}

	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), record.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(root, "target.bin")
	target, err := OpenRestoreTarget(RestoreTargetConfig{
		MarkerPath:      filepath.Join(root, "target.restore.json"),
		TargetDataPath:  targetPath,
		SnapshotID:      record.SnapshotID,
		TargetVolumeID:  "target-vol",
		TargetReplicaID: "r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	targetStore, err := storage.CreateWALStore(targetPath, numBlocks, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.BindStorage(targetStore); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Apply(context.Background(), bytes.NewReader(archive.Bytes()), record); err != nil {
		t.Fatal(err)
	}
	if err := targetStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := storage.OpenWALStore(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, imageSize)
	for lba := uint32(0); lba < numBlocks; lba++ {
		block, err := reopened.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		copy(got[int(lba)*blockSize:], block)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restored ext4 image differs from source")
	}
	restoredImage := filepath.Join(root, "restored.ext4")
	if err := os.WriteFile(restoredImage, got, 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(e2fsck, "-fn", restoredImage).CombinedOutput(); err != nil {
		t.Fatalf("e2fsck restored image: %v: %s", err, output)
	}
}
