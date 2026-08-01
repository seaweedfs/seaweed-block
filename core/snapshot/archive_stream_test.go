package snapshot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175StreamArchiveRoundTripAndApply(t *testing.T) {
	manager, rec, want := createStreamFixture(t)
	var archive bytes.Buffer
	streamed, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive)
	if err != nil {
		t.Fatal(err)
	}
	if streamed != rec || int64(archive.Len()) != rec.ArchiveBytes {
		t.Fatalf("streamed=%+v bytes=%d", streamed, archive.Len())
	}

	target := storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)
	cut, err := ApplyArchiveStream(context.Background(), bytes.NewReader(archive.Bytes()), rec, func(lba uint32, data []byte) error {
		_, err := target.Write(lba, data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if cut.Frontier != rec.Frontier || cut.BlockCount != rec.RecordCount {
		t.Fatalf("cut=%+v", cut)
	}
	for lba, expected := range want {
		got, err := target.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("LBA %d mismatch", lba)
		}
	}
}

func TestPhase175ApplyArchiveStreamFailsClosedOnInvalidBytes(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	var archive bytes.Buffer
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &archive); err != nil {
		t.Fatal(err)
	}
	original := archive.Bytes()
	tests := []struct {
		name string
		data []byte
		rec  Record
	}{
		{name: "truncated", data: append([]byte(nil), original[:len(original)-1]...), rec: rec},
		{name: "trailing", data: append(append([]byte(nil), original...), 0xff), rec: rec},
		{name: "corrupt", data: corruptCopy(original, archiveHeaderSize+recordHeaderSize+3), rec: rec},
		{name: "catalog-mismatch", data: append([]byte(nil), original...), rec: changedGeometry(rec)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writes := 0
			_, err := ApplyArchiveStream(context.Background(), bytes.NewReader(tc.data), tc.rec, func(uint32, []byte) error {
				writes++
				return nil
			})
			if !errors.Is(err, ErrArchiveCorrupt) {
				t.Fatalf("error=%v", err)
			}
			if tc.name == "catalog-mismatch" && writes != 0 {
				t.Fatalf("catalog mismatch applied %d blocks", writes)
			}
		})
	}
}

func TestPhase175StreamArchiveHoldsDeleteLease(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	w := &blockingArchiveWriter{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := manager.StreamArchive(context.Background(), rec.SnapshotID, w)
		done <- err
	}()
	<-w.started
	if err := manager.Delete(rec.SnapshotID); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete during stream error=%v", err)
	}
	close(w.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(rec.SnapshotID); err != nil {
		t.Fatal(err)
	}
}

func TestPhase175StreamArchiveDetectsCatalogDamage(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	path := manager.archivePath(rec.SnapshotID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, archiveHeaderSize+recordHeaderSize+3); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StreamArchive(context.Background(), rec.SnapshotID, &bytes.Buffer{}); !errors.Is(err, ErrArchiveCorrupt) {
		t.Fatalf("stream damaged archive error=%v", err)
	}
}

type blockingArchiveWriter struct {
	started chan struct{}
	release chan struct{}
}

func (w *blockingArchiveWriter) Write(p []byte) (int, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-w.release
	return len(p), nil
}

func createStreamFixture(t *testing.T) (*Manager, Record, map[uint32][]byte) {
	t.Helper()
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := storage.NewBlockStore(4, 4096)
	want := map[uint32][]byte{0: testBlock(0x61), 3: testBlock(0x63)}
	for lba, data := range want {
		if _, err := source.Write(lba, data); err != nil {
			t.Fatal(err)
		}
	}
	rec, err := manager.Create(context.Background(), CreateRequest{Name: "stream-a", SourceVolumeID: "vol-a"}, source)
	if err != nil {
		t.Fatal(err)
	}
	return manager, rec, want
}

func corruptCopy(data []byte, offset int) []byte {
	out := append([]byte(nil), data...)
	out[offset] ^= 0xff
	return out
}

func changedGeometry(rec Record) Record {
	rec.NumBlocks++
	rec.SizeBytes = uint64(rec.NumBlocks) * uint64(rec.BlockSize)
	return rec
}
