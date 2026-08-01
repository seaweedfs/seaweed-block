package storage_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

func TestPhase175SnapshotCutBlocksConcurrentMutations(t *testing.T) {
	tests := []struct {
		name string
		new  func(*testing.T) storage.LogicalStorage
	}{
		{
			name: "memory",
			new: func(*testing.T) storage.LogicalStorage {
				return storage.NewBlockStore(4, 4096)
			},
		},
		{
			name: "walstore",
			new: func(t *testing.T) storage.LogicalStorage {
				s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "volume.bin"), 4, 4096)
				if err != nil {
					t.Fatal(err)
				}
				return s
			},
		},
		{
			name: "smartwal",
			new: func(t *testing.T) storage.LogicalStorage {
				s, err := smartwal.CreateStore(filepath.Join(t.TempDir(), "volume.bin"), 4, 4096)
				if err != nil {
					t.Fatal(err)
				}
				return s
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.new(t)
			t.Cleanup(func() { _ = s.Close() })
			snapper, ok := s.(storage.SnapshotSource)
			if !ok {
				t.Fatalf("%T does not implement SnapshotSource", s)
			}

			beforeA := filledBlock(0x11)
			beforeB := filledBlock(0x22)
			afterA := filledBlock(0x33)
			if _, err := s.Write(0, beforeA); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(1, beforeB); err != nil {
				t.Fatal(err)
			}

			entered := make(chan struct{})
			release := make(chan struct{})
			type captureResult struct {
				cut    storage.SnapshotCut
				blocks map[uint32][]byte
				err    error
			}
			captured := make(chan captureResult, 1)
			go func() {
				blocks := make(map[uint32][]byte)
				cut, err := snapper.CaptureSnapshot(context.Background(), func(lba uint32, data []byte) error {
					blocks[lba] = append([]byte(nil), data...)
					if lba == 0 {
						close(entered)
						<-release
					}
					return nil
				})
				captured <- captureResult{cut: cut, blocks: blocks, err: err}
			}()

			<-entered
			writeDone := make(chan error, 1)
			go func() {
				_, err := s.Write(0, afterA)
				writeDone <- err
			}()
			select {
			case err := <-writeDone:
				t.Fatalf("write crossed snapshot barrier: %v", err)
			case <-time.After(50 * time.Millisecond):
			}

			close(release)
			result := <-captured
			if result.err != nil {
				t.Fatalf("CaptureSnapshot: %v", result.err)
			}
			if err := <-writeDone; err != nil {
				t.Fatalf("post-cut write: %v", err)
			}
			if result.cut.Frontier != 2 || result.cut.BlockCount != 2 || result.cut.DataBytes != 8192 {
				t.Fatalf("cut=%+v", result.cut)
			}
			if got := result.blocks[0]; string(got) != string(beforeA) {
				t.Fatal("snapshot contains post-cut bytes")
			}
			if got := result.blocks[1]; string(got) != string(beforeB) {
				t.Fatal("snapshot lost a pre-cut block")
			}
			live, err := s.Read(0)
			if err != nil {
				t.Fatal(err)
			}
			if string(live) != string(afterA) {
				t.Fatal("post-cut write did not reach live source")
			}
		})
	}
}

func TestPhase175SnapshotCutBlocksDirectExtentInstall(t *testing.T) {
	s, err := smartwal.CreateStore(filepath.Join(t.TempDir(), "volume.bin"), 2, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Write(0, filledBlock(0x44)); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	captureDone := make(chan error, 1)
	go func() {
		_, err := s.CaptureSnapshot(context.Background(), func(lba uint32, data []byte) error {
			close(entered)
			<-release
			return nil
		})
		captureDone <- err
	}()
	<-entered

	directDone := make(chan error, 1)
	go func() { directDone <- s.WriteExtentDirect(1, filledBlock(0x55)) }()
	select {
	case err := <-directDone:
		t.Fatalf("direct extent install crossed snapshot barrier: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-captureDone; err != nil {
		t.Fatal(err)
	}
	if err := <-directDone; err != nil {
		t.Fatal(err)
	}
}

func filledBlock(value byte) []byte {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = value
	}
	return data
}
