package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestCutCOWV3_FreshImageFourTimesSpillLimitPublishes(t *testing.T) {
	for _, bound := range []int{0, 4} {
		name := "cow"
		if bound != 0 {
			name = "stage"
		}
		t.Run(name, func(t *testing.T) {
			s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), ctNumBlocks, ctBlockSize, WALStoreOptions{ChunkBlocks: ctChunkBlocks})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err = s.Recover(); err != nil {
				t.Fatal(err)
			}
			cfg := ctConfig(1)
			cfg.Bitmap = nil
			cfg.BarrierBound = bound
			cfg.UploadConcurrency = 2
			imageBytes := int64(ctNumBlocks) * int64(ctBlockSize)
			cfg.COWSpillLimit = imageBytes / 4
			f := newFakeChunkRemote()
			l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
			want := make([]byte, imageBytes)
			for lba := uint32(0); lba < ctNumBlocks; lba++ {
				data := makeBlock(ctBlockSize, byte(lba+1))
				if _, err = s.Write(lba, data); err != nil {
					t.Fatal(err)
				}
				copy(want[int(lba)*ctBlockSize:], data)
			}
			result, err := l.Cut(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(f.imageOf(t, result.Head), want) {
				t.Fatal("first HEAD does not match submitted bytes")
			}
			if bound != 0 && result.Stats.StageRounds == 0 {
				t.Fatal("pre-copy path was not exercised")
			}
			files, err := filepath.Glob(s.path + ".cut-cow-*")
			if err != nil || len(files) != 0 {
				t.Fatalf("unexpected scratch files: %v %v", files, err)
			}
		})
	}
}
