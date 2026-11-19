package storage

import (
	"bytes"
	"context"
	"testing"
	"time"
)

type slowSuccessfulUploadRemote struct{ *fakeChunkRemote }

func (r *slowSuccessfulUploadRemote) Put(ctx context.Context, key string, data []byte) error {
	timer := time.NewTimer(600 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return r.fakeChunkRemote.Put(ctx, key, data)
	}
}

func TestChunkCutLegitimateUploadsExceedTwoSeconds(t *testing.T) {
	for _, detach := range []bool{false, true} {
		name := "cut"
		if detach {
			name = "detach"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			f := newFakeChunkRemote()
			r := &slowSuccessfulUploadRemote{f}
			cfg := ctConfig(1)
			cfg.UploadConcurrency = 1
			l := mustAttach(t, s, r, cfg).(*chunkLayerImpl)
			want := make([]byte, ctNumBlocks*ctBlockSize)
			for i := 0; i < 5; i++ {
				data := makeBlock(ctBlockSize, byte(0x41+i))
				if _, err := s.Write(uint32(i*ctChunkBlocks), data); err != nil {
					t.Fatal(err)
				}
				copy(want[i*ctChunkBlocks*ctBlockSize:], data)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			start := time.Now()
			var err error
			if detach {
				_, err = l.Detach(ctx)
			} else {
				_, err = l.Cut(ctx)
			}
			if err != nil {
				t.Fatalf("legitimate multi-object work canceled: %v", err)
			}
			if time.Since(start) <= 2*time.Second {
				t.Fatal("fixture did not exceed the former whole-cut limit")
			}
			heads := f.installedHeads()
			if len(heads) != 1 || !bytes.Equal(f.imageOf(t, heads[0]), want) {
				t.Fatal("slow successful cut did not publish the exact image once")
			}
			if detach && heads[0].State != ChunkHeadDetached {
				t.Fatal("slow detach did not install detached proof")
			}
		})
	}
}
