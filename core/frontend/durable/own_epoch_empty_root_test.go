package durable_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func ownEpochProvider(t *testing.T, root string, remote storage.ChunkRemote, holder string, epoch uint64) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root,
		BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkHolder:    holder,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, viewAs("r1", epoch))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// An own-epoch HEAD is a record that this holder already claimed the volume.
// An empty replacement root cannot prove it still has the acknowledged suffix.
// Refuse before seeding or claiming, even when the last remote image is empty.
func TestOwnEpochEmptyLocalRootRefusesBeforeClaim(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty_remote_image", true: "published_remote_image"}[published], func(t *testing.T) {
			ctx := context.Background()
			remote := storage.NewMemChunkRemote()
			first := ownEpochProvider(t, t.TempDir(), remote, "s1/r1", 3)
			openRecovered(t, first)
			backend := first.Backend("v1")
			if published {
				if n, err := backend.Write(ctx, 4096, block('A')); err != nil || n != 4096 {
					t.Fatalf("write A: n=%d err=%v", n, err)
				}
				if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := backend.Write(ctx, 2*4096, block('B')); err != nil || n != 4096 {
				t.Fatalf("write B: n=%d err=%v", n, err)
			}
			if err := backend.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			localLSN, _, _ := firstStore(t, first).Boundaries()
			before := headHolder(t, remote)
			if before.Holder != "s1/r1" || before.Epoch != 3 || localLSN <= before.RemoteLSN {
				t.Fatalf("fixture lacks acknowledged suffix: HEAD=%+v local=%d", before, localLSN)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			observed := &casObservedRemote{ChunkRemote: remote}
			replacement := ownEpochProvider(t, t.TempDir(), observed, "s1/r1", 3)
			store, err := replacement.EnsureStorage("v1")
			if err != nil {
				t.Fatal(err)
			}
			_, err = replacement.RecoverVolume(ctx, "v1")
			if err == nil {
				got := make([]byte, 4096)
				n, readErr := replacement.Backend("v1").Read(ctx, 2*4096, got)
				t.Fatalf("empty root at its own epoch served without a move: HEAD R=%d acknowledged L=%d; guest read n=%d err=%v lost_B=%t", before.RemoteLSN, localLSN, n, readErr, !bytes.Equal(got, block('B')))
			}
			if observed.cass != 0 || !reflect.DeepEqual(before, headHolder(t, remote)) {
				t.Fatal("refused empty root changed HEAD")
			}
			if err := replacement.Backend("v1").AdmitsIO(); err == nil {
				t.Fatal("refused empty root admits guest I/O")
			}
			if local, _, _ := store.(*storage.WALStore).Boundaries(); local != 0 {
				t.Fatalf("refusal seeded the empty local root to %d", local)
			}
		})
	}
}

func firstStore(t *testing.T, p *durable.DurableProvider) *storage.WALStore {
	t.Helper()
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	return s.(*storage.WALStore)
}
