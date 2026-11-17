//go:build swblock_format_v3_test

package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type formatV3View struct{}

func (formatV3View) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
}

func mappedProviderV3(t *testing.T, root string, remote storage.ChunkRemote) *DurableProvider {
	t.Helper()
	p, err := NewDurableProvider(ProviderConfig{
		Impl: ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		WALRecoveryTestDisableFlusher: true,
		ChunkRemoteFor:                func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, formatV3View{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	p.testWALStoreFactory = func(path string, create bool) (*storage.WALStore, error) {
		if create {
			return storage.CreateMappedWALStoreV3Test(path, 64, 4096, 4)
		}
		return storage.OpenMappedWALStoreV3Test(path)
	}
	return p
}

func TestDurableProviderV3_MappedCheckpointCaptureAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	remote := storage.NewMemChunkRemote()
	p := mappedProviderV3(t, root, remote)
	st, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	b := p.Backend("v1")
	data := bytes.Repeat([]byte{0x57}, 4096)
	if n, err := b.Write(ctx, 5*4096, data); err != nil || n != len(data) {
		t.Fatalf("Write: %d %v", n, err)
	}
	if err = b.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = storage.FlushMappedWALStoreV3Test(st.(*storage.WALStore)); err != nil {
		t.Fatal(err)
	}
	if _, err = p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	check := func(want []byte) {
		t.Helper()
		head, e := remote.GetHead(ctx)
		if e != nil {
			t.Fatal(e)
		}
		ref, ok := head.Chunks[1]
		if !ok {
			t.Fatal("capture omitted checkpointed chunk")
		}
		chunk, e := remote.Get(ctx, ref.Key)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(chunk[4096:8192], want) {
			t.Fatal("published chunk bytes differ")
		}
	}
	check(data)
	raw, err := os.ReadFile(p.volumePath("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(raw[12:16]) != 3 {
		t.Fatal("provider did not open impl3")
	}
	if _, err = os.Stat(p.volumePath("v1") + ".chunkbitmap"); !os.IsNotExist(err) {
		t.Fatalf("legacy bitmap exists: %v", err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := mappedProviderV3(t, root, remote)
	st2, err := p2.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	b2 := p2.Backend("v1")
	got := make([]byte, 4096)
	if n, err := b2.Read(ctx, 5*4096, got); err != nil || n != len(got) || !bytes.Equal(got, data) {
		t.Fatalf("reopen read %d %v equal=%v", n, err, bytes.Equal(got, data))
	}
	next := bytes.Repeat([]byte{0x93}, 4096)
	if _, err = b2.Write(ctx, 5*4096, next); err != nil {
		t.Fatal(err)
	}
	if err = b2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = storage.FlushMappedWALStoreV3Test(st2.(*storage.WALStore)); err != nil {
		t.Fatal(err)
	}
	before := remote.Puts()
	if _, err = p2.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if remote.Puts()-before != 1 {
		t.Fatalf("incremental capture uploaded %d chunks", remote.Puts()-before)
	}
	check(next)
}
