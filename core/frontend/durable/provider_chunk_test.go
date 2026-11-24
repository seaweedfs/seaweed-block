package durable_test

// walstore-chunk provider wiring, against the storage package's in-memory
// remote. The chunk layer's own contract (C1-C11) is tested in
// core/storage; these tests cover the provider: selection, attach after
// recovery, fresh host vs existing volume, the capture gate and lifetime.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	pcBlocks      = 256
	pcChunkBlocks = 16
)

func chunkProvider(t *testing.T, root string, remote storage.ChunkRemote, view *stubView, interval time.Duration) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:                 durable.ImplWALStoreChunk,
		StorageRoot:          root,
		BlockSize:            4096,
		NumBlocks:            pcBlocks,
		ChunkBlocks:          pcChunkBlocks,
		ChunkCaptureInterval: interval,
		ChunkRemoteFor: func(volumeID string) (storage.ChunkRemote, error) {
			return remote, nil
		},
	}, view)
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func openRecovered(t *testing.T, p *durable.DurableProvider) storage.LogicalStorage {
	t.Helper()
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatalf("EnsureStorage: %v", err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("RecoverVolume: %v", err)
	}
	return s
}

// freshV1Claim is only for fresh v1View fixtures, never reopened/local roots.
// INV-WS-CLAIM-BEFORE-SERVE-001: validate the open before using its baseline.
func freshV1Claim(t *testing.T, remote *storage.MemChunkRemote) storage.ChunkHead {
	t.Helper()
	head := headHolder(t, remote)
	assigned := v1View().Projection()
	if head.Gen != 1 || head.Epoch != assigned.Epoch || head.Holder != assigned.ReplicaID ||
		head.State != storage.ChunkHeadActive || head.RemoteLSN != 0 || head.DetachedBy != nil ||
		len(head.Chunks) != 0 || len(head.Segments) != 0 || head.ManifestChunkCount != 0 || head.ChunkBytes != 0 || remote.Puts() != 0 {
		t.Fatalf("fresh open did not establish exactly one empty assigned active claim: HEAD=%+v PUTs=%d", head, remote.Puts())
	}
	return head
}

func block(fill byte) []byte { return bytes.Repeat([]byte{fill}, 4096) }

func headBlock(t *testing.T, remote *storage.MemChunkRemote, lba uint32) []byte {
	t.Helper()
	head, err := remote.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := head.Chunks[uint64(lba/pcChunkBlocks)]
	if !ok {
		return make([]byte, 4096)
	}
	b, err := remote.Get(context.Background(), ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	off := int(lba%pcChunkBlocks) * 4096
	return b[off : off+4096]
}

func v1View() *stubView {
	return newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}))
}

func TestWALStoreChunk_RequiresRemote(t *testing.T) {
	_, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks,
	}, v1View())
	if err == nil {
		t.Fatal("walstore-chunk without ChunkRemoteFor was accepted")
	}
}

// Capture publishes the volume; after a provider restart over the same
// local root, the volume is still authoritative and the next cut is
// incremental.
func TestWALStoreChunk_CaptureAndRestart(t *testing.T) {
	root := t.TempDir()
	remote := storage.NewMemChunkRemote()
	view := v1View()
	p := chunkProvider(t, root, remote, view, 0)
	s := openRecovered(t, p)
	for lba := uint32(0); lba < 40; lba += 8 { // chunks 0, 0, 1, 1, 2
		if _, err := s.Write(lba, block(byte(lba+1))); err != nil {
			t.Fatal(err)
		}
	}
	h, err := p.CaptureVolume(context.Background(), "v1")
	if err != nil {
		t.Fatalf("CaptureVolume: %v", err)
	}
	head, _ := remote.GetHead(context.Background())
	if head.RemoteLSN != h || head.Epoch != 3 || head.Holder != "r1" || len(head.Chunks) != 3 {
		t.Fatalf("HEAD after capture: remote_lsn %d (want %d) epoch %d holder %q chunks %d", head.RemoteLSN, h, head.Epoch, head.Holder, len(head.Chunks))
	}
	for lba := uint32(0); lba < 40; lba += 8 {
		if !bytes.Equal(headBlock(t, remote, lba), block(byte(lba+1))) {
			t.Fatalf("HEAD LBA %d differs", lba)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := chunkProvider(t, root, remote, view, 0)
	s2 := openRecovered(t, p2)
	if got, _ := s2.Read(16); !bytes.Equal(got, block(17)) {
		t.Fatal("restarted volume lost local data")
	}
	if _, err := s2.Write(100, block(0xAB)); err != nil { // chunk 6
		t.Fatal(err)
	}
	before := remote.Puts()
	if _, err := p2.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("CaptureVolume after restart: %v", err)
	}
	if n := remote.Puts() - before; n != 1 {
		t.Fatalf("cut after restart stored %d chunks, want 1 (incremental)", n)
	}
	if !bytes.Equal(headBlock(t, remote, 100), block(0xAB)) {
		t.Fatal("HEAD lacks the post-restart write")
	}
}

// A provider that creates the volume (a new host) starts from the remote
// HEAD and reads lazily.
func TestWALStoreChunk_FreshHostReadsFromHead(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	view := v1View()
	p := chunkProvider(t, t.TempDir(), remote, view, 0)
	s := openRecovered(t, p)
	if _, err := s.Write(33, block(0x33)); err != nil {
		t.Fatal(err)
	}
	if err := p.MakeVolumeRemoteDurable(context.Background(), "v1", 1); err != nil {
		t.Fatalf("MakeVolumeRemoteDurable: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := chunkProvider(t, t.TempDir(), remote, viewAs("r1", 4), 0) // higher issued epoch, empty local root
	s2 := openRecovered(t, fresh)
	if got, err := s2.Read(33); err != nil || !bytes.Equal(got, block(0x33)) {
		t.Fatalf("fresh host read: %v", err)
	}
}

func TestWALStoreChunk_CaptureNeedsHealthyProjection(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	view := v1View()
	p := chunkProvider(t, t.TempDir(), remote, view, 0)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	proj := view.Projection()
	proj.Healthy = false
	view.set(proj)
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, storage.ErrChunkCaptureRefused) {
		t.Fatalf("capture with an unhealthy projection: got %v, want refused", err)
	}
	if remote.Puts() != 0 {
		t.Fatal("a refused capture stored chunks")
	}
}

func TestWALStoreChunk_PeriodicCut(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 20*time.Millisecond)
	s := openRecovered(t, p)
	lsn, err := s.Write(50, block(0x50))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		head, _ := remote.GetHead(context.Background())
		if head.RemoteLSN >= lsn {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic cut never published LSN %d (HEAD remote_lsn %d)", lsn, head.RemoteLSN)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bytes.Equal(headBlock(t, remote, 50), block(0x50)) {
		t.Fatal("periodic cut published the wrong bytes")
	}
}

func TestWALStoreChunk_SelectorRules(t *testing.T) {
	view := v1View()
	// A non-chunk volume has no chunk operations.
	plain, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), NumBlocks: pcBlocks}, view)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	openRecovered(t, plain)
	if _, err := plain.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrNotChunkVolume) {
		t.Fatalf("CaptureVolume on a plain walstore volume: got %v", err)
	}
	// A smartwal file is not a chunk volume.
	root := t.TempDir()
	sw, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: root, NumBlocks: pcBlocks}, view)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sw.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_ = sw.Close()
	p := chunkProvider(t, root, storage.NewMemChunkRemote(), view, 0)
	if _, err := p.EnsureStorage("v1"); !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("walstore-chunk over a smartwal file: got %v, want ErrImplKindMismatch", err)
	}
}
