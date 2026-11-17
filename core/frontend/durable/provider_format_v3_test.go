//go:build swblock_format_v3_test

package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

// The selector must not turn a missing owned map into a legacy bitmap path.
func TestDurableProviderV3_MissingMapRefusesBeforeBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	remote := storage.NewMemChunkRemote()
	p := mappedProviderV3(t, root, remote)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	path := p.volumePath("v1")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".chunkmap"); err != nil {
		t.Fatal(err)
	}
	p2 := mappedProviderV3(t, root, remote)
	if st, err := p2.EnsureStorage("v1"); err == nil || st != nil {
		t.Fatalf("accepted missing map: store=%v err=%v", st, err)
	}
	if p2.Backend("v1") != nil {
		t.Fatal("failed open cached a backend")
	}
	if _, err := os.Stat(path + ".chunkbitmap"); !os.IsNotExist(err) {
		t.Fatalf("legacy fallback created a bitmap: %v", err)
	}
}

func TestDurableProviderV3_DefaultSelectorStillCreatesLegacy(t *testing.T) {
	p, err := NewDurableProvider(ProviderConfig{Impl: ImplWALStore, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096}, formatV3View{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p.volumePath("v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var hdr [16]byte
	if _, err = f.ReadAt(hdr[:], 0); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint16(hdr[4:6]) != 2 {
		t.Fatal("default provider activated v3")
	}
}

// An invalid inherited frontier must fail preparation without claiming HEAD.
func TestDurableProviderV3_InvalidHEADFrontierRefusesBeforeClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 2, Holder: "old", RemoteLSN: ^uint64(0)}); err != nil {
		t.Fatal(err)
	}
	before, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := mappedProviderV3(t, t.TempDir(), remote)
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(p.volumePath("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err == nil {
		t.Fatal("invalid HEAD frontier accepted")
	}
	after, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Gen != before.Gen || after.Epoch != before.Epoch || after.Holder != before.Holder {
		t.Fatal("failed seed claimed HEAD")
	}
	now, err := os.ReadFile(p.volumePath("v1"))
	if err != nil {
		t.Fatal(err)
	}
	// Recover may publish the serving scan's descriptor. It must never serialize
	// a legacy header or advance the inherited logical frontier.
	if binary.LittleEndian.Uint16(now[4:6]) != 3 || binary.LittleEndian.Uint16(raw[4:6]) != 3 {
		t.Fatal("restore serialized a legacy header")
	}
	var buf [4096]byte
	if _, err := p.Backend("v1").Read(ctx, 0, buf[:]); err == nil {
		t.Fatal("failed restore left backend operational")
	}
}

// This exercises the row7 caller seam without enabling production v3. The
// owner seed/install transaction must make this pass before restore is claimed.
type formatV3TakeoverView struct{}

func (formatV3TakeoverView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r2", Epoch: 4, EndpointVersion: 1, Healthy: true}
}

type formatV3RestoreRemote struct {
	storage.ChunkRemote
	beforeClaim       func() error
	loseClaimResponse bool
}

func (r *formatV3RestoreRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if expected.Epoch == 3 {
		if err := r.beforeClaim(); err != nil {
			return 0, err
		}
	}
	gen, err := r.ChunkRemote.CASHead(ctx, expected, next)
	if err == nil && expected.Epoch == 3 && r.loseClaimResponse {
		return 0, errors.New("claim response lost after commit")
	}
	return gen, err
}
func TestDurableProviderV3_HEADRestorePreservesRemoteImageAndNextLSN(t *testing.T) {
	exerciseMappedHEADRestoreV3(t, false)
}
func TestDurableProviderV3_HEADRestoreAfterLostClaimResponse(t *testing.T) {
	exerciseMappedHEADRestoreV3(t, true)
}
func exerciseMappedHEADRestoreV3(t *testing.T, loseClaimResponse bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	source := mappedProviderV3(t, t.TempDir(), remote)
	st, err := source.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	first, second := bytes.Repeat([]byte{0x51}, 4096), bytes.Repeat([]byte{0xC2}, 4096)
	for lba, data := range map[int64][]byte{5: first, 12: second} {
		if _, err = source.Backend("v1").Write(ctx, lba*4096, data); err != nil {
			t.Fatal(err)
		}
	}
	if err = source.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = storage.FlushMappedWALStoreV3Test(st.(*storage.WALStore)); err != nil {
		t.Fatal(err)
	}
	if h, err := source.CaptureVolume(ctx, "v1"); err != nil || h != 2 {
		t.Fatalf("source capture H=%d err=%v", h, err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	probe := &formatV3RestoreRemote{ChunkRemote: remote, loseClaimResponse: loseClaimResponse}
	target := mappedProviderV3(t, root, probe)
	target.view = formatV3TakeoverView{}
	restored, err := target.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	probe.beforeClaim = func() error {
		if restored.NextLSN() != 3 {
			return errors.New("HEAD takeover attempted before durable seed/rebase")
		}
		return nil
	}
	if _, err = target.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("actual mapped HEAD restore refused: %v", err)
	}
	if restored.NextLSN() != 3 {
		t.Fatalf("restored nextLSN=%d want3", restored.NextLSN())
	}
	head, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.RemoteLSN != 2 || head.Epoch != 4 || head.Holder != "r2" {
		t.Fatalf("takeover changed image/frontier: %+v", head)
	}
	before := remote.Puts()
	check := func(p *DurableProvider, lba int64, want []byte) {
		t.Helper()
		got := make([]byte, 4096)
		if n, e := p.Backend("v1").Read(ctx, lba*4096, got); e != nil || n != len(got) || !bytes.Equal(got, want) {
			t.Fatalf("LBA%d n=%d err=%v equal=%v", lba, n, e, bytes.Equal(got, want))
		}
	}
	check(target, 5, first)
	check(target, 12, second)
	check(target, 0, make([]byte, 4096))
	if remote.Puts() != before {
		t.Fatal("lazy read uploaded remote chunks")
	}
	next := bytes.Repeat([]byte{0x83}, 4096)
	if _, err = target.Backend("v1").Write(ctx, 5*4096, next); err != nil {
		t.Fatal(err)
	}
	if restored.NextLSN() != 4 {
		t.Fatalf("restored write reused inherited LSN: next=%d", restored.NextLSN())
	}
	if err = target.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = storage.FlushMappedWALStoreV3Test(restored.(*storage.WALStore)); err != nil {
		t.Fatal(err)
	}
	if h, err := target.CaptureVolume(ctx, "v1"); err != nil || h != 3 {
		t.Fatalf("restored capture H=%d err=%v", h, err)
	}
	if _, err = os.Stat(target.volumePath("v1") + ".chunkbitmap"); !os.IsNotExist(err) {
		t.Fatalf("restore used legacy bitmap: %v", err)
	}
	if err = target.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := mappedProviderV3(t, root, remote)
	reopened.view = formatV3TakeoverView{}
	if _, err = reopened.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	check(reopened, 5, next)
	check(reopened, 12, second)
}

// Exercise the same snapshots consumed by the host status endpoint, not a
// separately constructed status object.
func TestDurableProviderV3_StatusTracksRecoverSyncAndCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := mappedProviderV3(t, t.TempDir(), storage.NewMemChunkRemote())
	st, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	status := func() VolumeStatus {
		t.Helper()
		all := p.DurableStatuses()
		if len(all) != 1 {
			t.Fatalf("statuses=%+v", all)
		}
		v := all[0]
		if v.VolumeID != "v1" || v.ReplicaID != "r1" || v.Epoch != 3 || !v.Latched || !v.FrontierKnown {
			t.Fatalf("lineage/frontier=%+v", v)
		}
		return v
	}
	if v := status(); v.Operational || v.Chunk == nil || v.Chunk.Attached {
		t.Fatalf("before recover=%+v", v)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if v := status(); !v.Operational || v.Chunk == nil || !v.Chunk.Attached || v.Chunk.Fenced {
		t.Fatalf("after recover=%+v", v)
	}
	if n, e := p.Backend("v1").Write(ctx, 5*4096, bytes.Repeat([]byte{0x77}, 4096)); e != nil || n != 4096 {
		t.Fatal(n, e)
	}
	if err = p.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if v := status(); v.DurableLSN != 1 || v.HeadLSN != 1 || v.Chunk.RemoteLSN != 0 {
		t.Fatalf("after Sync=%+v", v)
	}
	if err = storage.FlushMappedWALStoreV3Test(st.(*storage.WALStore)); err != nil {
		t.Fatal(err)
	}
	if _, err = p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if v := status(); v.DurableLSN != 1 || v.Chunk.RemoteLSN != 1 || !v.Operational {
		t.Fatalf("after capture=%+v", v)
	}
}
