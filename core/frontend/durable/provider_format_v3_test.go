//go:build swblock_format_v3_test

package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type formatV3View struct{}

func (formatV3View) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
}

type formatV3Fixture struct {
	filer *chunkstore.MemFiler
	cat   *chunkstore.BCatalog
	ref   chunkstore.VolumeRef
	blobs *layoutCrashBlobs
}

func newFormatV3Fixture(t *testing.T) *formatV3Fixture {
	t.Helper()
	blobs := &layoutCrashBlobs{b: map[string][]byte{}}
	filer := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewBCatalog(filer, blobs, "/format-v3")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(context.Background(), ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	return &formatV3Fixture{filer: filer, cat: cat, ref: ref, blobs: blobs}
}

func (f *formatV3Fixture) head(t *testing.T) layout.Head {
	t.Helper()
	h, found, err := f.cat.ReadHead(context.Background(), f.ref)
	if err != nil || !found {
		t.Fatalf("B HEAD: found=%v err=%v", found, err)
	}
	return h
}

func (f *formatV3Fixture) block(t *testing.T, lba uint64) []byte {
	t.Helper()
	h := f.head(t)
	if h.Current == nil {
		return make([]byte, 4096)
	}
	st, err := f.cat.Store(f.ref)
	if err != nil {
		t.Fatal(err)
	}
	r, err := layout.NewReader("/format-v3", f.ref.Dir(), st.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	img, err := r.Open(context.Background(), *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := img.Chunk(context.Background(), lba/4)
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Zero {
		return make([]byte, 4096)
	}
	off := int(lba%4) * 4096
	return chunk.Data[off : off+4096]
}

func (f *formatV3Fixture) puts() int {
	f.blobs.Lock()
	defer f.blobs.Unlock()
	return len(f.blobs.b)
}

type formatV3Entry struct {
	Dir   string
	Entry *filerpb.Entry
}

type formatV3Image struct {
	Ref     chunkstore.VolumeRef
	Entries []formatV3Entry
	Blobs   map[string][]byte
}

func (f *formatV3Fixture) image(t *testing.T) formatV3Image {
	t.Helper()
	img := formatV3Image{Ref: f.ref, Blobs: map[string][]byte{}}
	base := path.Join("/format-v3", f.ref.Dir())
	for _, suffix := range []string{"", layout.RootDir, layout.SegDir, path.Join(layout.PoolDir, "0")} {
		dir := path.Join(base, suffix)
		entries, err := f.filer.ListEntries(context.Background(), dir, "", "", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			img.Entries = append(img.Entries, formatV3Entry{Dir: dir, Entry: entry})
		}
	}
	f.blobs.Lock()
	for name, data := range f.blobs.b {
		img.Blobs[name] = append([]byte(nil), data...)
	}
	f.blobs.Unlock()
	return img
}

func restoreFormatV3Image(t *testing.T, img formatV3Image) *formatV3Fixture {
	t.Helper()
	filer := chunkstore.NewMemFiler()
	for _, item := range img.Entries {
		if err := filer.CreateEntry(context.Background(), item.Dir, item.Entry, true); err != nil {
			t.Fatal(err)
		}
	}
	blobs := &layoutCrashBlobs{b: img.Blobs}
	cat, err := chunkstore.NewBCatalog(filer, blobs, "/format-v3")
	if err != nil {
		t.Fatal(err)
	}
	return &formatV3Fixture{filer: filer, cat: cat, ref: img.Ref, blobs: blobs}
}

func mappedProviderV3(t *testing.T, root string, fixture *formatV3Fixture, view frontend.ProjectionView, override ...layout.StartStore) *DurableProvider {
	t.Helper()
	headFor := func(string) (layout.StartStore, error) { return fixture.cat.Store(fixture.ref) }
	if len(override) > 0 {
		headFor = func(string) (layout.StartStore, error) { return override[0], nil }
	}
	p, err := NewDurableProvider(ProviderConfig{
		Impl: ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		WALRecoveryTestDisableFlusher: true,
		ChunkLayoutHeadFor:            headFor,
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(fixture.cat, fixture.ref, me)
		},
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	// Activation closure: use the production selector and provider geometry.
	return p
}

func TestDurableProviderV3_MappedCheckpointCaptureAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	fixture := newFormatV3Fixture(t)
	p := mappedProviderV3(t, root, fixture, formatV3View{})
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
		if fixture.head(t).Current == nil {
			t.Fatal("capture omitted checkpointed chunk")
		}
		if !bytes.Equal(fixture.block(t, 5), want) {
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
	p2 := mappedProviderV3(t, root, fixture, formatV3View{})
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
	before := fixture.puts()
	if _, err = p2.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if fixture.puts()-before != 1 {
		t.Fatalf("incremental capture uploaded %d chunks", fixture.puts()-before)
	}
	check(next)
}

// The selector must not turn a missing owned map into a legacy bitmap path.
func TestDurableProviderV3_MissingMapRefusesBeforeBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	fixture := newFormatV3Fixture(t)
	p := mappedProviderV3(t, root, fixture, formatV3View{})
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
	p2 := mappedProviderV3(t, root, fixture, formatV3View{})
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

func TestDurableProviderV3_DefaultSelectorCreatesV3(t *testing.T) {
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
	if binary.LittleEndian.Uint16(hdr[4:6]) != 3 || binary.LittleEndian.Uint32(hdr[12:16]) != 3 {
		t.Fatal("default provider did not select v3/impl3")
	}
}

// An invalid inherited frontier must fail preparation without claiming HEAD.
func TestDurableProviderV3_InvalidHEADFrontierRefusesBeforeClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := newFormatV3Fixture(t)
	store, err := fixture.cat.Store(fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	bad := fixture.head(t)
	bad.Epoch, bad.Holder, bad.ClaimID, bad.Position = 2, "old", "old-claim", ^uint64(0)
	if err := store.UpdateHead(ctx, bad.Gen, bad); err != nil {
		t.Fatal(err)
	}
	before := fixture.head(t)
	p := mappedProviderV3(t, t.TempDir(), fixture, formatV3View{})
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
	after := fixture.head(t)
	if after.Gen != before.Gen || after.Epoch != before.Epoch || after.Holder != before.Holder || after.Position != before.Position {
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
	layout.StartStore
	beforeClaim       func() error
	loseClaimResponse bool
}

func (r *formatV3RestoreRemote) UpdateHead(ctx context.Context, expected uint64, next layout.Head) error {
	if next.Epoch == 4 {
		if err := r.beforeClaim(); err != nil {
			return err
		}
	}
	err := r.StartStore.UpdateHead(ctx, expected, next)
	if err == nil && next.Epoch == 4 && r.loseClaimResponse {
		return errors.New("claim response lost after commit")
	}
	return err
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
	fixture := newFormatV3Fixture(t)
	source := mappedProviderV3(t, t.TempDir(), fixture, formatV3View{})
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
	store, err := fixture.cat.Store(fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	probe := &formatV3RestoreRemote{StartStore: store, loseClaimResponse: loseClaimResponse}
	target := mappedProviderV3(t, root, fixture, formatV3TakeoverView{}, probe)
	restored, err := target.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	probe.beforeClaim = func() error {
		if id, _, ok := restored.(*storage.WALStore).LayoutIdentity(); !ok || id == "" {
			return errors.New("HEAD takeover attempted before durable local identity")
		}
		return nil
	}
	if loseClaimResponse {
		if _, err = target.RecoverVolume(ctx, "v1"); err == nil {
			t.Fatal("lost B claim reply was treated as an acknowledged claim")
		}
		probe.loseClaimResponse = false
	}
	if _, err = target.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("actual mapped HEAD restore refused: %v", err)
	}
	if restored.NextLSN() != 3 {
		t.Fatalf("restored nextLSN=%d want3", restored.NextLSN())
	}
	head := fixture.head(t)
	if head.Position != 2 || head.Epoch != 4 || head.Holder != "r2" {
		t.Fatalf("takeover changed image/frontier: %+v", head)
	}
	before := fixture.puts()
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
	if fixture.puts() != before {
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
	reopened := mappedProviderV3(t, root, fixture, formatV3TakeoverView{})
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
	p := mappedProviderV3(t, t.TempDir(), newFormatV3Fixture(t), formatV3View{})
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

func TestDurableProviderV3_ContiguousWriteSurvivesCheckpointAndReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	fixture := newFormatV3Fixture(t)
	p := mappedProviderV3(t, root, fixture, formatV3View{})
	st, e := p.EnsureStorage("v1")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.RecoverVolume(ctx, "v1"); e != nil {
		t.Fatal(e)
	}
	want := append(bytes.Repeat([]byte{0x31}, 4096), bytes.Repeat([]byte{0x72}, 4096)...)
	if n, e := p.Backend("v1").Write(ctx, 4*4096, want); e != nil || n != len(want) {
		t.Fatalf("actual provider contiguous Write: n=%d err=%v", n, e)
	}
	if e = p.Backend("v1").Sync(ctx); e != nil {
		t.Fatal(e)
	}
	if e = storage.FlushMappedWALStoreV3Test(st.(*storage.WALStore)); e != nil {
		t.Fatal(e)
	}
	if _, e = p.CaptureVolume(ctx, "v1"); e != nil {
		t.Fatal(e)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	q := mappedProviderV3(t, root, fixture, formatV3View{})
	if _, e = q.EnsureStorage("v1"); e != nil {
		t.Fatal(e)
	}
	if _, e = q.RecoverVolume(ctx, "v1"); e != nil {
		t.Fatal(e)
	}
	got := make([]byte, len(want))
	if n, e := q.Backend("v1").Read(ctx, 4*4096, got); e != nil || n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("reopen n=%d err=%v equal=%v", n, e, bytes.Equal(got, want))
	}
}
