package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const catBase = "/swblock/chunks"

var catGeo = Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}

// publishSource writes chunks for vol and installs its first HEAD (epoch 1,
// remote_lsn 42) through the real ChunkRemote; it returns the HEAD.
func publishSource(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string, chunks map[uint64][]byte) storage.ChunkHead {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, vol))
	if err != nil {
		t.Fatal(err)
	}
	h := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 42, Chunks: map[uint64]storage.ChunkRef{}}
	for idx, data := range chunks {
		key := "chunks/e1/c" + string(rune('0'+idx)) + "/k"
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		h.Chunks[idx] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, h)
	if err != nil {
		t.Fatal(err)
	}
	h.Gen = gen
	return h
}

func newCat(t *testing.T, f *fakeFiler) *Catalog {
	t.Helper()
	c, err := NewCatalog(f, catBase)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func chunkBytes(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, int(catGeo.ChunkBlocks)*int(catGeo.BlockSize))
}

func catalogFixture(t *testing.T) (*fakeFiler, *fakeBlobs, *Catalog, storage.ChunkHead) {
	t.Helper()
	f, b := newFakeFiler(), newFakeBlobs()
	c := newCat(t, f)
	if _, err := c.EnsureVolume(context.Background(), "v1", catGeo); err != nil {
		t.Fatal(err)
	}
	h := publishSource(t, f, b, "v1", map[uint64][]byte{0: chunkBytes(0x10), 3: chunkBytes(0x13)})
	return f, b, c, h
}

func assertNoFilerChunks(t *testing.T, f *fakeFiler) {
	t.Helper()
	for _, e := range f.allWritten() {
		if len(e.Chunks) != 0 {
			t.Fatalf("entry %q written with filer chunks", e.Name)
		}
	}
}

func TestCatalog_SnapshotCreateExclusive(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	s1, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req-a", Labels: map[string]string{"agent.step": "npm-ci"}}, h)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Replayed || s1.HeadGen != h.Gen || s1.HeadEpoch != 1 || s1.CutLSN != 42 || s1.ChunkCount != 2 || s1.State != SnapActive || s1.Consistency != "crash" {
		t.Fatalf("created %+v", s1)
	}
	again, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req-a"}, h)
	if err != nil || !again.Replayed || again.HeadGen != s1.HeadGen {
		t.Fatalf("same request id: %+v %v, want a replay", again, err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req-b"}, h); CodeOf(err) != CodeSnapshotExists {
		t.Fatalf("other request id: %v, want snapshot_exists", err)
	}
	for _, bad := range []string{"", ".hidden", "HEAD", "snaps", "a/b", strings.Repeat("x", 129)} {
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: bad, RequestID: "r"}, h); CodeOf(err) != CodeInvalidName {
			t.Errorf("name %q: %v, want invalid_name", bad, err)
		}
	}
	assertNoFilerChunks(t, f)
	// The stored HEAD copy is byte-for-byte the HEAD document.
	e, _ := f.LookupEntry(ctx, catBase+"/v1/snaps", "s1")
	var doc snapDoc
	if err := json.Unmarshal(e.Content, &doc); err != nil {
		t.Fatal(err)
	}
	headEnt, _ := f.LookupEntry(ctx, catBase+"/v1", "HEAD")
	if !bytes.Equal(doc.Head, headEnt.Content) {
		t.Fatal("snapshot's head is not the HEAD document")
	}
}

func TestCatalog_SnapshotEntryNeverCarriesChunks(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	assertNoFilerChunks(t, f)
	f.mu.Lock()
	f.entries[catBase+"/v1/snaps/s1"].Chunks = []*filerpb.FileChunk{{FileId: "3,01"}}
	f.mu.Unlock()
	if _, _, err := c.ListSnapshots(ctx, "v1", ListOptions{}); err == nil || CodeOf(err) == CodeSnapshotNotFound {
		t.Fatalf("a snapshot entry with filer chunks listed: %v", err)
	}
}

func TestCatalog_ListPagesAndFilters(t *testing.T) {
	_, _, c, h := catalogFixture(t)
	ctx := context.Background()
	for _, n := range []string{"a1", "a2", "b1", "b2", "b3"} {
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: n, RequestID: n}, h); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "b2"); err != nil {
		t.Fatal(err)
	}
	names := func(s []SnapshotInfo) string {
		var out []string
		for _, x := range s {
			out = append(out, x.Name)
		}
		return strings.Join(out, ",")
	}
	page, next, err := c.ListSnapshots(ctx, "v1", ListOptions{Limit: 2})
	if err != nil || names(page) != "a1,a2" || next != "a2" {
		t.Fatalf("page 1: %s next %q %v", names(page), next, err)
	}
	page, next, _ = c.ListSnapshots(ctx, "v1", ListOptions{Limit: 2, StartAfter: next})
	if names(page) != "b1,b3" || next != "b3" {
		t.Fatalf("page 2 (deleting b2 skipped): %s next %q", names(page), next)
	}
	page, next, _ = c.ListSnapshots(ctx, "v1", ListOptions{Limit: 2, StartAfter: next})
	if names(page) != "" || next != "" {
		t.Fatalf("page 3: %s next %q", names(page), next)
	}
	page, _, _ = c.ListSnapshots(ctx, "v1", ListOptions{Prefix: "b", IncludeDeleting: true})
	if names(page) != "b1,b2,b3" || page[1].State != SnapDeleting {
		t.Fatalf("prefix b with deleting: %s", names(page))
	}
	if page, _, err := c.ListSnapshots(ctx, "v9", ListOptions{}); err != nil || len(page) != 0 {
		t.Fatalf("volume without snapshots: %v %v", page, err)
	}
	vols, err := c.ListVolumes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = vols // the fake filer has no directory entries; the real filer is covered by the integration test
}

func TestCatalog_DeleteMarksThenIdempotent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	before, _ := f.LookupEntry(ctx, catBase+"/v1/snaps", "s1")
	f.mu.Lock()
	deletesBefore := f.deletes
	f.mu.Unlock()
	st, err := c.DeleteSnapshot(ctx, "v1", "s1")
	if err != nil || st != SnapDeleting {
		t.Fatalf("delete: %q %v", st, err)
	}
	after, _ := f.LookupEntry(ctx, catBase+"/v1/snaps", "s1")
	if !bytes.Equal(before.Content, after.Content) || string(after.Extended[extSnapState]) != SnapDeleting || len(after.Extended[extSnapDeletedNs]) == 0 {
		t.Fatal("delete must only mark the entry (content unchanged, state deleting, deleted_ns set)")
	}
	if st, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
		t.Fatalf("repeated delete: %q %v", st, err)
	}
	if st, err := c.DeleteSnapshot(ctx, "v1", "nope"); err != nil || st != SnapAbsent {
		t.Fatalf("delete of a missing snapshot: %q %v", st, err)
	}
	f.mu.Lock()
	removed := f.deletes - deletesBefore
	f.mu.Unlock()
	if removed != 0 {
		t.Fatalf("DeleteSnapshot removed %d entries; it must only mark", removed)
	}
}

func TestFork_HeadIsEpochZeroWithQualifiedKeys(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	intentsAtHead := -1
	f.beforeCreate = func(p string) {
		if p == catBase+"/v2/HEAD" {
			intentsAtHead = 0
			for k := range f.entries {
				if strings.HasPrefix(k, catBase+"/v2/intents/") {
					intentsAtHead++
				}
			}
		}
	}
	res, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork-1"})
	if err != nil {
		t.Fatal(err)
	}
	if intentsAtHead != 1 {
		t.Fatalf("intents in v2 when its HEAD was written: %d, want 1", intentsAtHead)
	}
	if got, _ := f.ListEntries(ctx, catBase+"/v2/intents", "", "", 0); len(got) != 0 {
		t.Fatalf("fork left %d intents", len(got))
	}
	r2, _ := NewChunkRemote(f, newFakeBlobs(), catBase+"/v2")
	head, err := r2.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.Gen != 1 || head.Epoch != 0 || head.Holder != "" || head.RemoteLSN != 42 || len(head.Chunks) != 2 {
		t.Fatalf("fork HEAD %+v: want gen 1, epoch 0, no holder, the snapshot's remote_lsn 42", head)
	}
	for idx, ref := range head.Chunks {
		src := h.Chunks[idx]
		if ref.Key != "@v1/"+src.Key || ref.SHA256 != src.SHA256 {
			t.Fatalf("chunk %d ref %+v, want @v1/%s with the same hash", idx, ref, src.Key)
		}
	}
	if res.HeadGen != 1 || res.HeadEpoch != 0 || res.QualifiedRefs != 2 || res.SourceGen != h.Gen || res.RemoteLSN != 42 || res.Replayed {
		t.Fatalf("result %+v", res)
	}
	v, ok, err := c.ReadVolume(ctx, "v2")
	if err != nil || !ok || v.Origin != OriginFork || v.Geometry != catGeo || v.Parent == nil || v.Parent.VolumeID != "v1" || v.Parent.Snapshot != "s1" || v.Parent.Gen != h.Gen {
		t.Fatalf("fork VOLUME %+v %v %v", v, ok, err)
	}
	again, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork-1"})
	if err != nil || !again.Replayed {
		t.Fatalf("retried fork: %+v %v", again, err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork-2"}); CodeOf(err) != CodeTargetExists {
		t.Fatalf("fork onto an existing volume: %v, want target_exists", err)
	}
	assertNoFilerChunks(t, f)
}

func TestFork_ForkOfForkKeepsOriginalOwner(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f1"}); err != nil {
		t.Fatal(err)
	}
	r2, _ := NewChunkRemote(f, b, catBase+"/v2")
	h2, _ := r2.GetHead(ctx)
	if _, err := c.CreateSnapshot(ctx, "v2", SnapshotRequest{Name: "t1", RequestID: "r2"}, h2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v2", Snapshot: "t1", Target: "v3", RequestID: "f2"}); err != nil {
		t.Fatal(err)
	}
	r3, _ := NewChunkRemote(f, b, catBase+"/v3")
	h3, _ := r3.GetHead(ctx)
	for idx, ref := range h3.Chunks {
		if ref.Key != "@v1/"+h.Chunks[idx].Key {
			t.Fatalf("second-level fork chunk %d names %q, want the original owner @v1/%s", idx, ref.Key, h.Chunks[idx].Key)
		}
		if got, err := r3.Get(ctx, ref.Key); err != nil || sha256.Sum256(got) != ref.SHA256 {
			t.Fatalf("v3 reads chunk %d through its ref: %v", idx, err)
		}
	}
}

func TestFork_RefusalsAndDeadline(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "nope", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeSnapshotNotFound {
		t.Fatalf("fork of a missing snapshot: %v", err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "gone", RequestID: "g"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "gone", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeSnapshotDeleting {
		t.Fatalf("fork of a deleting snapshot: %v", err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "../x", RequestID: "f"}); CodeOf(err) != CodeInvalidName {
		t.Fatalf("fork onto a non-plain volume id: %v", err)
	}
	// The clock passes OpDeadline between reading the snapshot and the HEAD write.
	start := time.Unix(1000, 0)
	calls := 0
	c.SetClock(func() time.Time {
		calls++
		if calls > 2 {
			return start.Add(DefaultOpDeadline + time.Second)
		}
		return start
	})
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v4", RequestID: "late"}); CodeOf(err) != CodeDeadlineExceeded {
		t.Fatalf("late fork: %v, want deadline_exceeded", err)
	}
	if _, err := f.LookupEntry(ctx, catBase+"/v4", "HEAD"); err == nil {
		t.Fatal("a fork past its deadline wrote a HEAD")
	}
	c.SetClock(time.Now)
	// The VOLUME from the late attempt is reused by its own retry.
	if res, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v4", RequestID: "late"}); err != nil || res.Replayed {
		t.Fatalf("retry after the late attempt: %+v %v", res, err)
	}
	// A fork whose VOLUME is written but HEAD is not refuses to attach.
	s1, _, _, err := c.readSnap(ctx, "v1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := headDigest(s1.Head())
	vol := VolumeInfo{Format: volumeFormat, VolumeID: "v5", Geometry: catGeo, Origin: OriginFork, Parent: &Parent{VolumeID: "v1", Snapshot: "s1", Gen: h.Gen},
		Fork: &ForkRecord{RequestID: "crashed", Source: "v1", Snapshot: "s1", SourceGen: h.Gen, HeadSHA256: digest, RemoteLSN: 42, ChunkCount: 2, QualifiedRefs: 2}}
	ve, _ := volumeEntry(vol, "crashed")
	if err := f.CreateEntry(ctx, catBase+"/v5", ve, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v5", catGeo); CodeOf(err) != CodeForkIncomplete {
		t.Fatalf("attach of an incomplete fork: %v, want fork_incomplete", err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v5", RequestID: "crashed"}); err != nil {
		t.Fatalf("retry completes the crashed fork: %v", err)
	}
	if _, err := c.EnsureVolume(ctx, "v5", catGeo); err != nil {
		t.Fatalf("attach after the fork completed: %v", err)
	}
	if _, err := c.EnsureVolume(ctx, "v5", Geometry{NumBlocks: 128, BlockSize: 4096, ChunkBlocks: 8}); CodeOf(err) != CodeGeometryMismatch {
		t.Fatalf("attach with other geometry: %v, want geometry_mismatch", err)
	}
}

// End to end over the walstore chunk layer: a fork attached as a fresh host
// reads exactly the snapshot's image, lazily, through the owner-qualified
// refs; its own first cut writes only into its own keys/.
func TestFork_ThenFreshHostReadsSnapshotImage(t *testing.T) {
	ctx := context.Background()
	const blocks, chunkBlocks = 64, 8
	f, b := newFakeFiler(), newFakeBlobs()
	c := newCat(t, f)
	geo := Geometry{NumBlocks: blocks, BlockSize: 4096, ChunkBlocks: chunkBlocks}
	dir := t.TempDir()
	attach := func(vol string, fresh bool, epoch uint64) (*storage.WALStore, storage.ChunkLayer) {
		if _, err := c.EnsureVolume(ctx, vol, geo); err != nil && !fresh {
			t.Fatal(err)
		}
		p := filepath.Join(dir, vol)
		s, err := storage.CreateWALStore(p, blocks, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		r, _ := NewChunkRemote(f, b, path.Join(catBase, vol))
		l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: chunkBlocks, Epoch: epoch, Holder: vol, FreshFromHead: fresh,
			Bitmap: storage.NewFileChunkBitmapStore(p + ".chunkbitmap")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s, l
	}
	src, layer := attach("v1", false, 1)
	want := map[uint32][]byte{}
	for _, lba := range []uint32{0, 9, 17, 63} {
		want[lba] = bytes.Repeat([]byte{byte(lba + 1)}, 4096)
		if _, err := src.Write(lba, want[lba]); err != nil {
			t.Fatal(err)
		}
	}
	cut, err := layer.Cut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, cut.Head); err != nil {
		t.Fatal(err)
	}
	// The source keeps writing after the snapshot; the fork must not see it.
	if _, err := src.Write(9, bytes.Repeat([]byte{0xee}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); err != nil {
		t.Fatal(err)
	}
	dst, dlayer := attach("v2", true, 1)
	zero := make([]byte, 4096)
	for lba := uint32(0); lba < blocks; lba++ {
		got, err := dst.Read(lba)
		if err != nil {
			t.Fatalf("fork read %d: %v", lba, err)
		}
		exp, ok := want[lba]
		if !ok {
			exp = zero
		}
		if !bytes.Equal(got, exp) {
			t.Fatalf("fork block %d = %x..., want the snapshot's %x...", lba, got[:2], exp[:2])
		}
	}
	keysBefore, _ := f.ListEntries(ctx, catBase+"/v1/keys", "", "", 0)
	if _, err := dst.Write(40, bytes.Repeat([]byte{0x40}, 4096)); err != nil {
		t.Fatal(err)
	}
	r2, err := dlayer.Cut(ctx)
	if err != nil {
		t.Fatalf("fork's first cut: %v", err)
	}
	if r2.Head.Epoch != 1 || r2.Head.RemoteLSN < 1 {
		t.Fatalf("fork's first HEAD %+v", r2.Head)
	}
	keysAfter, _ := f.ListEntries(ctx, catBase+"/v1/keys", "", "", 0)
	if len(keysAfter) != len(keysBefore) {
		t.Fatalf("the fork's cut wrote %d keys into the source volume", len(keysAfter)-len(keysBefore))
	}
	if own, _ := f.ListEntries(ctx, catBase+"/v2/keys", "", "", 0); len(own) != 1 {
		t.Fatalf("the fork's cut wrote %d keys of its own, want 1", len(own))
	}
}

func TestCatalog_SnapshotIntentAroundEntry(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	seen := -1
	f.beforeCreate = func(p string) {
		if p == catBase+"/v1/snaps/s1" {
			seen = 0
			for k := range f.entries {
				if strings.HasPrefix(k, catBase+"/v1/intents/") {
					seen++
				}
			}
		}
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("intents when the snapshot entry was written: %d, want 1", seen)
	}
	if got, _ := f.ListEntries(ctx, catBase+"/v1/intents", "", "", 0); len(got) != 0 {
		t.Fatalf("snapshot left %d intents", len(got))
	}
}

// The HEAD document carries "state" ("active" by every normal cut) and,
// only when detached, "detachedBy" {holder, epoch, gen} naming its own
// holder, epoch and generation; a document without "state" reads as active,
// and an inconsistent detachedBy is refused.
func TestHeadDoc_StateAndDetachedBy(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	r, _ := NewChunkRemote(f, b, catBase+"/v1")
	ctx := context.Background()
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "h", RemoteLSN: 5, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadActive})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := f.LookupEntry(ctx, catBase+"/v1", "HEAD")
	if !strings.Contains(string(e.Content), `"state":"active"`) || strings.Contains(string(e.Content), "detachedBy") {
		t.Fatalf("an active HEAD document: %s", e.Content)
	}
	det := storage.ChunkHead{Epoch: 1, Holder: "h", RemoteLSN: 5, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadDetached,
		DetachedBy: &storage.ChunkDetachedBy{Holder: "h", Epoch: 1, Gen: gen + 1}}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: gen, Epoch: 1}, det); err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil || h.State != storage.ChunkHeadDetached || h.DetachedBy == nil || *h.DetachedBy != (storage.ChunkDetachedBy{Holder: "h", Epoch: 1, Gen: gen + 1}) {
		t.Fatalf("detached HEAD read back as %+v %v", h, err)
	}

	// An older document without "state" reads as active.
	e, _ = f.LookupEntry(ctx, catBase+"/v1", "HEAD")
	var doc map[string]any
	_ = json.Unmarshal(e.Content, &doc)
	delete(doc, "state")
	delete(doc, "detachedBy")
	old, _ := json.Marshal(doc)
	f.mu.Lock()
	f.entries[catBase+"/v1/HEAD"].Content = old
	f.mu.Unlock()
	if h, err := r.GetHead(ctx); err != nil || h.State == storage.ChunkHeadDetached {
		t.Fatalf("a HEAD without state: %+v %v, want active", h, err)
	}
	// detachedBy that is not the HEAD's own holder/epoch/gen is refused.
	doc["state"] = "detached"
	doc["detachedBy"] = map[string]any{"holder": "h", "epoch": 1, "gen": gen} // stale gen
	bad, _ := json.Marshal(doc)
	f.mu.Lock()
	f.entries[catBase+"/v1/HEAD"].Content = bad
	f.mu.Unlock()
	if _, err := r.GetHead(ctx); err == nil {
		t.Fatal("a detached HEAD with another generation in detachedBy was accepted")
	}
}

// Production serialization end to end: the walstore chunk layer's Detach,
// over the SeaweedFS remote, writes HEAD to the filer with exactly the
// ruled fields, in one CAS with the final cut.
func TestDetach_ProductionSerializedHead(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	p := filepath.Join(t.TempDir(), "v1")
	s, err := storage.CreateWALStore(p, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	r, _ := NewChunkRemote(f, b, catBase+"/v1")
	l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: 8, Epoch: 3, Holder: "host-a",
		Bitmap: storage.NewFileChunkBitmapStore(p + ".chunkbitmap")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(9, bytes.Repeat([]byte{9}, 4096)); err != nil {
		t.Fatal(err)
	}
	res, err := l.Detach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, catBase+"/v1", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Gen        uint64         `json:"gen"`
		Epoch      uint64         `json:"epoch"`
		Holder     string         `json:"holder"`
		RemoteLSN  uint64         `json:"remote_lsn"`
		State      string         `json:"state"`
		DetachedBy map[string]any `json:"detachedBy"`
	}
	if err := json.Unmarshal(e.Content, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Gen != 1 || doc.State != "detached" || doc.Holder != "host-a" || doc.Epoch != 3 || doc.RemoteLSN != res.H {
		t.Fatalf("serialized HEAD: %s", e.Content)
	}
	if len(doc.DetachedBy) != 3 || doc.DetachedBy["holder"] != "host-a" || doc.DetachedBy["epoch"] != float64(3) || doc.DetachedBy["gen"] != float64(1) {
		t.Fatalf("serialized detachedBy %v, want exactly {holder host-a, epoch 3, gen 1}", doc.DetachedBy)
	}
	if string(e.Extended[extGen]) != "1" || len(e.Chunks) != 0 {
		t.Fatalf("HEAD entry extended gen %q, filer chunks %d", e.Extended[extGen], len(e.Chunks))
	}
}
