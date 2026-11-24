package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Volume delete: one linearization point, no snapshot cascade. One
// control per review schedule.

// detachHead publishes vol's HEAD as a clean detach by its holder (what a
// volume server's Detach writes).
func detachHead(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string) {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, vol))
	if err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := h
	next.State = storage.ChunkHeadDetached
	next.DetachedBy = &storage.ChunkDetachedBy{Holder: h.Holder, Epoch: h.Epoch, Gen: h.Gen + 1}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next); err != nil {
		t.Fatal(err)
	}
}

// attachEntry is the HEAD an attaching holder installs over cur.
func attachEntry(t *testing.T, cur storage.ChunkHead, holder string) *filerpb.Entry {
	t.Helper()
	att := cur
	att.Gen, att.Epoch, att.Holder, att.State, att.DetachedBy = cur.Gen+1, cur.Epoch+1, holder, storage.ChunkHeadActive, nil
	e, err := headEntry(att)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// deleteFixture: v1 with an attached HEAD (held by h1) and no snapshots;
// directory entries for v1, w2, w3 (the fake lists only what it holds).
func deleteFixture(t *testing.T) (*fakeFiler, *fakeBlobs, *Catalog, storage.ChunkHead) {
	t.Helper()
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	for _, vol := range []string{"v1", "w2", "w3"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	return f, b, c, h
}

func headState(t *testing.T, c *Catalog, vol string) storage.ChunkHead {
	t.Helper()
	h, _, err := c.ReadHead(context.Background(), vol)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func mustHead(t *testing.T, c *Catalog, vol string) storage.ChunkHead {
	t.Helper()
	h, has, err := c.ReadHead(context.Background(), vol)
	if err != nil || !has {
		t.Fatalf("HEAD of %s: has=%t %v", vol, has, err)
	}
	return h
}

// fakeReservation mirrors the blockmaster's deletion reservation (tokens,
// admitted/committed) for the catalog's Admission.
type fakeReservation struct {
	mu        sync.Mutex
	state     string
	holders   map[string]bool
	refuse    bool // Admit refuses (nothing recorded)
	loseAdmit bool // Admit records, then its response is lost
}

const (
	reservationAdmitted  = "admitted"
	reservationCommitted = "committed"
)

type fakeAdmit struct {
	r   *fakeReservation
	tok string
}

func (r *fakeReservation) admission(tok string) Admission { return &fakeAdmit{r, tok} }

func (a *fakeAdmit) Admit(context.Context, string) error {
	a.r.mu.Lock()
	defer a.r.mu.Unlock()
	if a.r.refuse {
		return fmt.Errorf("%w: assigned to r1", ErrNotAdmitted)
	}
	if a.r.holders == nil {
		a.r.holders = map[string]bool{}
	}
	a.r.holders[a.tok] = true
	if a.r.state == "" {
		a.r.state = reservationAdmitted
	}
	if a.r.loseAdmit {
		return errors.New("response lost")
	}
	return nil
}

func (a *fakeAdmit) Commit(context.Context, string) error {
	a.r.mu.Lock()
	defer a.r.mu.Unlock()
	if a.r.state != reservationCommitted && !a.r.holders[a.tok] {
		return errors.New("token holds nothing")
	}
	a.r.state = reservationCommitted
	return nil
}

func (a *fakeAdmit) Abort(context.Context, string) error {
	a.r.mu.Lock()
	defer a.r.mu.Unlock()
	if a.r.state == reservationCommitted {
		return errors.New("committed")
	}
	delete(a.r.holders, a.tok)
	if len(a.r.holders) == 0 {
		a.r.state = ""
	}
	return nil
}

// An attached volume (HEAD held), or one the admission refuses, is refused
// with volume_in_use and nothing is written; a held HEAD found after the
// admission gives the admission back.
func TestCatalog_DeleteVolumeRefusesAVolumeInUse(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	before := len(f.allWritten())
	rsv := &fakeReservation{}
	res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
	if CodeOf(err) != CodeVolumeInUse || !res.AdmissionAborted || rsv.state != "" {
		t.Fatalf("delete of an attached volume: %+v %v reservation=%q; want volume_in_use, admission given back", res, err, rsv.state)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("a refused delete wrote %d entries", got-before)
	}
	detachHead(t, f, b, "v1")
	before = len(f.allWritten())
	res, err = c.DeleteVolume(ctx, "v1", (&fakeReservation{refuse: true}).admission("d1"))
	var ce *CatalogError
	if !errors.As(err, &ce) || ce.Code != CodeVolumeInUse || ce.Retryable || !strings.Contains(ce.Detail(), "assigned to r1") || res.Admitted {
		t.Fatalf("delete the admission refuses: %+v %v", res, err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("a refused delete wrote %d entries", got-before)
	}
}

// Schedule: a holder attaches after the delete's first read, before the
// mark's read; the mark reads the held HEAD and refuses.
func TestCatalog_DeleteVolumeRefusesAnAttachBeforeTheMark(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	detachHead(t, f, b, "v1")
	headPath := path.Join(catBase, "v1", headName)
	lookups := 0
	f.afterLookup = func(p string) {
		if p != headPath {
			return
		}
		if lookups++; lookups == 1 {
			cur, _ := decodeHead(p, f.entries[p], f.entries[p].GetContent())
			f.entries[p] = attachEntry(t, cur, "h2")
		}
	}
	if _, err := c.DeleteVolume(context.Background(), "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete racing an attach: %v; want volume_in_use", err)
	}
	f.afterLookup = nil
	if h := headState(t, c, "v1"); h.Holder != "h2" || h.State != storage.ChunkHeadActive {
		t.Fatalf("HEAD after the refused delete: %+v; want h2's", h)
	}
}

// An attach between the mark's read and its write makes the write's
// condition fail; the re-read sees the holder and refuses.
func TestCatalog_DeleteVolumeLosesToAnAttachAtTheMark(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	detachHead(t, f, b, "v1")
	headPath := path.Join(catBase, "v1", headName)
	lookups := 0
	f.afterLookup = func(p string) {
		if p != headPath {
			return
		}
		if lookups++; lookups == 2 {
			cur, _ := decodeHead(p, f.entries[p], f.entries[p].GetContent())
			f.entries[p] = attachEntry(t, cur, "h2")
		}
	}
	if _, err := c.DeleteVolume(context.Background(), "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete losing to an attach: %v; want volume_in_use", err)
	}
	f.afterLookup = nil
	if h := headState(t, c, "v1"); h.Holder != "h2" {
		t.Fatalf("HEAD after the lost delete: %+v; want h2's", h)
	}
}

// An attach that passed EnsureVolume before the mark claims HEAD afterwards
// (CASHead from the deleting or deleted HEAD): the remote refuses.
func TestCatalog_DeletedHeadCannotBeRevivedByAClaim(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	claim := func(stage string) {
		t.Helper()
		h, err := r.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: h.Epoch + 1, Holder: "late", RemoteLSN: h.RemoteLSN, Chunks: h.Chunks, State: storage.ChunkHeadActive})
		if !errors.Is(err, storage.ErrChunkHeadPrecondition) || !errors.Is(err, storage.ErrChunkHeadDeleted) {
			t.Fatalf("claim of a %s HEAD: %v; want a precondition failure (deleted)", stage, err)
		}
		if _, err := c.EnsureVolume(ctx, "v1", catGeo); CodeOf(err) != CodeVolumeDeleted {
			t.Fatalf("attach of a %s volume: %v; want volume_deleted", stage, err)
		}
	}
	c.retainHook = func(stage, _ string) {
		if stage == hookHeadMarked {
			claim("deleting")
		}
	}
	res, err := c.DeleteVolume(ctx, "v1", nil)
	c.retainHook = nil
	if err != nil || !res.Completed {
		t.Fatalf("delete: %+v %v", res, err)
	}
	claim("deleted")
}

// The cut: a volume with any snapshot is refused (volume_has_snapshots)
// with HEAD restored and the admission given back; deleting the snapshot
// first lets the delete complete; snapshots are never touched by it.
func TestCatalog_DeleteVolumeRefusesWhileSnapshotsExist(t *testing.T) {
	f, b, c, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r-s1"}, h); err != nil {
		t.Fatal(err)
	}
	before := headState(t, c, "v1")
	rsv := &fakeReservation{}
	res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
	if CodeOf(err) != CodeVolumeHasSnapshots || !res.Reverted || !res.AdmissionAborted || res.Committed || rsv.state != "" {
		t.Fatalf("delete with a snapshot: %+v %v reservation=%q; want volume_has_snapshots, reverted, given back", res, err, rsv.state)
	}
	after := headState(t, c, "v1")
	if after.State != storage.ChunkHeadDetached || after.Holder != before.Holder || len(after.Chunks) != len(before.Chunks) {
		t.Fatalf("HEAD after the refusal: %+v; want the detached HEAD with its chunks", after)
	}
	if s, err := c.readSnap(ctx, "v1", "s1"); err != nil || s.State != SnapActive {
		t.Fatalf("s1 after the refusal: %+v %v; want untouched", s, err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	res, err = c.DeleteVolume(ctx, "v1", rsv.admission("d2"))
	if err != nil || !res.Completed || !res.Committed || rsv.state != reservationCommitted {
		t.Fatalf("delete after DeleteSnapshot: %+v %v", res, err)
	}
}

// A snapshot attempt in flight (name taken, publication
// held) when the delete marks HEAD: the delete refuses and reverts, and the
// snapshot then publishes cleanly. A snapshot started after the mark
// refuses itself.
func TestCatalog_InFlightSnapshotRefusesTheDelete(t *testing.T) {
	f, b, c, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	held := catBase + "/v1/" + snapsDir + "/s3/"
	f.deferCreate = func(p string, _ *filerpb.Entry) bool { return strings.HasPrefix(p, held) }
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s3", RequestID: "r-s3"}, h); err == nil {
		t.Fatal("held snapshot publication reported success")
	}
	f.deferCreate = nil
	late := false
	c.retainHook = func(stage, _ string) {
		if stage == hookHeadMarked && !late {
			late = true
			if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s4", RequestID: "r-s4"}, h); CodeOf(err) != CodeVolumeDeleted {
				t.Fatalf("snapshot started after the mark: %v; want volume_deleted", err)
			}
		}
	}
	res, err := c.DeleteVolume(ctx, "v1", nil)
	c.retainHook = nil
	if CodeOf(err) != CodeVolumeHasSnapshots || !res.Reverted {
		t.Fatalf("delete with an in-flight snapshot: %+v %v; want volume_has_snapshots, reverted", res, err)
	}
	if n := f.releaseDeferredCreates(); n != 1 {
		t.Fatalf("released %d held publications, want 1", n)
	}
	s, err := c.readSnap(ctx, "v1", "s3")
	if err != nil || s.State != SnapActive {
		t.Fatalf("s3 after the refused delete: %+v %v; want published", s, err)
	}
	if hh := headState(t, c, "v1"); hh.State != storage.ChunkHeadDetached {
		t.Fatalf("HEAD after the refused delete: %+v", hh)
	}
}

// A fork that reads during the delete mark sees the deleting source and
// refuses before installing anything; the delete refuses too (the forked
// snapshot exists) and reverts.
func TestCatalog_ForkDuringTheDeleteMarkRefuses(t *testing.T) {
	f, b, c, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r-s1"}, h); err != nil {
		t.Fatal(err)
	}
	c.retainHook = func(stage, _ string) {
		if stage == hookHeadMarked {
			c.retainHook = nil
			if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "w2", RequestID: "late"}); CodeOf(err) != CodeVolumeDeleted {
				t.Fatalf("fork during the delete mark: %v; want volume_deleted", err)
			}
		}
	}
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeHasSnapshots {
		t.Fatalf("delete: %v; want volume_has_snapshots", err)
	}
	if _, ok, _ := c.ReadVolume(ctx, "w2"); ok {
		t.Fatal("the refused fork published its VOLUME")
	}
}

// P2 (incarnation): D1 checks under deleting generation G and pauses
// before its commit. A blocker appears, D2 sees it and reverts G, and a
// new delete mark (D3's) installs generation G'. D1 must not commit G'
// (whose snapshots it never checked): it checks again, finds the blocker
// and gives back.
func TestCatalog_DeleteCommitIsBoundToItsIncarnation(t *testing.T) {
	f, b, c, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	root := path.Join(catBase, "v1")
	rsv := &fakeReservation{}
	var d2err error
	c.retainHook = func(stage, _ string) {
		if stage != hookClaimsChecked {
			return
		}
		c.retainHook = nil
		// A snapshot name appears (a creator that took its name, then saw
		// the mark and refused: its name record stays).
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "late", RequestID: "r-late"}, h); CodeOf(err) != CodeVolumeDeleted {
			t.Fatalf("snapshot during the mark: %v", err)
		}
		_, d2err = c.DeleteVolume(ctx, "v1", rsv.admission("d2"))         // sees it, reverts G
		if _, _, err := c.markHeadDeleting(ctx, "v1", root); err != nil { // D3 re-marks: G'
			t.Fatal(err)
		}
	}
	res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
	if CodeOf(d2err) != CodeVolumeHasSnapshots {
		t.Fatalf("D2: %v; want volume_has_snapshots", d2err)
	}
	if CodeOf(err) != CodeVolumeHasSnapshots || res.Committed {
		t.Fatalf("D1 after the re-mark: %+v %v; want volume_has_snapshots, not committed", res, err)
	}
	if hh := headState(t, c, "v1"); hh.State == storage.ChunkHeadPurging || hh.State == storage.ChunkHeadDeleted {
		t.Fatalf("HEAD %q: D1 committed an incarnation it did not check", hh.State)
	}
	if rsv.state == reservationCommitted {
		t.Fatal("the reservation was committed")
	}
}

// Concurrent deletes: D1 finds a snapshot and pauses before its revert;
// the snapshot is deleted and D2 joins, commits and completes. D1's revert
// then finds the HEAD committed: it neither reverts nor gives back.
func TestCatalog_ConcurrentDeleteCannotRollBackACommittedOne(t *testing.T) {
	f, b, c, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r-s1"}, h); err != nil {
		t.Fatal(err)
	}
	rsv := &fakeReservation{}
	var d2 VolumeDeleteResult
	var d2err error
	c.retainHook = func(stage, _ string) {
		if stage != hookBlocked {
			return
		}
		c.retainHook = nil
		if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
			t.Fatal(err)
		}
		d2, d2err = c.DeleteVolume(ctx, "v1", rsv.admission("d2"))
	}
	d1, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
	if d2err != nil || !d2.Completed {
		t.Fatalf("D2: %+v %v; want completed", d2, d2err)
	}
	if err == nil || d1.Reverted || d1.AdmissionAborted {
		t.Fatalf("D1 after D2 committed: %+v %v; want an error, no revert, no give-back", d1, err)
	}
	if hh := headState(t, c, "v1"); hh.State != storage.ChunkHeadDeleted || rsv.state != reservationCommitted {
		t.Fatalf("after both: HEAD %q reservation %q; want deleted, committed", hh.State, rsv.state)
	}
}

// A lost admission response is an unknown outcome (it may be recorded),
// never a refusal, and nothing is written.
func TestCatalog_DeleteVolumeLostAdmissionResponse(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	detachHead(t, f, b, "v1")
	before := len(f.allWritten())
	rsv := &fakeReservation{loseAdmit: true}
	res, err := c.DeleteVolume(context.Background(), "v1", rsv.admission("d1"))
	var ce *CatalogError
	if !errors.As(err, &ce) || ce.Code != CodeOutcomeUnknown || !ce.Retryable || !res.AdmissionUncertain || !res.Uncertain || res.Admitted {
		t.Fatalf("lost admission response: %+v %v", res, err)
	}
	if got := len(f.allWritten()); got != before || rsv.state != reservationAdmitted {
		t.Fatalf("wrote %d entries; reservation %q (the lost admission did land)", got-before, rsv.state)
	}
}

// The acceptance flow on the fake: fork w2 from s1; w2's own snapshot must
// be deleted first; then w2 is deleted, s1 can be deleted, and the id is
// never attached, recreated or forked to again; an incomplete fork w3 is
// deleted too (its claim is then dead); a rerun replays.
func TestCatalog_DeleteVolumeTombstonesAndRefusesReuse(t *testing.T) {
	f, _, c, h := deleteFixture(t)
	ctx := context.Background()
	for _, name := range []string{"s1", "s2"} {
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: "r-" + name}, h); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "w2", RequestID: "f2"}); err != nil {
		t.Fatalf("fork w2: %v", err)
	}
	if _, err := c.CreateSnapshot(ctx, "w2", SnapshotRequest{Name: "w2snap", RequestID: "rw"}, mustHead(t, c, "w2")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteVolume(ctx, "w2", nil); CodeOf(err) != CodeVolumeHasSnapshots {
		t.Fatalf("delete w2 with a snapshot: %v; want volume_has_snapshots", err)
	}
	if _, err := c.DeleteSnapshot(ctx, "w2", "w2snap"); err != nil {
		t.Fatal(err)
	}
	res, err := c.DeleteVolume(ctx, "w2", nil)
	if err != nil || res.Replayed || !res.Completed {
		t.Fatalf("delete w2: %+v %v", res, err)
	}
	if hh := headState(t, c, "w2"); hh.State != storage.ChunkHeadDeleted || len(hh.Chunks) != 0 {
		t.Fatalf("w2 HEAD: %+v; want a deleted tombstone with no chunks", hh)
	}
	if v, _, _ := c.ReadVolume(ctx, "w2"); v.State != VolumeDeleted {
		t.Fatalf("w2 VOLUME state %q", v.State)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatalf("delete s1 after its fork was deleted: %v", err)
	}
	if _, err := c.EnsureVolume(ctx, "w2", catGeo); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("attach/create of deleted w2: %v; want volume_deleted", err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w2", RequestID: "again"}); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("fork to deleted w2: %v; want volume_deleted", err)
	}
	if res, err := c.DeleteVolume(ctx, "w2", nil); err != nil || !res.Replayed {
		t.Fatalf("rerun: %+v %v; want a replay", res, err)
	}
	holdFirst(f, catBase+"/w3/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork w3: %v", err)
	}
	if got, err := c.DeleteSnapshot(ctx, "v1", "s2"); err != nil || got != SnapDeleting {
		t.Fatalf("delete s2 under the incomplete fork: %q %v; forks do not hold their parent", got, err)
	}
	if res, err := c.DeleteVolume(ctx, "w3", nil); err != nil || !res.Completed {
		t.Fatalf("delete w3: %+v %v", res, err)
	}
	if got, err := c.DeleteSnapshot(ctx, "v1", "s2"); err != nil || got != SnapAbsent {
		t.Fatalf("delete s2 again: %q %v; want absent", got, err)
	}
}

// r4 P2: the original fork request's early replay never reports success
// for a target past the delete mark, in every window (deleting, purging,
// HEAD deleted before VOLUME is marked) and after; a completed fork still
// replays before its delete.
func TestCatalog_ForkReplayRefusesADeletingTarget(t *testing.T) {
	_, _, c, h := deleteFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r-s1"}, h); err != nil {
		t.Fatal(err)
	}
	req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "w2", RequestID: "f2"}
	if _, err := c.Fork(ctx, req); err != nil {
		t.Fatalf("fork w2: %v", err)
	}
	if res, err := c.Fork(ctx, req); err != nil || !res.Replayed {
		t.Fatalf("replay before the delete: %+v %v; want a successful replay", res, err)
	}
	seen := map[string]bool{}
	c.retainHook = func(stage, _ string) {
		switch stage {
		case hookHeadMarked, hookCommitted, hookHeadDeleted:
			seen[stage] = true
			if _, err := c.Fork(ctx, req); CodeOf(err) != CodeVolumeDeleted {
				t.Fatalf("replay at %s: %v; want volume_deleted", stage, err)
			}
		}
	}
	res, err := c.DeleteVolume(ctx, "w2", nil)
	c.retainHook = nil
	if err != nil || !res.Completed || len(seen) != 3 {
		t.Fatalf("delete w2: %+v %v; windows seen %v", res, err, seen)
	}
	if _, err := c.Fork(ctx, req); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("replay after the delete: %v; want volume_deleted", err)
	}
}
