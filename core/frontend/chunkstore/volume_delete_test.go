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

// Volume delete (claude05's item; one linearization point per codex01's
// review of 7c43367). One control per review schedule.

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

// attachEntry is the HEAD an attaching holder installs over cur (a takeover:
// new holder, next epoch).
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

func deleteFixture(t *testing.T) (*fakeFiler, *fakeBlobs, *Catalog, storage.ChunkHead) {
	t.Helper()
	f, b, c, h := catalogFixture(t) // v1 HEAD held by h1 (attached)
	ctx := context.Background()
	for _, vol := range []string{"v1", "w2", "w3"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"s1", "s2"} {
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: "r-" + name}, h); err != nil {
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

// An attached volume (HEAD held), or one the admission refuses, is refused
// with volume_in_use and nothing is written.
func TestCatalog_DeleteVolumeRefusesAVolumeInUse(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	before := len(f.allWritten())
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete of an attached volume: %v; want volume_in_use", err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("a refused delete wrote %d entries", got-before)
	}
	detachHead(t, f, b, "v1")
	before = len(f.allWritten())
	res, err := c.DeleteVolume(ctx, "v1", (&fakeReservation{refuse: true}).admission("d1"))
	var ce *CatalogError
	if !errors.As(err, &ce) || ce.Code != CodeVolumeInUse || ce.Retryable || !strings.Contains(ce.Detail(), "assigned to r1") || res.Admitted || res.AdmissionUncertain {
		t.Fatalf("delete the admission refuses: %+v %v; want volume_in_use naming the reason", res, err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("a refused delete wrote %d entries", got-before)
	}
}

// Schedule 1: a holder attaches after the delete's first read, before the
// HEAD mark's read. The mark reads the held HEAD and refuses; HEAD stays
// the holder's.
func TestCatalog_DeleteVolumeRefusesAnAttachBeforeTheMark(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	headPath := path.Join(catBase, "v1", headName)
	lookups := 0
	f.afterLookup = func(p string) {
		if p != headPath {
			return
		}
		if lookups++; lookups == 1 { // the delete's first read; the mark reads next
			cur, _ := decodeHead(p, f.entries[p])
			f.entries[p] = attachEntry(t, cur, "h2")
		}
	}
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete racing an attach: %v; want volume_in_use", err)
	}
	f.afterLookup = nil
	if h := headState(t, c, "v1"); h.Holder != "h2" || h.State != storage.ChunkHeadActive {
		t.Fatalf("HEAD after the refused delete: %+v; want h2's, active", h)
	}
}

// An attach between the mark's read and its write makes the write's
// condition fail; the re-read sees the holder and refuses.
func TestCatalog_DeleteVolumeLosesToAnAttachAtTheMark(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	headPath := path.Join(catBase, "v1", headName)
	lookups := 0
	f.afterLookup = func(p string) {
		if p != headPath {
			return
		}
		if lookups++; lookups == 2 { // the mark's read; the write is conditioned on it
			cur, _ := decodeHead(p, f.entries[p])
			f.entries[p] = attachEntry(t, cur, "h2")
		}
	}
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete losing to an attach: %v; want volume_in_use", err)
	}
	f.afterLookup = nil
	if h := headState(t, c, "v1"); h.Holder != "h2" || h.State != storage.ChunkHeadActive {
		t.Fatalf("HEAD after the lost delete: %+v; want h2's", h)
	}
}

// Schedule 2: an attach that passed EnsureVolume before the mark claims
// HEAD afterwards (CASHead from the deleting or deleted HEAD it reads): the
// remote refuses, so nothing revives the volume.
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
	if h := headState(t, c, "v1"); h.State != storage.ChunkHeadDeleted || h.Holder != "" || len(h.Chunks) != 0 {
		t.Fatalf("HEAD after the delete: %+v; want the deleted tombstone", h)
	}
}

// A fork claim that lands before the claims are read (after the mark)
// makes the delete give the mark back: snapshot_claimed, HEAD restored,
// nothing irreversible done.
func TestCatalog_DeleteVolumeGivesBackOnALiveClaim(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	before := headState(t, c, "v1")
	holdFirst(f, catBase+"/w3/HEAD")
	c.retainHook = func(stage, _ string) {
		if stage == hookHeadMarked { // the claim lands after the mark, before the read of claims
			c.retainHook = nil
			// Fork's own claim write is what lands here; its post-check then
			// refuses on the deleting HEAD, so take only the claim.
			if _, err := c.claimForkParent(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); err != nil {
				t.Fatal(err)
			}
			// The fork published its VOLUME before the delete began.
			ve, _ := volumeEntry(VolumeInfo{Format: volumeFormat, VolumeID: "w3", Geometry: catGeo, Origin: OriginFork, Parent: &Parent{VolumeID: "v1", Snapshot: "s2"},
				Fork: &ForkRecord{RequestID: "f3", Source: "v1", Snapshot: "s2"}}, "f3")
			if err := f.CreateEntry(ctx, catBase+"/w3", ve, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	rsv := &fakeReservation{}
	res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
	if CodeOf(err) != CodeSnapshotClaimed || !res.Reverted || res.HeadMarked || len(res.SnapshotsDeleted) != 0 || !res.AdmissionAborted || res.Committed {
		t.Fatalf("delete under a live claim: %+v %v; want snapshot_claimed, reverted, admission given back", res, err)
	}
	if rsv.state != "" {
		t.Fatalf("reservation after the give-back: %q; want none (the volume is usable again)", rsv.state)
	}
	after := headState(t, c, "v1")
	if after.State != storage.ChunkHeadDetached || after.DetachedBy == nil || after.Holder != before.Holder || len(after.Chunks) != len(before.Chunks) {
		t.Fatalf("HEAD after the give-back: %+v; want the detached HEAD with its chunks", after)
	}
	if v, _, _ := c.ReadVolume(ctx, "v1"); v.State != "" {
		t.Fatalf("VOLUME state %q after the give-back", v.State)
	}
	for _, s := range []string{"s1", "s2"} {
		if _, err := c.readSnap(ctx, "v1", s); err != nil {
			t.Fatalf("%s after the give-back: %v", s, err)
		}
	}
}

// A fork that claims after the claims were read sees the deleting HEAD and
// gives its claim back before installing anything; the delete completes.
func TestCatalog_ForkAfterTheDeleteMarkRefuses(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	ran := false
	c.retainHook = func(stage, _ string) {
		if stage != hookClaimsChecked || ran {
			return
		}
		ran = true
		if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "w2", RequestID: "late"}); CodeOf(err) != CodeVolumeDeleted {
			t.Fatalf("fork after the delete mark: %v; want volume_deleted", err)
		}
	}
	res, err := c.DeleteVolume(ctx, "v1", nil)
	if err != nil || !ran || !res.Completed || strings.Join(res.SnapshotsDeleted, ",") != "s1,s2" {
		t.Fatalf("delete: ran=%t %+v %v", ran, res, err)
	}
	if _, ok, _ := c.ReadVolume(ctx, "w2"); ok {
		t.Fatal("the refused fork published its VOLUME")
	}
}

// P2: a snapshot whose publication is held while the delete lists the names
// (its name record exists, its attempt not yet) is tombstoned, so its late
// publish cannot land; a snapshot started after the mark refuses itself.
func TestCatalog_DeleteVolumeCatchesInFlightSnapshots(t *testing.T) {
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
	if err != nil || !res.Completed || strings.Join(res.AttemptsAborted, ",") != "s3" {
		t.Fatalf("delete: %+v %v; want s3's unpublished attempt aborted", res, err)
	}
	f.releaseDeferredCreates() // the held publish finally runs
	for _, name := range []string{"s3", "s4"} {
		if _, ok, _ := c.resolveSnap(ctx, "v1", name); ok {
			t.Fatalf("%s's name survived the delete", name)
		}
	}
	snaps, err := c.listSnapshots(ctx, "v1", true)
	if err != nil || len(snaps) != 0 {
		t.Fatalf("snapshots left after the delete: %v %v", snaps, err)
	}
	attempts, _ := f.ListEntries(ctx, held[:len(held)-1], "", "", 0)
	for _, e := range attempts {
		if s := string(e.Extended[extSnapState]); s != SnapAborted {
			t.Fatalf("s3 attempt %s is %q after the late publish; want the aborted tombstone", e.GetName(), s)
		}
	}
}

// The acceptance flow on the fake: fork w2 from s1, delete w2, then s1 can
// be deleted; the deleted id is never attached, recreated or forked to
// again; an incomplete fork w3 is deleted and its claim released (only
// when confirmed); a rerun replays.
func TestCatalog_DeleteVolumeTombstonesAndReleases(t *testing.T) {
	f, _, c, _ := deleteFixture(t)
	ctx := context.Background()
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "w2", RequestID: "f2"}); err != nil {
		t.Fatalf("fork w2: %v", err)
	}
	if _, err := c.CreateSnapshot(ctx, "w2", SnapshotRequest{Name: "w2snap", RequestID: "rw"}, mustHead(t, c, "w2")); err != nil {
		t.Fatal(err)
	}
	res, err := c.DeleteVolume(ctx, "w2", nil)
	if err != nil || res.Replayed || !res.Completed || strings.Join(res.SnapshotsDeleted, ",") != "w2snap" {
		t.Fatalf("delete w2: %+v %v", res, err)
	}
	if h := headState(t, c, "w2"); h.State != storage.ChunkHeadDeleted || len(h.Chunks) != 0 {
		t.Fatalf("w2 HEAD: %+v; want a deleted tombstone with no chunks", h)
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

	// Incomplete fork w3: a failed claim removal is not reported released.
	holdFirst(f, catBase+"/w3/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork w3: %v", err)
	}
	s2 := path.Join(catBase, "v1", snapsDir, "s2")
	f.failUpdate = func(p string) error {
		if strings.HasPrefix(p, s2+"/") {
			return errors.New("filer unavailable")
		}
		return nil
	}
	res, err = c.DeleteVolume(ctx, "w3", nil)
	if err == nil || res.ClaimReleased || !res.Uncertain || res.Completed {
		t.Fatalf("delete w3 with a failing claim removal: %+v %v; want an error, claim not released, uncertain", res, err)
	}
	f.failUpdate = nil
	rsv := &fakeReservation{}
	res, err = c.DeleteVolume(ctx, "w3", rsv.admission("d3"))
	if err != nil || !res.ClaimReleased || !res.Completed || !res.Committed || rsv.state != reservationCommitted {
		t.Fatalf("delete w3 rerun: %+v %v; want its claim released", res, err)
	}
	if got := claimsOf(t, c, "s2"); len(got) != 0 {
		t.Fatalf("s2 claims after w3 was deleted: %v", got)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s2"); err != nil {
		t.Fatalf("delete s2 after w3 was deleted: %v", err)
	}
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

// A held HEAD found after the admission gives the admission back.
func TestCatalog_DeleteVolumeHeldAfterAdmissionGivesBack(t *testing.T) {
	_, _, c, _ := deleteFixture(t) // v1 is attached
	rsv := &fakeReservation{}
	res, err := c.DeleteVolume(context.Background(), "v1", rsv.admission("d1"))
	if CodeOf(err) != CodeVolumeInUse || !res.Admitted || !res.AdmissionAborted || rsv.state != "" {
		t.Fatalf("held after admission: %+v %v reservation=%q; want volume_in_use and the admission given back", res, err, rsv.state)
	}
}

// A lost admission response is reported as an unknown outcome (it may be
// recorded), never as a plain refusal, and nothing is written.
func TestCatalog_DeleteVolumeLostAdmissionResponse(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	detachHead(t, f, b, "v1")
	before := len(f.allWritten())
	rsv := &fakeReservation{loseAdmit: true}
	res, err := c.DeleteVolume(context.Background(), "v1", rsv.admission("d1"))
	var ce *CatalogError
	if !errors.As(err, &ce) || ce.Code != CodeOutcomeUnknown || !ce.Retryable || !res.AdmissionUncertain || !res.Uncertain || res.Admitted {
		t.Fatalf("lost admission response: %+v %v; want outcome_unknown, admission uncertain", res, err)
	}
	if got := len(f.allWritten()); got != before || rsv.state != reservationAdmitted {
		t.Fatalf("wrote %d entries; reservation %q (the lost admission did land)", got-before, rsv.state)
	}
}

// P2 (concurrent deletes): D1 samples a live claim and pauses before its
// revert; the fork completes, D2 joins the deleting HEAD, finds no claim,
// commits and deletes. D1's revert then finds the HEAD past the commit
// point: it neither reverts nor gives its admission back, and the
// reservation stays committed.
func TestCatalog_ConcurrentDeleteCannotRollBackACommittedOne(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	holdFirst(f, catBase+"/w3/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork w3: %v", err)
	}
	rsv := &fakeReservation{}
	var d2 VolumeDeleteResult
	var d2err error
	c.retainHook = func(stage, _ string) {
		if stage != hookClaimFound {
			return
		}
		c.retainHook = nil
		// The fork completes (its claim is no longer live), then D2 runs.
		if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); CodeOf(err) != CodeVolumeDeleted {
			// Its retry sees the deleting source and backs out; give the
			// target a real HEAD as a completed fork would have.
			t.Logf("fork retry during the delete: %v", err)
		}
		forceHead(t, f, b, "w3")
		d2, d2err = c.DeleteVolume(ctx, "v1", rsv.admission("d2"))
	}
	d1, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
	if d2err != nil || !d2.Completed {
		t.Fatalf("D2: %+v %v; want completed", d2, d2err)
	}
	if err == nil || d1.Reverted || d1.AdmissionAborted {
		t.Fatalf("D1 after D2 committed: %+v %v; want an error, no revert, no give-back", d1, err)
	}
	if h := headState(t, c, "v1"); h.State != storage.ChunkHeadDeleted {
		t.Fatalf("HEAD after both: %+v; want deleted", h)
	}
	if rsv.state != reservationCommitted {
		t.Fatalf("reservation %q; want committed", rsv.state)
	}
}

// forceHead installs a real active HEAD for vol (a completed fork's).
func forceHead(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	e, err := headEntry(storage.ChunkHead{Gen: 1, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadActive})
	if err != nil {
		t.Fatal(err)
	}
	f.entries[path.Join(catBase, vol, headName)] = e
	_ = b
}

// P2 (failed abort, late publication): snapshot A takes name s5 and parks
// before publishing; the delete marks HEAD; A's same-request retry B sees
// the mark and tries to abort the attempt, but the tombstone write fails:
// the name must stay (not freed), so the delete still finds and settles it
// and the late publication cannot leave an active root.
func TestCatalog_FailedAbortCannotHideALatePublication(t *testing.T) {
	f, b, c, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	held := catBase + "/v1/" + snapsDir + "/s5/"
	f.deferCreate = func(p string, _ *filerpb.Entry) bool { return strings.HasPrefix(p, held) }
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s5", RequestID: "r-s5"}, h); err == nil {
		t.Fatal("A's held publication reported success")
	}
	f.deferCreate = nil
	c.retainHook = func(stage, _ string) {
		if stage != hookHeadMarked {
			return
		}
		c.retainHook = nil
		f.failCreate = func(p string) error {
			if strings.HasPrefix(p, held) {
				return errors.New("filer unavailable")
			}
			return nil
		}
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s5", RequestID: "r-s5"}, h); CodeOf(err) != CodeVolumeDeleted {
			t.Fatalf("B, the same request after the mark: %v; want volume_deleted", err)
		}
		f.failCreate = nil
		if _, ok, _ := c.resolveSnap(ctx, "v1", "s5"); !ok {
			t.Fatal("B freed s5's name although its abort failed")
		}
	}
	res, err := c.DeleteVolume(ctx, "v1", nil)
	if err != nil || !res.Completed {
		t.Fatalf("delete: %+v %v", res, err)
	}
	f.releaseDeferredCreates() // A's publish finally runs
	attempts, _ := f.ListEntries(ctx, held[:len(held)-1], "", "", 0)
	for _, e := range attempts {
		if s := string(e.Extended[extSnapState]); s == SnapActive {
			t.Fatalf("an active s5 attempt %s survived the completed delete", e.GetName())
		}
	}
}

func mustHead(t *testing.T, c *Catalog, vol string) storage.ChunkHead {
	t.Helper()
	h, has, err := c.ReadHead(context.Background(), vol)
	if err != nil || !has {
		t.Fatalf("HEAD of %s: has=%t %v", vol, has, err)
	}
	return h
}
