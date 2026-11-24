package chunkstore

import (
	"context"
	"errors"
	"path"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Volume delete (no snapshot cascade).
//
//	(a) Admission: the caller's Admission (the CLI's: the blockmaster's
//	    durable deletion reservation, MarkVolumeDeleting; refused while the
//	    volume has live or pending authority; while held, no new assignment,
//	    placement or binding is admitted and the id is not created again).
//	    Every give-back below aborts this delete's admission (restoring the
//	    volume's usability); the commit point commits it (permanent).
//	(b) One HEAD write from the HEAD read (active or detached, NOT held: an
//	    active HEAD that names a holder belongs to an attached server) to
//	    state deleting, chunks kept: a new incarnation of the delete mark
//	    (its generation). ChunkRemote.CASHead refuses to move a deleting,
//	    purging or deleted HEAD, so no attach, claim or restore can revive
//	    it; EnsureVolume refuses it; CreateSnapshot and Fork check the
//	    volume after taking their name or claim and refuse.
//	(c) Under that incarnation, the volume must have no snapshot at all: no
//	    snapshot name record (a published snapshot, one being deleted, or an
//	    attempt still in flight). Any is volume_has_snapshots: HEAD is
//	    reverted and the admission given back. The operator deletes the
//	    snapshots first (DeleteSnapshot); a delete never touches snapshot
//	    names, attempts or their roots. (A fork claim lives on a named
//	    snapshot, so it is covered too.)
//	(c') The commit point: HEAD deleting -> purging for that same
//	    incarnation only (conditioned on its generation); if HEAD moved (a
//	    revert and re-mark by another delete), the check is made again on
//	    the incarnation found. A revert is bound the same way, so no delete
//	    can undo another's commit, nor commit an incarnation it did not
//	    check. Then the admission is committed.
//	(e) HEAD becomes deleted (no chunks: nothing it held stays a GC root) and
//	    VOLUME is marked deleted. GC reclaims.
//
// A call that fails reports what it did (VolumeDeleteResult) with the
// error; a rerun resumes from the state it finds.

const (
	CodeVolumeInUse        = "volume_in_use"
	CodeVolumeDeleted      = "volume_deleted"
	CodeVolumeHasSnapshots = "volume_has_snapshots"

	// VolumeDeleted is the state of a deleted volume's VOLUME entry.
	VolumeDeleted = "deleted"

	extVolState     = "swb.volume.state"
	extVolDeletedNs = "swb.volume.deleted_ns"

	// Test seams (retainHook stages) in a DeleteVolume.
	hookHeadMarked    = "head-marked"    // after (b)
	hookBlocked       = "blocked"        // (c) found a snapshot, before the revert
	hookClaimsChecked = "claims-checked" // after (c), before the commit point
	hookCommitted     = "committed"      // after the commit point (HEAD purging)
	hookHeadDeleted   = "head-deleted"   // HEAD deleted, VOLUME not yet marked
)

// ErrNotAdmitted marks an Admission error that is a definite refusal
// (nothing was recorded). Any other Admission error leaves the outcome
// unknown.
var ErrNotAdmitted = errors.New("chunkstore: volume delete not admitted")

// Admission is the delete's reservation outside the catalog (the CLI's is
// the blockmaster's). Admit before anything is written; Commit at the
// irreversible boundary; Abort when the delete gives back with no
// irreversible effect.
type Admission interface {
	Admit(ctx context.Context, vol string) error
	Commit(ctx context.Context, vol string) error
	Abort(ctx context.Context, vol string) error
}

// VolumeDeleteResult is what DeleteVolume did, also on failure.
type VolumeDeleteResult struct {
	VolumeID string `json:"volumeID"`
	Replayed bool   `json:"replayed"` // it was already deleted
	Admitted bool   `json:"admitted"` // (a) passed
	// AdmissionUncertain: the admission call failed with an unknown
	// outcome (it may have been recorded); AdmissionAborted: this delete
	// gave its admission back; Committed: the admission is permanent.
	AdmissionUncertain bool `json:"admissionUncertain"`
	AdmissionAborted   bool `json:"admissionAborted"`
	Committed          bool `json:"committed"`
	HeadMarked         bool `json:"headMarked"` // HEAD is deleting or deleted
	Reverted           bool `json:"reverted"`   // (c) gave the HEAD mark back
	Completed          bool `json:"completed"`  // HEAD and VOLUME are deleted
	// Uncertain is set when the failing call was a write whose outcome is
	// unknown; a rerun settles it.
	Uncertain bool `json:"uncertain"`
}

// DeleteVolume deletes vol (see the header). admit may be nil (a catalog-
// only delete, e.g. in tests): (b) still refuses a held HEAD.
func (c *Catalog) DeleteVolume(ctx context.Context, vol string, admit Admission) (VolumeDeleteResult, error) {
	res := VolumeDeleteResult{VolumeID: vol}
	root, err := c.volRoot(vol)
	if err != nil {
		return res, err
	}
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return res, err
	}
	if !ok {
		return res, catErr(CodeNotChunkVolume, false, nil, "volume %s has no VOLUME entry", vol)
	}
	h, hasHead, err := c.ReadHead(ctx, vol)
	if err != nil {
		return res, err
	}
	if !hasHead {
		// The delete would write the first HEAD (deleting); a refused delete
		// must not leave one behind (its revert would make it an active,
		// empty HEAD over the volume's keys). Decide the refusal first.
		live, err := c.snapshotsBlock(ctx, vol, root)
		if err != nil {
			return res, err
		}
		if live != nil {
			return res, live
		}
	}
	res.Replayed = v.State == VolumeDeleted && h.State == storage.ChunkHeadDeleted
	// (a) Every call takes its own admission (a rerun too: it settles a
	// commit an earlier run may have lost).
	if admit != nil {
		if err := admit.Admit(ctx, vol); err != nil {
			if errors.Is(err, ErrNotAdmitted) {
				return res, catErr(CodeVolumeInUse, false, err, "volume %s: delete not admitted", vol)
			}
			res.AdmissionUncertain, res.Uncertain = true, true
			return res, catErr(CodeOutcomeUnknown, true, err, "volume %s: delete admission outcome unknown (it may be recorded); rerun the delete", vol)
		}
	}
	res.Admitted = true
	giveBack := func(cause error) (VolumeDeleteResult, error) {
		if admit != nil {
			if err := admit.Abort(ctx, vol); err != nil {
				res.Uncertain = true
				return res, catErr(CodeOutcomeUnknown, true, err, "volume %s: %v; giving the admission back failed, rerun the delete", vol, cause)
			}
			res.AdmissionAborted = true
		}
		return res, cause
	}
	// (b), (c), (c'), each bound to one deleting incarnation (the HEAD
	// generation the mark produced or joined): a claim check is only valid
	// for the generation it ran under, so if HEAD moved (another delete
	// reverted and re-marked it) the check is run again on the new one.
	for incarnation := 0; h.State != storage.ChunkHeadPurging && h.State != storage.ChunkHeadDeleted; incarnation++ {
		if incarnation == 5 {
			return res, remoteErr(nil, "volume %s: the delete mark kept changing", vol)
		}
		gen, u, err := c.markHeadDeleting(ctx, vol, root)
		if err != nil {
			if u {
				res.Uncertain = true
				return res, err
			}
			return giveBack(err)
		}
		res.HeadMarked = true
		c.hook(hookHeadMarked, vol)
		// (c)
		live, err := c.snapshotsBlock(ctx, vol, root)
		if err != nil {
			return res, err
		}
		if live != nil {
			c.hook(hookBlocked, vol)
			out, u, rerr := c.revertHeadDeleting(ctx, vol, root, gen)
			switch {
			case rerr != nil:
				res.Uncertain = u
				return res, rerr
			case out == incarnationMoved:
				if h, _, err = c.ReadHead(ctx, vol); err != nil {
					return res, err
				}
				continue // decide again on what HEAD is now
			case out == incarnationCommitted:
				// Another delete committed meanwhile: its irreversible steps
				// stand; this call neither reverts nor gives back.
				return res, catErr(CodeOutcomeUnknown, true, nil, "volume %s: another delete committed while this one found %v; rerun the delete", vol, live)
			}
			res.Reverted, res.HeadMarked = true, false
			return giveBack(live)
		}
		c.hook(hookClaimsChecked, vol)
		// (c') the commit point, on the incarnation the check ran under
		out, u, err := c.commitHeadPurging(ctx, vol, root, gen)
		if err != nil {
			res.Uncertain = u
			return res, err
		}
		if out == incarnationDone {
			break
		}
		if out == incarnationGivenBack {
			res.HeadMarked = false
			return giveBack(catErr(CodeOutcomeUnknown, true, nil, "volume %s: a concurrent delete gave the mark back; rerun the delete", vol))
		}
		if h, _, err = c.ReadHead(ctx, vol); err != nil { // moved: validate again
			return res, err
		}
	}
	res.HeadMarked = true
	c.hook(hookCommitted, vol)
	if admit != nil {
		if err := admit.Commit(ctx, vol); err != nil {
			res.Uncertain = true
			return res, catErr(CodeOutcomeUnknown, true, err, "volume %s: committing the admission failed; rerun the delete", vol)
		}
	}
	res.Committed = true
	// (e)
	if u, err := c.finishHeadDeleted(ctx, vol, root); err != nil {
		res.Uncertain = u
		return res, err
	}
	c.hook(hookHeadDeleted, vol)
	if err := c.markVolumeDeleted(ctx, vol, root); err != nil {
		res.Uncertain = true
		return res, err
	}
	res.Completed = true
	return res, nil
}

// headHeld: an active HEAD that names a holder belongs to an attached
// volume server (a clean detach publishes state detached).
func headHeld(h storage.ChunkHead) bool {
	return (h.State == "" || h.State == storage.ChunkHeadActive) && h.Holder != ""
}

// headDeletingOrDeleted reports a HEAD a delete has passed its
// linearization point on.
func headDeletingOrDeleted(h storage.ChunkHead) bool {
	return h.State == storage.ChunkHeadDeleting || h.State == storage.ChunkHeadPurging || h.State == storage.ChunkHeadDeleted
}

// writeHead writes next over the HEAD read as cur (an exclusive create when
// there was none), conditioned on cur's generation and epoch. It is the
// catalog's own HEAD write for the delete transitions, which CASHead
// refuses to anyone else.
func (c *Catalog) writeHead(ctx context.Context, root string, cur, next storage.ChunkHead) error {
	next.Gen = cur.Gen + 1
	var ids []string
	if next.Segments == nil && len(next.Chunks) != 0 {
		v, ok, err := c.ReadVolume(ctx, path.Base(root))
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("chunkstore: legacy delete needs VOLUME geometry")
		}
		next, ids, err = c.prepareLegacyHead(ctx, root, cur, next, v.Geometry)
		if err != nil {
			c.dropManifestIntents(ctx, root, ids)
			return err
		}
	}
	e, err := headEntry(next)
	if err != nil {
		c.dropManifestIntents(ctx, root, ids)
		return err
	}
	if cur.Gen == 0 {
		err = c.f.CreateEntry(ctx, root, e, true)
	} else {
		err = c.f.UpdateEntry(ctx, root, e, map[string][]byte{
			extGen: []byte(strconv.FormatUint(cur.Gen, 10)), extEpoch: []byte(strconv.FormatUint(cur.Epoch, 10)),
		})
	}
	// Unknown publication retains protection; a later sweep can settle it.
	if err == nil || lostCondition(err) {
		c.dropManifestIntents(ctx, root, ids)
	}
	return err
}

func lostCondition(err error) bool {
	return errors.Is(err, ErrPrecondition) || errors.Is(err, ErrExists) || errors.Is(err, ErrNotFound)
}

// markHeadDeleting is (b): every read decides afresh whether the HEAD is
// held, so an attach that landed before this read is refused, and one that
// lands after it makes the write's condition fail. gen is the generation of
// the deleting HEAD this call produced or joined (its incarnation).
func (c *Catalog) markHeadDeleting(ctx context.Context, vol, root string) (gen uint64, uncertain bool, err error) {
	for try := 0; try < 5; try++ {
		h, _, err := c.ReadHead(ctx, vol)
		if err != nil {
			return 0, false, err
		}
		if h.State == storage.ChunkHeadDeleting {
			return h.Gen, false, nil // join this incarnation
		}
		if headDeletingOrDeleted(h) {
			return h.Gen, false, nil
		}
		if headHeld(h) {
			return 0, false, catErr(CodeVolumeInUse, false, nil, "volume %s is attached: HEAD is held by %q at epoch %d; detach it first", vol, h.Holder, h.Epoch)
		}
		next := h
		next.State = storage.ChunkHeadDeleting
		if next.Chunks == nil {
			next.Chunks = map[uint64]storage.ChunkRef{}
		}
		err = c.writeHead(ctx, root, h, next)
		if lostCondition(err) {
			continue
		}
		if err != nil {
			return 0, true, catErr(CodeOutcomeUnknown, true, err, "mark HEAD of %s deleting: outcome unknown; rerun the delete", vol)
		}
		return h.Gen + 1, false, nil
	}
	return 0, false, remoteErr(nil, "HEAD of %s kept changing", vol)
}

// What a revert or commit found of the incarnation it was bound to.
type incarnationOutcome int

const (
	incarnationDone      incarnationOutcome = iota // reverted / committed by this call, or committed already
	incarnationMoved                               // HEAD is another deleting incarnation now: validate again
	incarnationCommitted                           // past the commit point (by another delete)
	incarnationGivenBack                           // active or detached again (another delete reverted)
)

// revertHeadDeleting gives (b) back for incarnation gen only: deleting
// returns to detached when the HEAD carries its holder's detach record,
// else to active.
func (c *Catalog) revertHeadDeleting(ctx context.Context, vol, root string, gen uint64) (incarnationOutcome, bool, error) {
	h, _, err := c.ReadHead(ctx, vol)
	if err != nil {
		return 0, false, err
	}
	switch {
	case h.State == storage.ChunkHeadPurging || h.State == storage.ChunkHeadDeleted:
		return incarnationCommitted, false, nil
	case h.State != storage.ChunkHeadDeleting:
		return incarnationGivenBack, false, nil // already given back: nothing to do
	case h.Gen != gen:
		return incarnationMoved, false, nil
	}
	next := h
	next.State = storage.ChunkHeadActive
	if h.DetachedBy != nil {
		next.State = storage.ChunkHeadDetached
		d := *h.DetachedBy
		d.Gen = h.Gen + 1
		next.DetachedBy = &d
	}
	err = c.writeHead(ctx, root, h, next)
	if lostCondition(err) {
		return incarnationMoved, false, nil // HEAD moved under the write: decide again
	}
	if err != nil {
		return 0, true, catErr(CodeOutcomeUnknown, true, err, "revert the delete mark on HEAD of %s: outcome unknown; rerun the delete", vol)
	}
	return incarnationDone, false, nil
}

// commitHeadPurging is the commit point for incarnation gen only: deleting
// -> purging, conditioned on that exact HEAD.
func (c *Catalog) commitHeadPurging(ctx context.Context, vol, root string, gen uint64) (incarnationOutcome, bool, error) {
	h, _, err := c.ReadHead(ctx, vol)
	if err != nil {
		return 0, false, err
	}
	switch {
	case h.State == storage.ChunkHeadPurging || h.State == storage.ChunkHeadDeleted:
		return incarnationDone, false, nil // committed (by another delete of the same mark)
	case h.State != storage.ChunkHeadDeleting:
		return incarnationGivenBack, false, nil
	case h.Gen != gen:
		return incarnationMoved, false, nil // another incarnation: its claims were not checked here
	}
	next := h
	next.State = storage.ChunkHeadPurging
	err = c.writeHead(ctx, root, h, next)
	if lostCondition(err) {
		return incarnationMoved, false, nil
	}
	if err != nil {
		return 0, true, catErr(CodeOutcomeUnknown, true, err, "commit the delete of %s: outcome unknown; rerun the delete", vol)
	}
	return incarnationDone, false, nil
}

// finishHeadDeleted is (e) for HEAD: deleting becomes deleted, no chunks.
func (c *Catalog) finishHeadDeleted(ctx context.Context, vol, root string) (bool, error) {
	for try := 0; try < 5; try++ {
		h, _, err := c.ReadHead(ctx, vol)
		if err != nil {
			return false, err
		}
		if h.State == storage.ChunkHeadDeleted {
			return false, nil
		}
		if h.State != storage.ChunkHeadPurging {
			return false, remoteErr(nil, "HEAD of %s is not committed for deletion (%q)", vol, h.State)
		}
		next := storage.ChunkHead{Epoch: h.Epoch, RemoteLSN: h.RemoteLSN, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadDeleted}
		err = c.writeHead(ctx, root, h, next)
		if lostCondition(err) {
			continue
		}
		if err != nil {
			return true, catErr(CodeOutcomeUnknown, true, err, "tombstone HEAD of %s: outcome unknown; rerun the delete", vol)
		}
		return false, nil
	}
	return false, remoteErr(nil, "HEAD of %s kept changing", vol)
}

// snapshotsBlock returns volume_has_snapshots when vol has any snapshot
// name record (published, being deleted, or an attempt in flight), or nil.
func (c *Catalog) snapshotsBlock(ctx context.Context, vol, root string) (error, error) {
	names, err := c.listNames(ctx, root)
	if err != nil {
		return nil, err
	}
	if len(names) > 0 {
		return catErr(CodeVolumeHasSnapshots, false, nil, "volume %s has snapshots (%s); delete them first", vol, strings.Join(names, ", ")), nil
	}
	return nil, nil
}

// listNames lists every snapshot name record of the volume at root.
func (c *Catalog) listNames(ctx context.Context, root string) ([]string, error) {
	dir := path.Join(root, snapNamesDir)
	var out []string
	after := ""
	for {
		page, err := c.f.ListEntries(ctx, dir, "", after, listPage)
		if err != nil {
			return nil, remoteErr(err, "list the snapshot names under %s", root)
		}
		for _, e := range page {
			if e.GetName() <= after {
				return nil, errors.New("chunkstore: non-advancing snapshot name listing")
			}
			after = e.GetName()
			if !e.GetIsDirectory() {
				out = append(out, e.GetName())
			}
		}
		if len(page) < listPage {
			return out, nil
		}
	}
}

// markVolumeDeleted marks vol's VOLUME entry deleted, conditioned on it not
// being marked yet.
func (c *Catalog) markVolumeDeleted(ctx context.Context, vol, root string) error {
	e, err := c.f.LookupEntry(ctx, root, volumeName)
	if err != nil {
		return remoteErr(err, "read VOLUME of %s", vol)
	}
	if string(e.Extended[extVolState]) == VolumeDeleted {
		return nil
	}
	next := proto.Clone(e).(*filerpb.Entry)
	if next.Extended == nil {
		next.Extended = map[string][]byte{}
	}
	next.Extended[extVolState] = []byte(VolumeDeleted)
	next.Extended[extVolDeletedNs] = []byte(strconv.FormatInt(c.now().UnixNano(), 10))
	err = c.f.UpdateEntry(ctx, root, next, map[string][]byte{extVolState: nil})
	if errors.Is(err, ErrPrecondition) {
		return nil // marked by a concurrent delete
	}
	if err != nil {
		return remoteErr(err, "mark VOLUME of %s deleted", vol)
	}
	return nil
}

// refuseDeleted is volume_deleted for a deleted (or deleting) volume id.
func refuseDeleted(vol, what string) error {
	return catErr(CodeVolumeDeleted, false, nil, "volume %s is deleted or being deleted; %s (a deleted id is never reused)", vol, what)
}

// volumeGone reports whether vol is being deleted or deleted: its VOLUME
// is marked, or its HEAD is past the delete's linearization point.
func (c *Catalog) volumeGone(ctx context.Context, vol string) (bool, error) {
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return false, err
	}
	if ok && v.State == VolumeDeleted {
		return true, nil
	}
	h, has, err := c.ReadHead(ctx, vol)
	if err != nil {
		return false, err
	}
	return has && headDeletingOrDeleted(h), nil
}
