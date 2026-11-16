package chunkstore

import (
	"context"
	"errors"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Volume delete (claude05's item, 2026-11-16 02:39Z; one linearization
// point per codex01's review of 7c43367).
//
//	(a) Admission: the caller's Admission (the CLI's: the blockmaster's
//	    durable deletion reservation, MarkVolumeDeleting; refused while the
//	    volume has live or pending authority; while held, no new assignment,
//	    placement or binding is admitted and the id is not created again).
//	    Every give-back below aborts this delete's admission (restoring the
//	    volume's usability); the commit point commits it (permanent).
//	(b) The linearization point: one conditional write of HEAD from the
//	    HEAD read (active or detached, NOT held: an active HEAD that names a
//	    holder belongs to an attached server) to state deleting, chunks
//	    kept. ChunkRemote.CASHead refuses to move a deleting or deleted
//	    HEAD, so no attach, claim or restore can revive it; EnsureVolume
//	    refuses it; Fork and CreateSnapshot check HEAD again after taking
//	    their claim or name and back out when it is deleting.
//	(c) Claims are read after (b): a live fork claim reverts HEAD to its
//	    previous state (nothing irreversible was done) and refuses
//	    (snapshot_claimed). A fork claiming later sees the deleting HEAD
//	    and gives its claim back.
//	(c') The commit point: HEAD deleting -> purging, conditioned on the
//	    HEAD read, then the admission is committed. A revert and a commit
//	    race on the same conditional write, so a delete that sampled a
//	    claim can never undo another delete's irreversible steps, and a
//	    delete whose mark was given back never starts them.
//	(d) Only then the irreversible steps: every snapshot name, published or
//	    not (an unpublished attempt gets an aborted tombstone so its late
//	    publish cannot land), is marked through markDeleting (the one mark
//	    path) and freed; the volume's own fork claim is released.
//	(e) Finally HEAD becomes deleted (no chunks: nothing it held stays a GC
//	    root) and VOLUME is marked deleted. GC reclaims.
//
// A call that fails reports what it did (VolumeDeleteResult) with the
// error; a rerun resumes from the state it finds.

const (
	CodeVolumeInUse   = "volume_in_use"
	CodeVolumeDeleted = "volume_deleted"

	// VolumeDeleted is the state of a deleted volume's VOLUME entry.
	VolumeDeleted = "deleted"

	extVolState     = "swb.volume.state"
	extVolDeletedNs = "swb.volume.deleted_ns"

	// Test seams (retainHook stages) in a DeleteVolume.
	hookHeadMarked    = "head-marked"    // after (b)
	hookClaimFound    = "claim-found"    // (c) found a live claim, before the revert
	hookClaimsChecked = "claims-checked" // after (c), before the commit point
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
	AdmissionUncertain bool     `json:"admissionUncertain"`
	AdmissionAborted   bool     `json:"admissionAborted"`
	Committed          bool     `json:"committed"`
	HeadMarked         bool     `json:"headMarked"` // HEAD is deleting or deleted
	Reverted           bool     `json:"reverted"`   // (c) gave the HEAD mark back
	SnapshotsDeleted   []string `json:"snapshotsDeleted"`
	AttemptsAborted    []string `json:"attemptsAborted"` // unpublished attempts tombstoned
	ClaimReleased      bool     `json:"claimReleased"`   // its fork claim was confirmed removed
	Completed          bool     `json:"completed"`       // HEAD and VOLUME are deleted
	// Uncertain is set when the failing call was a write whose outcome is
	// unknown; a rerun settles it.
	Uncertain bool `json:"uncertain"`
}

// DeleteVolume deletes vol (see the header). admit may be nil (a catalog-
// only delete, e.g. in tests): (b) still refuses a held HEAD.
func (c *Catalog) DeleteVolume(ctx context.Context, vol string, admit Admission) (VolumeDeleteResult, error) {
	res := VolumeDeleteResult{VolumeID: vol, SnapshotsDeleted: []string{}, AttemptsAborted: []string{}}
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
	h, _, err := c.ReadHead(ctx, vol)
	if err != nil {
		return res, err
	}
	res.Replayed = v.State == VolumeDeleted && h.State == storage.ChunkHeadDeleted
	// (a) Every call takes its own admission (a rerun too: it settles a
	// commit an earlier run may have lost).
	if admit != nil {
		if err := admit.Admit(ctx, vol); err != nil {
			if errors.Is(err, ErrNotAdmitted) || true { // RED: any admission error reads as a refusal
				return res, catErr(CodeVolumeInUse, false, err, "volume %s: delete not admitted", vol)
			}
			res.AdmissionUncertain, res.Uncertain = true, true
			return res, catErr(CodeOutcomeUnknown, true, err, "volume %s: delete admission outcome unknown (it may be recorded); rerun the delete", vol)
		}
	}
	res.Admitted = true
	giveBack := func(cause error) (VolumeDeleteResult, error) {
		if red := true; red { // RED: a give-back leaves the admission in place
			return res, cause
		}
		if admit != nil {
			if err := admit.Abort(ctx, vol); err != nil {
				res.Uncertain = true
				return res, catErr(CodeOutcomeUnknown, true, err, "volume %s: %v; giving the admission back failed, rerun the delete", vol, cause)
			}
			res.AdmissionAborted = true
		}
		return res, cause
	}
	// (b)
	if h.State != storage.ChunkHeadPurging && h.State != storage.ChunkHeadDeleted {
		u, err := c.markHeadDeleting(ctx, vol, root)
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
		live, err := c.anyLiveClaim(ctx, vol)
		if err != nil {
			return res, err
		}
		if live != nil {
			c.hook(hookClaimFound, vol)
			reverted, u, rerr := c.revertHeadDeleting(ctx, vol, root)
			switch {
			case rerr != nil:
				res.Uncertain = u
				return res, rerr
			case !reverted && false: // RED: a revert that found the HEAD committed still counts as given back
				// Another delete committed meanwhile: its irreversible steps
				// stand; this call neither reverts nor gives back.
				return res, catErr(CodeOutcomeUnknown, true, nil, "volume %s: another delete committed while this one found %v; rerun the delete", vol, live)
			}
			res.Reverted, res.HeadMarked = true, false
			return giveBack(live)
		}
		c.hook(hookClaimsChecked, vol)
		// (c') the commit point
		committed, u, err := c.commitHeadPurging(ctx, vol, root)
		if err != nil {
			res.Uncertain = u
			return res, err
		}
		if !committed {
			res.HeadMarked = false
			return giveBack(catErr(CodeOutcomeUnknown, true, nil, "volume %s: a concurrent delete gave the mark back; rerun the delete", vol))
		}
	}
	res.HeadMarked = true
	if admit != nil {
		if err := admit.Commit(ctx, vol); err != nil {
			res.Uncertain = true
			return res, catErr(CodeOutcomeUnknown, true, err, "volume %s: committing the admission failed; rerun the delete", vol)
		}
	}
	res.Committed = true
	// (d)
	names, err := c.listNames(ctx, root)
	if err != nil {
		return res, err
	}
	for _, name := range names {
		gone, aborted, u, err := c.deleteNameForVolume(ctx, vol, root, name)
		if err != nil {
			res.Uncertain = u
			return res, err
		}
		if aborted {
			res.AttemptsAborted = append(res.AttemptsAborted, name)
		} else if gone {
			res.SnapshotsDeleted = append(res.SnapshotsDeleted, name)
		}
	}
	if v.Fork != nil {
		req := ForkRequest{Source: v.Fork.Source, Snapshot: v.Fork.Snapshot, Target: vol, RequestID: v.Fork.RequestID}
		if ref, ok, err := c.resolveSnap(ctx, req.Source, req.Snapshot); err != nil {
			return res, err
		} else if ok && ref.entry != nil {
			removed, err := c.removeForkClaim(ctx, req, ref.rec.Attempt)
			if err != nil {
				res.Uncertain = true
				return res, err
			}
			res.ClaimReleased = removed
		}
	}
	// (e)
	if u, err := c.finishHeadDeleted(ctx, vol, root); err != nil {
		res.Uncertain = u
		return res, err
	}
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
	e, err := headEntry(next)
	if err != nil {
		return err
	}
	if cur.Gen == 0 {
		return c.f.CreateEntry(ctx, root, e, true)
	}
	return c.f.UpdateEntry(ctx, root, e, map[string][]byte{
		extGen:   []byte(strconv.FormatUint(cur.Gen, 10)),
		extEpoch: []byte(strconv.FormatUint(cur.Epoch, 10)),
	})
}

func lostCondition(err error) bool {
	return errors.Is(err, ErrPrecondition) || errors.Is(err, ErrExists) || errors.Is(err, ErrNotFound)
}

// markHeadDeleting is (b): every read decides afresh whether the HEAD is
// held, so an attach that landed before this read is refused, and one that
// lands after it makes the write's condition fail.
func (c *Catalog) markHeadDeleting(ctx context.Context, vol, root string) (bool, error) {
	for try := 0; try < 5; try++ {
		h, _, err := c.ReadHead(ctx, vol)
		if err != nil {
			return false, err
		}
		if headDeletingOrDeleted(h) {
			return false, nil
		}
		if headHeld(h) {
			return false, catErr(CodeVolumeInUse, false, nil, "volume %s is attached: HEAD is held by %q at epoch %d; detach it first", vol, h.Holder, h.Epoch)
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
			return true, catErr(CodeOutcomeUnknown, true, err, "mark HEAD of %s deleting: outcome unknown; rerun the delete", vol)
		}
		return false, nil
	}
	return false, remoteErr(nil, "HEAD of %s kept changing", vol)
}

// revertHeadDeleting gives (b) back: deleting returns to detached when the
// HEAD carries its holder's detach record, else to active. reverted is
// false when the HEAD is no longer deleting (another delete committed, or
// already gave it back).
func (c *Catalog) revertHeadDeleting(ctx context.Context, vol, root string) (reverted, uncertain bool, err error) {
	for try := 0; try < 5; try++ {
		h, _, err := c.ReadHead(ctx, vol)
		if err != nil {
			return false, false, err
		}
		if h.State != storage.ChunkHeadDeleting {
			return h.State == storage.ChunkHeadActive || h.State == storage.ChunkHeadDetached || h.State == "", false, nil
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
			continue
		}
		if err != nil {
			return false, true, catErr(CodeOutcomeUnknown, true, err, "revert the delete mark on HEAD of %s: outcome unknown; rerun the delete", vol)
		}
		return true, false, nil
	}
	return false, false, remoteErr(nil, "HEAD of %s kept changing", vol)
}

// commitHeadPurging is the commit point: deleting -> purging, conditioned
// on the HEAD read. committed is false when the mark was given back (the
// HEAD is active or detached again).
func (c *Catalog) commitHeadPurging(ctx context.Context, vol, root string) (committed, uncertain bool, err error) {
	for try := 0; try < 5; try++ {
		h, _, err := c.ReadHead(ctx, vol)
		if err != nil {
			return false, false, err
		}
		switch h.State {
		case storage.ChunkHeadPurging, storage.ChunkHeadDeleted:
			return true, false, nil // committed (by this call's earlier try or another delete)
		case storage.ChunkHeadDeleting:
		default:
			return false, false, nil
		}
		next := h
		next.State = storage.ChunkHeadPurging
		err = c.writeHead(ctx, root, h, next)
		if lostCondition(err) {
			continue
		}
		if err != nil {
			return false, true, catErr(CodeOutcomeUnknown, true, err, "commit the delete of %s: outcome unknown; rerun the delete", vol)
		}
		return true, false, nil
	}
	return false, false, remoteErr(nil, "HEAD of %s kept changing", vol)
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

// anyLiveClaim returns snapshot_claimed for the first published snapshot of
// vol a live fork claims, or nil.
func (c *Catalog) anyLiveClaim(ctx context.Context, vol string) (error, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return nil, err
	}
	names, err := c.listNames(ctx, root)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		ref, ok, err := c.resolveSnap(ctx, vol, name)
		if err != nil {
			return nil, err
		}
		if !ok || ref.entry == nil || ref.info.State != SnapActive {
			continue
		}
		live, err := c.liveClaims(ctx, vol, name, decodeClaims(ref.entry))
		if err != nil {
			return nil, err
		}
		if len(live) > 0 {
			return claimedErr(vol, name, live), nil
		}
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

// deleteNameForVolume is (d) for one name: a published attempt goes through
// markDeleting; an unpublished one gets an aborted tombstone at its path
// (so its late publish can never land); then the name is freed.
func (c *Catalog) deleteNameForVolume(ctx context.Context, vol, root, name string) (gone, aborted, uncertain bool, err error) {
	for try := 0; try < 5; try++ {
		ref, ok, err := c.resolveSnap(ctx, vol, name)
		if err != nil {
			return false, false, false, err
		}
		if !ok {
			return false, false, false, nil // freed meanwhile
		}
		attempt := ref.rec.Attempt
		if ref.entry == nil {
			tomb, err := c.abortedSnapEntry(vol, name, attempt)
			if err != nil {
				return false, false, false, err
			}
			if err := c.f.CreateEntry(ctx, path.Join(root, snapsDir, name), tomb, true); err != nil && !errors.Is(err, ErrExists) {
				return false, false, true, remoteErr(err, "abort the unpublished attempt of %s/%s", vol, name)
			}
			continue // re-read: the tombstone, or the attempt that won
		}
		wasAborted := ref.info.State == SnapAborted
		m, live, u, err := c.markDeleting(ctx, vol, name, attempt)
		if err != nil {
			return false, false, u, err
		}
		if m == markClaimed {
			// A fork claimed after (c); it backs out on the deleting HEAD.
			if try < 4 {
				continue
			}
			return false, false, false, claimedErr(vol, name, live)
		}
		if _, u, err := c.freeName(ctx, root, name, attempt); err != nil {
			return false, false, u, err
		}
		return !wasAborted, wasAborted, false, nil
	}
	return false, false, false, remoteErr(nil, "snapshot %s/%s kept changing", vol, name)
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
