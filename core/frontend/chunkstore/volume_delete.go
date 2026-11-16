package chunkstore

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Volume delete (claude05's item, 2026-11-16 02:39Z).
//
// DeleteVolume refuses while the volume is in use: its HEAD is held (an
// active HEAD with a holder: a volume server attached it), or the caller's
// InUse check says so (the blockmaster has it assigned or served). It also
// refuses, before writing anything, while a live fork claims one of its
// snapshots (snapshot_claimed, the claim rule of retention.go).
//
// Otherwise, in this order:
//  1. HEAD becomes a tombstone (state deleted, no chunks), by a conditional
//     write on the HEAD it read (or an exclusive create when there is no
//     HEAD yet): a volume server that attached meanwhile makes it fail and
//     the delete decides again. Its chunks stop being GC roots.
//  2. VOLUME is marked deleted (an extended attribute; the document is
//     kept). The id is never reused: EnsureVolume (attach, create) and Fork
//     refuse a deleted volume, and its VOLUME and HEAD entries stay.
//  3. Every snapshot is marked deleting through markDeleting (the one mark
//     path) and its name freed; GC stops counting it after its grace.
//  4. A fork's claim on its parent snapshot is released.
//
// Reclamation is GC's. A rerun after a partial delete (e.g. a fork claimed
// a snapshot between the check and step 3) resumes from what is there.

const (
	CodeVolumeInUse   = "volume_in_use"
	CodeVolumeDeleted = "volume_deleted"

	// VolumeDeleted is the state of a deleted volume's VOLUME entry.
	VolumeDeleted = "deleted"

	extVolState     = "swb.volume.state"
	extVolDeletedNs = "swb.volume.deleted_ns"
)

// InUseCheck reports whether a volume is in use outside the catalog (e.g.
// assigned or served by the blockmaster), with a reason for the error.
type InUseCheck func(ctx context.Context, vol string) (inUse bool, why string, err error)

// VolumeDeleteResult is what DeleteVolume did.
type VolumeDeleteResult struct {
	VolumeID         string   `json:"volumeID"`
	Replayed         bool     `json:"replayed"` // the volume was already deleted; this call finished what was left
	SnapshotsDeleted []string `json:"snapshotsDeleted"`
	ClaimReleased    bool     `json:"claimReleased"` // its fork claim on the parent snapshot was removed
}

// DeleteVolume deletes vol (see the header). inUse may be nil.
func (c *Catalog) DeleteVolume(ctx context.Context, vol string, inUse InUseCheck) (VolumeDeleteResult, error) {
	res := VolumeDeleteResult{VolumeID: vol, SnapshotsDeleted: []string{}}
	if red := true; red { // RED: deletes nothing
		return res, nil
	}
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
	res.Replayed = v.State == VolumeDeleted
	snaps, err := c.listSnapshots(ctx, vol, true)
	if err != nil {
		return res, err
	}
	if !res.Replayed {
		if err := c.checkVolumeFree(ctx, vol, inUse); err != nil {
			return res, err
		}
		// No mutation while a live fork claims a snapshot.
		for _, s := range snaps {
			if s.State != SnapActive {
				continue
			}
			ref, ok, err := c.resolveSnap(ctx, vol, s.Name)
			if err != nil {
				return res, err
			}
			if !ok || ref.entry == nil {
				continue
			}
			live, err := c.liveClaims(ctx, vol, s.Name, decodeClaims(ref.entry))
			if err != nil {
				return res, err
			}
			if len(live) > 0 {
				return res, claimedErr(vol, s.Name, live)
			}
		}
	}
	if err := c.tombstoneHead(ctx, vol, root, inUse, res.Replayed); err != nil {
		return res, err
	}
	if !res.Replayed {
		if err := c.markVolumeDeleted(ctx, vol, root); err != nil {
			return res, err
		}
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Name < snaps[j].Name })
	for _, s := range snaps {
		m, live, _, err := c.markDeleting(ctx, vol, s.Name, s.attempt)
		if err != nil {
			return res, err
		}
		if m == markClaimed {
			return res, claimedErr(vol, s.Name, live)
		}
		if _, _, err := c.freeName(ctx, root, s.Name, s.attempt); err != nil {
			return res, err
		}
		res.SnapshotsDeleted = append(res.SnapshotsDeleted, s.Name)
	}
	if v.Fork != nil {
		req := ForkRequest{Source: v.Fork.Source, Snapshot: v.Fork.Snapshot, Target: vol, RequestID: v.Fork.RequestID}
		if ref, ok, err := c.resolveSnap(ctx, req.Source, req.Snapshot); err == nil && ok && ref.entry != nil {
			token := forkClaim(vol, v.Fork.RequestID)
			for _, cl := range decodeClaims(ref.entry) {
				if cl == token {
					c.dropForkClaim(ctx, req, ref.rec.Attempt)
					res.ClaimReleased = true
				}
			}
		}
	}
	return res, nil
}

// checkVolumeFree refuses a volume in use: a held HEAD, or inUse says so.
func (c *Catalog) checkVolumeFree(ctx context.Context, vol string, inUse InUseCheck) error {
	h, has, err := c.ReadHead(ctx, vol)
	if err != nil {
		return err
	}
	if has && headHeld(h) {
		return catErr(CodeVolumeInUse, false, nil, "volume %s is attached: HEAD is held by %q at epoch %d; detach it first", vol, h.Holder, h.Epoch)
	}
	if inUse != nil {
		busy, why, err := inUse(ctx, vol)
		if err != nil {
			return remoteErr(err, "check whether volume %s is in use", vol)
		}
		if busy {
			return catErr(CodeVolumeInUse, false, nil, "volume %s is in use: %s", vol, why)
		}
	}
	return nil
}

// headHeld: an active HEAD that names a holder belongs to an attached
// volume server (a clean detach publishes state detached).
func headHeld(h storage.ChunkHead) bool {
	return (h.State == "" || h.State == storage.ChunkHeadActive) && h.Holder != ""
}

// tombstoneHead replaces vol's HEAD with a deleted tombstone, conditioned
// on the HEAD read; a lost condition re-reads and decides again (refusing
// if the volume was attached meanwhile).
func (c *Catalog) tombstoneHead(ctx context.Context, vol, root string, inUse InUseCheck, replay bool) error {
	r, err := NewChunkRemote(c.f, nilBlobs{}, root)
	if err != nil {
		return err
	}
	for try := 0; try < 5; try++ {
		h, err := r.GetHead(ctx)
		if err != nil {
			return remoteErr(err, "read HEAD of %s", vol)
		}
		if h.State == storage.ChunkHeadDeleted {
			return nil
		}
		if try > 0 || replay {
			if err := c.checkVolumeFree(ctx, vol, inUse); err != nil {
				return err
			}
		}
		next := storage.ChunkHead{Epoch: h.Epoch, RemoteLSN: h.RemoteLSN, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadDeleted}
		_, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next)
		if errors.Is(err, storage.ErrChunkHeadPrecondition) {
			continue
		}
		if err != nil {
			return catErr(CodeOutcomeUnknown, true, err, "tombstone HEAD of %s: outcome unknown; rerun the delete", vol)
		}
		return nil
	}
	return remoteErr(nil, "HEAD of %s kept changing", vol)
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

// refuseDeleted is volume_deleted for a deleted volume id.
func refuseDeleted(vol, what string) error {
	return catErr(CodeVolumeDeleted, false, nil, "volume %s was deleted; %s (a deleted id is never reused)", vol, what)
}
