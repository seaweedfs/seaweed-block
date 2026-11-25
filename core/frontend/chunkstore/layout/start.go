package layout

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrMissingLocalRoot: the holder starts at its own epoch on a fresh
	// local root; it cannot prove it holds what it acknowledged.
	ErrMissingLocalRoot = errors.New("layout: local root missing at the holder's own epoch")
	// ErrStaleLocalRoot: the local root belongs to an earlier claim of this
	// server (a same-host forced move published past it).
	ErrStaleLocalRoot = errors.New("layout: local root is from an earlier claim")
)

// StaleLocalRootCure is the way back a stale-root refusal names: after a
// clean stop the published image holds everything the old directory does.
const StaleLocalRootCure = "another root claimed this volume since this directory's last claim (for example a planned move away and back); after a clean stop the directory holds nothing the published image lacks: clear it and start again on a fresh root"

// MissingLocalRootCure is the way back the refusal names.
const MissingLocalRootCure = "check for a wrong or unmounted data directory; if the local data is gone, a forced move (also to the same host) records the lost range and restarts from the published image"

// LocalRoot is what a starting holder knows of its local data: the claim
// id its local root's marker carries (written before any claim, so it is
// durable before HEAD can name it), whether the root is fresh, and the
// position it holds synced. A root is fresh from its creation until the
// base of its restore from HEAD is durable, across any crash in between:
// reported non-fresh before that, its position below HEAD's would be
// refused as an old copy.
type LocalRoot struct {
	Geometry
	ClaimID  string
	Fresh    bool
	Position uint64 // the position the local root holds synced
}

// Start is the HEAD side of a holder's start on the line it was granted
// (epoch, server), with the claim id of its local root. At HEAD's epoch
// and server, the local root whose claim id HEAD holds continues (its own
// restart, or a claim whose reply was lost); a fresh root with another
// claim id is ErrMissingLocalRoot, an older root with another claim id is
// ErrStaleLocalRoot (fenced); neither writes HEAD. At a higher epoch the
// holder claims with its local claim id (RunClaim, which waits for the old
// lease) only if its root is fresh (it restores from HEAD) or carries
// HEAD's claim id (the continuing lineage); any other root holds data
// older than HEAD's image and is ErrStaleLocalRoot. An older epoch,
// another server at HEAD's epoch, a deleting volume and an absent HEAD
// are refused.
type StartStore interface {
	HeadStore
	ReadVolumeRecord(context.Context) (Volume, bool, error)
}

func Start(ctx context.Context, st StartStore, lease LeaseWait, line HolderID, local LocalRoot) (Head, error) {
	if local.ClaimID == "" {
		return Head{}, errors.New("layout: start needs the local root's claim id")
	}
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return Head{}, err
	}
	if !found {
		return Head{}, ErrHeadAbsent
	}
	v, present, err := st.ReadVolumeRecord(ctx)
	if err != nil {
		return Head{}, err
	}
	if !present {
		return Head{}, ErrMissing
	}
	if v.ID != h.VolumeID {
		return Head{}, ErrIdentity
	}
	if err := v.Geometry.Check(); err != nil {
		return Head{}, err
	}
	if local.Geometry != v.Geometry {
		return Head{}, fmt.Errorf("%w: local %+v differs from VOLUME %+v", ErrGeometryMismatch, local.Geometry, v.Geometry)
	}
	if err := startCheck(h, line, local); err != nil {
		return Head{}, err
	}
	if h.Epoch == line.Epoch && h.Holder == line.Holder && h.State == StateLive {
		return h, nil // HEAD holds this root's claim id: it continues
	}
	// The claim re-checks the local root against the very HEAD it updates:
	// another lineage's claim or a higher published position after the
	// read above refuses, never writes over the newer HEAD.
	me := HolderID{Epoch: line.Epoch, Holder: line.Holder, ClaimID: local.ClaimID}
	return RunClaim(ctx, st, lease, me, func(h Head) error { return startCheck(h, line, local) })
}

// startCheck refuses a local root that cannot start on h: an old copy of
// HEAD's lineage (same claim id, position below HEAD's), at HEAD's epoch
// and server a root without HEAD's claim id, and above HEAD's epoch a
// non-fresh root of another lineage.
func startCheck(h Head, line HolderID, local LocalRoot) error {
	if h.State == StateDetached && line.Epoch == h.Epoch {
		return ErrDetached
	}
	if !local.Fresh && local.ClaimID == h.ClaimID && local.Position < h.Position && line.Epoch >= h.Epoch {
		return fmt.Errorf("%w (%w): the local root holds position %d, HEAD published %d: an old copy of this lineage", ErrStaleLocalRoot, ErrNotHolder, local.Position, h.Position)
	}
	if h.Epoch == line.Epoch && h.Holder == line.Holder && h.State == StateLive {
		switch {
		case h.ClaimID == local.ClaimID:
			return nil
		case local.Fresh:
			return fmt.Errorf("%w at epoch %d: %s", ErrMissingLocalRoot, h.Epoch, MissingLocalRootCure)
		default:
			return fmt.Errorf("%w (%w): HEAD holds claim %q at epoch %d, the local root carries %q: %s", ErrStaleLocalRoot, ErrNotHolder, h.ClaimID, h.Epoch, local.ClaimID, StaleLocalRootCure)
		}
	}
	if line.Epoch > h.Epoch && !local.Fresh && local.ClaimID != h.ClaimID {
		return fmt.Errorf("%w (%w): the local root carries claim %q, HEAD's lineage is %q: %s", ErrStaleLocalRoot, ErrNotHolder, local.ClaimID, h.ClaimID, StaleLocalRootCure)
	}
	return nil
}
