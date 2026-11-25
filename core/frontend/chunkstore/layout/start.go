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

// MissingLocalRootCure is the way back the refusal names.
const MissingLocalRootCure = "check for a wrong or unmounted data directory; if the local data is gone, a forced move (also to the same host) records the lost range and restarts from the published image"

// LocalRoot is what a starting holder knows of its local data: the claim
// id its local root's marker carries, and whether the root is fresh (just
// created, holding no data).
type LocalRoot struct {
	ClaimID string
	Fresh   bool
}

// Start is the HEAD side of a holder's start on the line it was granted
// (epoch, server), with the claim id of its local root. At HEAD's epoch
// and server, the local root whose claim id HEAD holds continues (its own
// restart, or a claim whose reply was lost); a fresh root with another
// claim id is ErrMissingLocalRoot, an older root with another claim id is
// ErrStaleLocalRoot (fenced); neither writes HEAD. At a higher epoch the
// holder claims with its local claim id (RunClaim, which waits for the old
// lease). An older epoch, another server at HEAD's epoch, a deleting
// volume and an absent HEAD are refused.
func Start(ctx context.Context, st HeadStore, lease LeaseWait, line HolderID, local LocalRoot) (Head, error) {
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
	me := HolderID{Epoch: line.Epoch, Holder: line.Holder, ClaimID: local.ClaimID}
	if h.Epoch == line.Epoch && h.Holder == line.Holder && h.State == StateLive {
		switch {
		case h.ClaimID == local.ClaimID:
			return h, nil
		case local.Fresh:
			return Head{}, fmt.Errorf("%w at epoch %d: %s", ErrMissingLocalRoot, h.Epoch, MissingLocalRootCure)
		default:
			return Head{}, fmt.Errorf("%w (%w): HEAD holds claim %q at epoch %d, the local root carries %q", ErrStaleLocalRoot, ErrNotHolder, h.ClaimID, h.Epoch, local.ClaimID)
		}
	}
	return RunClaim(ctx, st, lease, me)
}
