package layout

import (
	"context"
	"errors"
	"fmt"
)

// ErrStaleEpoch: the caller's epoch is below HEAD's; a newer holder has
// claimed the volume.
var ErrStaleEpoch = errors.New("layout: epoch below HEAD's")

// LeaseWait is the time premise of a claim: it returns nil only once the
// lease of every holder below epoch has ended, and an error otherwise. A
// claim calls it immediately before its HEAD update, every time.
type LeaseWait interface {
	OldLeaseEnded(ctx context.Context, epoch uint64) error
}

// Claim is transition 1 as a pure step on HEAD: a holder granted epoch e >
// HEAD's epoch takes HEAD (epoch = e, holder = me, and the claim id that
// identifies its local data) and voids any open session or cleaning
// (activity idle); the image and the snapshots are unchanged. A HEAD
// already claimed at e by this server with this claim id is a replay (a
// lost reply). A HEAD at e held by another server, or by this server with
// another claim id, is ErrNotHolder; one above e is ErrStaleEpoch.
func (h Head) Claim(me HolderID) (next Head, changed bool, err error) {
	if me.Epoch == 0 || me.Holder == "" || me.ClaimID == "" {
		return Head{}, false, errors.New("layout: Claim needs an epoch, a holder and a claim id")
	}
	if h.State != StateLive {
		return Head{}, false, fmt.Errorf("%w: HEAD is %s", ErrVolumeDeleting, h.State)
	}
	switch {
	case me.Epoch < h.Epoch:
		return Head{}, false, fmt.Errorf("%w: HEAD epoch %d, caller %d", ErrStaleEpoch, h.Epoch, me.Epoch)
	case h.heldBy(me):
		return h, false, nil
	case me.Epoch == h.Epoch:
		return Head{}, false, fmt.Errorf("%w: epoch %d is held by %q with claim %q", ErrNotHolder, h.Epoch, h.Holder, h.ClaimID)
	}
	next = h.clone()
	next.Epoch, next.Holder, next.ClaimID = me.Epoch, me.Holder, me.ClaimID
	next.Activity = Activity{Kind: ActIdle}
	return next, true, nil
}

// RunClaim reads HEAD, applies Claim, waits for the old lease's end (the
// check runs after the read and before the update), and commits by one
// conditional update. An absent HEAD refuses.
func RunClaim(ctx context.Context, st HeadStore, lease LeaseWait, me HolderID) (Head, error) {
	if lease == nil {
		return Head{}, errors.New("layout: Claim needs a lease wait")
	}
	return runTransition(ctx, st, func(h Head) (Head, bool, error) {
		next, changed, err := h.Claim(me)
		if err != nil || !changed {
			return next, changed, err
		}
		if err := lease.OldLeaseEnded(ctx, me.Epoch); err != nil {
			return Head{}, false, fmt.Errorf("layout: claim at epoch %d: %w", me.Epoch, err)
		}
		return next, true, nil
	})
}
