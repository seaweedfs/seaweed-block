package layout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// HEAD states: a live volume, one being deleted, one deleted.
const (
	StateLive     = ""
	StateDeleting = "deleting"
	StateDeleted  = "deleted"
)

var (
	// ErrVolumeDeleting: the volume is being deleted or is deleted; no
	// holder transition and no claim applies.
	ErrVolumeDeleting = errors.New("layout: volume is being deleted")
	// ErrHasDependents: a snapshot still has restore targets naming its
	// chunks; the volume cannot be deleted.
	ErrHasDependents = errors.New("layout: a snapshot has dependents")
	// ErrHolderActive: a transaction is open on the volume.
	ErrHolderActive = errors.New("layout: a holder is acting on the volume")
	// ErrTargetLive: the operator asked to release a dependent whose
	// target still exists and may still name the source's chunks.
	ErrTargetLive = errors.New("layout: the dependent's target is live")
)

// TargetStore is a restore target's HEAD entry as the operator's
// remove-dependent sees it: read, and one exclusive create of a deleted
// HEAD (a tombstone) in the target's never-reused directory; ErrExists if
// a HEAD is already there.
type TargetStore interface {
	ReadHead(ctx context.Context) (h Head, found bool, err error)
	CreateDeletedHead(ctx context.Context, h Head) error
}

// HolderGone is the deletion's premise: it returns nil only when no block
// holds the volume (no line or lease the master grants). DeleteVolume calls
// it immediately before marking HEAD deleting.
type HolderGone interface {
	NoHolder(ctx context.Context) error
}

// MarkDeleting is the first step of a volume delete: live -> deleting,
// refused while a transaction is open or any snapshot has dependents.
// Deleting or deleted again is a replay.
func (h Head) MarkDeleting() (next Head, changed bool, err error) {
	switch h.State {
	case StateDeleting, StateDeleted:
		return h, false, nil
	}
	if h.Activity.Kind != ActIdle {
		return Head{}, false, fmt.Errorf("%w: %s(%s)", ErrHolderActive, h.Activity.Kind, h.Activity.Session)
	}
	for n, s := range h.Snapshots {
		if len(s.Dependents) > 0 {
			return Head{}, false, fmt.Errorf("%w: snapshot %q has %d", ErrHasDependents, n, len(s.Dependents))
		}
	}
	next = h.clone()
	next.State = StateDeleting
	return next, true, nil
}

// MarkDeleted is the last step: deleting -> deleted. Deleted again is a
// replay; a live HEAD is refused (the delete never began).
func (h Head) MarkDeleted() (next Head, changed bool, err error) {
	switch h.State {
	case StateDeleted:
		return h, false, nil
	case StateLive:
		return Head{}, false, fmt.Errorf("layout: HEAD is live; a delete marks it deleting first")
	}
	next = h.clone()
	next.State = StateDeleted
	return next, true, nil
}

// ReleaseDependent removes the target volume id from the dependents of
// every snapshot of this (source) HEAD: the step a target being deleted
// makes on each source. It does not look at the target; the operator's
// command goes through OperatorReleaseDependent, which does. A HEAD with
// no such dependent is a replay.
func (h Head) ReleaseDependent(target VolumeID) (next Head, changed bool, err error) {
	if target.IsZero() {
		return Head{}, false, errors.New("layout: ReleaseDependent needs a target volume id")
	}
	next = h.clone()
	for n, s := range next.Snapshots {
		kept := s.Dependents[:0]
		for _, d := range s.Dependents {
			if !bytes.Equal(d.TargetID[:], target[:]) {
				kept = append(kept, d)
			}
		}
		if len(kept) != len(s.Dependents) {
			s.Dependents = kept
			next.Snapshots[n] = s
			changed = true
		}
	}
	if !changed {
		return h, false, nil
	}
	return next, true, nil
}

// RunMarkDeleting applies MarkDeleting by one conditional update.
func RunMarkDeleting(ctx context.Context, st HeadStore) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.MarkDeleting() })
}

// RunMarkDeleted applies MarkDeleted by one conditional update.
func RunMarkDeleted(ctx context.Context, st HeadStore) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.MarkDeleted() })
}

// RunReleaseDependent applies ReleaseDependent to a source's HEAD by one
// conditional update. Its callers know the target is gone: DeleteVolume
// (the target's HEAD is deleting) and OperatorReleaseDependent (checked).
func RunReleaseDependent(ctx context.Context, source HeadStore, target VolumeID) (Head, error) {
	return runTransition(ctx, source, func(h Head) (Head, bool, error) { return h.ReleaseDependent(target) })
}

// DeleteVolume runs a volume delete in its order, every prefix safe and
// the whole re-runnable: HEAD deleting (refused while a holder acts or a
// snapshot has dependents; no holder transition, claim or removal applies
// from here), then this volume released from every source it depends on,
// then every object of its pool, segments and roots deleted, each
// conditional on belonging to this volume (a late delete of this run never
// removes the object of a volume created again under the name), then HEAD
// deleted. A source left out keeps an extra dependent, which over-protects
// and is released by the operator's command.
func DeleteVolume(ctx context.Context, st HeadStore, gone HolderGone, id VolumeID, sources []HeadStore, os ObjectStore, objects []string) error {
	if gone == nil || os == nil {
		return errors.New("layout: delete needs a holder check and an object store")
	}
	if err := gone.NoHolder(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrHolderActive, err)
	}
	if _, err := RunMarkDeleting(ctx, st); err != nil {
		return err
	}
	for _, src := range sources {
		if _, err := RunReleaseDependent(ctx, src, id); err != nil {
			return err
		}
	}
	for _, rel := range objects {
		if err := os.DeleteOfVolume(ctx, rel, id); err != nil {
			if errors.Is(err, ErrPrecondition) {
				continue // the name holds another volume's object now
			}
			return fmt.Errorf("layout: delete %s: %w", rel, err)
		}
	}
	_, err := RunMarkDeleted(ctx, st)
	return err
}

// OperatorReleaseDependent is the operator's remove-dependent command: it
// releases the target from the source's dependents only when the target
// exists and is deleted or no longer names the source, read first: its
// HEAD is deleting or deleted; its directory's VOLUME carries another id;
// or its HEAD is live and no root in it (current or snapshot) names a
// chunk of the source. A target never created (no VOLUME, no HEAD) first
// gets a deleted HEAD written exclusively in its never-reused directory,
// so the restore that registered it can never create it later, and is
// then released. A live target that still names the source, one being
// created (VOLUME without HEAD), and one whose HEAD appeared before the
// tombstone are refused (ErrTargetLive); the source is not written then.
func OperatorReleaseDependent(ctx context.Context, source HeadStore, target TargetStore, r *Reader, targetID VolumeID) (Head, error) {
	sh, found, err := source.ReadHead(ctx)
	if err != nil {
		return Head{}, err
	}
	if !found {
		return Head{}, ErrHeadAbsent
	}
	id, err := r.VolumeID(ctx, r.vol)
	hasVolume := err == nil
	if err != nil && !errors.Is(err, ErrMissing) {
		return Head{}, err
	}
	th, hasHead, err := target.ReadHead(ctx)
	if err != nil {
		return Head{}, err
	}
	switch {
	case hasHead && (th.State == StateDeleting || th.State == StateDeleted):
	case hasVolume && id != targetID:
	case !hasVolume && !hasHead:
		// Never created: a deleted HEAD written exclusively in the target's
		// never-reused directory makes sure the restore can never create it
		// later; only then is the dependent released. If a HEAD appeared
		// first, the restore got there: refused, re-run decides.
		tomb := Head{VolumeID: targetID, Activity: Activity{Kind: ActIdle}, State: StateDeleted}
		if err := target.CreateDeletedHead(ctx, tomb); err != nil {
			if errors.Is(err, ErrExists) {
				return Head{}, fmt.Errorf("%w: %s appeared while being tombstoned; run again", ErrTargetLive, r.vol)
			}
			return Head{}, err
		}
	case hasVolume && hasHead:
		names, err := namesVolume(ctx, r, th, sh.VolumeID)
		if err != nil {
			return Head{}, err
		}
		if names {
			return Head{}, fmt.Errorf("%w: %s still names chunks of the source", ErrTargetLive, r.vol)
		}
	default:
		return Head{}, fmt.Errorf("%w: %s is being created", ErrTargetLive, r.vol)
	}
	return RunReleaseDependent(ctx, source, targetID)
}

// namesVolume reports whether any root in h names a chunk owned by owner.
// Every shard of every root is read; an unreadable one is an error, never
// "names nothing".
func namesVolume(ctx context.Context, r *Reader, h Head, owner VolumeID) (bool, error) {
	roots := map[RootName]bool{}
	if h.Current != nil {
		roots[*h.Current] = true
	}
	for _, s := range h.Snapshots {
		roots[s.Root] = true
	}
	for rn := range roots {
		img, err := r.Open(ctx, rn)
		if err != nil {
			return false, err
		}
		for shard := range img.root.Segments {
			seg, _, err := img.Segment(ctx, shard)
			if err != nil {
				return false, err
			}
			for _, ref := range seg.Chunks {
				if ref.OwnerID == owner {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
