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
)

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
// every snapshot of this (source) HEAD: the step a deleted target makes on
// each source, and the operator's remove-dependent command. A HEAD with no
// such dependent is a replay.
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
// conditional update.
func RunReleaseDependent(ctx context.Context, source HeadStore, target VolumeID) (Head, error) {
	return runTransition(ctx, source, func(h Head) (Head, bool, error) { return h.ReleaseDependent(target) })
}

// DeleteVolume runs a volume delete in its order, every prefix safe and
// the whole re-runnable: HEAD deleting (refused while a holder acts or a
// snapshot has dependents; no holder transition, claim or removal applies
// from here), then this volume released from every source it depends on,
// then every object of its pool, segments and roots deleted, then HEAD
// deleted. A source left out keeps an extra dependent, which over-protects
// and is released by the operator's command.
func DeleteVolume(ctx context.Context, st HeadStore, gone HolderGone, id VolumeID, sources []HeadStore, pool Pool, objects []string) error {
	if gone == nil || pool == nil {
		return errors.New("layout: delete needs a holder check and a pool")
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
		if err := pool.DeleteObject(ctx, rel); err != nil {
			return fmt.Errorf("layout: delete %s: %w", rel, err)
		}
	}
	_, err := RunMarkDeleted(ctx, st)
	return err
}
