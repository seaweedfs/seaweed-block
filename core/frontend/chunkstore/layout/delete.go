package layout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
)

// HEAD states: a live volume, a cleanly stopped one, one being deleted, one deleted.
const (
	StateLive     = ""
	StateDetached = "detached"
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

// DeleteAuthority is the master's side of a volume delete. Reserve durably
// records the volume's delete reservation, after which the master grants
// no line for it, ever (re-running it is a replay). NoLiveLease returns nil
// only when no line of the volume holds a live lease, granted but
// unclaimed lines included. After Reserve, "no live lease" can never
// become false again, so DeleteVolume checks it between its HEAD read and
// the conditional update that marks HEAD deleting: a claim landing in
// between changes HEAD and the update fails.
type DeleteAuthority interface {
	Reserve(ctx context.Context) error
	NoLiveLease(ctx context.Context) error
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
	case StateLive, StateDetached:
		return Head{}, false, fmt.Errorf("layout: HEAD is %q; a delete marks it deleting first", h.State)
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
//
// The authority's no-live-lease check runs after the read and before the
// update, as the claim's lease wait does.
func RunMarkDeleting(ctx context.Context, st HeadStore, auth DeleteAuthority) (Head, error) {
	if auth == nil {
		return Head{}, errors.New("layout: MarkDeleting needs the master's delete authority")
	}
	return runTransition(ctx, st, func(h Head) (Head, bool, error) {
		next, changed, err := h.MarkDeleting()
		if err != nil || !changed {
			return next, changed, err
		}
		if err := auth.NoLiveLease(ctx); err != nil {
			return Head{}, false, fmt.Errorf("%w: %v", ErrHolderActive, err)
		}
		return next, true, nil
	})
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
// the whole re-runnable: the master's delete reservation (no line is
// granted for the volume from then on), then HEAD deleting (refused while
// any line holds a live lease, checked between the HEAD read and its
// conditional update, or while a snapshot has dependents; no holder
// transition, claim or removal applies from here), then this volume released from every source it depends on,
// then every object of its pool, segments and roots deleted, each
// conditional on belonging to this volume (a late delete of this run never
// removes the object of a volume created again under the name), then HEAD
// deleted. A source left out keeps an extra dependent, which over-protects
// and is released by the operator's command.
//
// The objects are listed (list) only after HEAD is deleting, so nothing a
// holder uploaded before the reservation escapes the listing. A re-run on
// a deleted HEAD lists and deletes again: the operator's sweep for a late
// object of a dead holder.
func DeleteVolume(ctx context.Context, st HeadStore, auth DeleteAuthority, id VolumeID, sources []HeadStore, os ObjectStore, list func(context.Context) ([]string, error)) error {
	if auth == nil || os == nil || list == nil {
		return errors.New("layout: delete needs the master's delete authority, an object store and a lister")
	}
	if err := auth.Reserve(ctx); err != nil {
		return fmt.Errorf("layout: delete reservation: %w", err)
	}
	if _, err := RunMarkDeleting(ctx, st, auth); err != nil {
		return err
	}
	for _, src := range sources {
		if _, err := RunReleaseDependent(ctx, src, id); err != nil {
			return err
		}
	}
	objects, err := list(ctx)
	if err != nil {
		return fmt.Errorf("layout: list the volume's objects: %w", err)
	}
	for _, rel := range objects {
		if !isObjectPath(rel) {
			return fmt.Errorf("%w: %q is not a pool, segment or root object; HEAD, VOLUME and the directory stay", ErrMalformed, rel)
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
	_, err = RunMarkDeleted(ctx, st)
	return err
}

// OperatorReleaseDependent is the operator's remove-dependent command: it
// releases the target from the source's dependents only when the target
// exists and is deleted or no longer names the source, read first: its
// HEAD is deleting or deleted; its directory's VOLUME carries another id;
// or its HEAD is live and no root in it (current or snapshot) names a
// chunk of the source. A target without HEAD (never created, or a restore
// that died between its VOLUME and its HEAD) first gets a deleted HEAD
// written exclusively in its never-reused directory, so the restore that
// registered it can never create it later, and is then released. A live
// target that still names the source, and one whose HEAD appeared before
// the tombstone, are refused (ErrTargetLive); the source is not written
// then.
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
	case !hasHead:
		// No HEAD (never created, or a restore that died between its VOLUME
		// and its HEAD): a deleted HEAD written exclusively in the target's
		// never-reused directory makes sure the restore can never create it
		// later; only then is the dependent released. If a HEAD appeared
		// first, the restore got there: refused, a re-run decides.
		tomb := Head{VolumeID: targetID, Activity: Activity{Kind: ActIdle}, State: StateDeleted}
		if err := target.CreateDeletedHead(ctx, tomb); err != nil {
			if errors.Is(err, ErrExists) {
				return Head{}, fmt.Errorf("%w: %s appeared while being tombstoned; run again", ErrTargetLive, r.vol)
			}
			return Head{}, err
		}
	default:
		names, err := namesVolume(ctx, r, th, sh.VolumeID)
		if err != nil {
			return Head{}, err
		}
		if names {
			return Head{}, fmt.Errorf("%w: %s still names chunks of the source", ErrTargetLive, r.vol)
		}
	}
	return RunReleaseDependent(ctx, source, targetID)
}

// namesVolume reports whether any root in h names a chunk owned by owner.
// Every shard of every root is read; an unreadable one is an error, never
// "names nothing".
func namesVolume(ctx context.Context, r *Reader, h Head, owner VolumeID) (bool, error) {
	found := false
	for _, rn := range HeadRoots(h) {
		if err := r.EachRef(ctx, rn, AllShards, func(_ uint64, ref Ref) bool { found = ref.OwnerID == owner; return !found }); err != nil || found {
			return found, err
		}
	}
	return false, nil
}

// isObjectPath reports whether rel names a content-named object of the
// volume (a chunk, a segment or a root): the only entries a volume delete
// removes. HEAD (which stays, deleted), VOLUME and the directory are not.
func isObjectPath(rel string) bool {
	for _, dir := range []string{PoolDir + "/", SegDir + "/", RootDir + "/"} {
		if strings.HasPrefix(rel, dir) && len(rel) > len(dir) && !strings.Contains(rel, "..") {
			return true
		}
	}
	return false
}
