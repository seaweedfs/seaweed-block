package layout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrNoSnapshot: the named snapshot is not in HEAD.
	ErrNoSnapshot = errors.New("layout: no such snapshot")
	// ErrForeignSource: the snapshot names chunks of another volume, so it
	// cannot be a restore source (dependency depth stays one).
	ErrForeignSource = errors.New("layout: snapshot names another volume's chunks")
)

// DeleteSnapshot is transition 9: the named snapshot is removed, refused
// while it has dependents (ErrHasDependents) or while the volume is being
// deleted. A missing name is a replay.
func (h Head) DeleteSnapshot(name string) (next Head, changed bool, err error) {
	if h.State != StateLive {
		return Head{}, false, fmt.Errorf("%w: HEAD is %s", ErrVolumeDeleting, h.State)
	}
	s, ok := h.Snapshots[name]
	if !ok {
		return h, false, nil
	}
	if len(s.Dependents) > 0 {
		return Head{}, false, fmt.Errorf("%w: snapshot %q has %d", ErrHasDependents, name, len(s.Dependents))
	}
	next = h.clone()
	delete(next.Snapshots, name)
	return next, true, nil
}

// AddDependent is transition 10 on the source's HEAD: target (its name and
// volume id) is recorded as a dependent of the named snapshot, with that
// snapshot's root, which must be root (the one checked to name no foreign
// chunk). A dependent already recorded is a replay.
func (h Head) AddDependent(name string, target Dependent, root RootName) (next Head, changed bool, err error) {
	if h.State != StateLive {
		return Head{}, false, fmt.Errorf("%w: HEAD is %s", ErrVolumeDeleting, h.State)
	}
	if target.TargetID.IsZero() {
		return Head{}, false, errors.New("layout: AddDependent needs the target's volume id")
	}
	s, ok := h.Snapshots[name]
	if !ok {
		return Head{}, false, fmt.Errorf("%w: %q", ErrNoSnapshot, name)
	}
	if s.Root != root {
		return Head{}, false, fmt.Errorf("%w: snapshot %q now has root %s, not the checked %s", ErrNoSnapshot, name, s.Root, root)
	}
	for _, d := range s.Dependents {
		if d.TargetID == target.TargetID {
			return h, false, nil
		}
	}
	next = h.clone()
	s = next.Snapshots[name]
	s.Dependents = append(s.Dependents, Dependent{Target: target.Target, TargetID: target.TargetID, Root: root})
	sort.Slice(s.Dependents, func(a, b int) bool {
		return bytes.Compare(s.Dependents[a].TargetID[:], s.Dependents[b].TargetID[:]) < 0
	})
	next.Snapshots[name] = s
	return next, true, nil
}

// RunDeleteSnapshot applies DeleteSnapshot by one conditional update.
func RunDeleteSnapshot(ctx context.Context, st HeadStore, name string) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.DeleteSnapshot(name) })
}

// RunAddDependent checks that the named snapshot's root names no chunk of
// another volume (read through r, the source's reader), then records the
// dependent by one conditional update conditional on that root.
func RunAddDependent(ctx context.Context, st HeadStore, r *Reader, name string, target Dependent) (Head, error) {
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return Head{}, err
	}
	if !found {
		return Head{}, ErrHeadAbsent
	}
	s, ok := h.Snapshots[name]
	if !ok {
		return Head{}, fmt.Errorf("%w: %q", ErrNoSnapshot, name)
	}
	foreign, err := namesOtherThan(ctx, r, s.Root, h.VolumeID)
	if err != nil {
		return Head{}, err
	}
	if foreign {
		return Head{}, fmt.Errorf("%w: %q", ErrForeignSource, name)
	}
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.AddDependent(name, target, s.Root) })
}

// namesOtherThan reports whether root names a chunk owned by a volume
// other than own; an unreadable segment is an error.
func namesOtherThan(ctx context.Context, r *Reader, rn RootName, own VolumeID) (bool, error) {
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
			if ref.OwnerID != own {
				return true, nil
			}
		}
	}
	return false, nil
}

// ErrPinned: an automatic delete of a snapshot pinned at its time.
var ErrPinned = errors.New("layout: snapshot is pinned")

// RetireSnapshot is transition 9 for the automatic caller (retention): as
// DeleteSnapshot, and also refused for a snapshot pinned at nowNs (class
// pinned, or an expiry not yet passed), checked on the HEAD the update
// replaces.
func (h Head) RetireSnapshot(name string, nowNs int64) (next Head, changed bool, err error) {
	if s, ok := h.Snapshots[name]; ok && s.Meta.PinnedAt(nowNs) {
		return Head{}, false, fmt.Errorf("%w: %q", ErrPinned, name)
	}
	return h.DeleteSnapshot(name)
}

// RunRetireSnapshot applies RetireSnapshot by one conditional update.
func RunRetireSnapshot(ctx context.Context, st HeadStore, name string, nowNs int64) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.RetireSnapshot(name, nowNs) })
}
