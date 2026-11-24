package layout

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// Commit refusals.
var (
	// ErrNotPrepared: an object the new root reaches is not in the store
	// yet (a chunk of this volume, a segment or the root itself).
	ErrNotPrepared = errors.New("layout: commit names an object not written yet")
	// ErrStaleBase: the previous manifest given is not HEAD's current
	// root, so what is new cannot be told.
	ErrStaleBase = errors.New("layout: previous manifest is not HEAD's current root")
	// ErrSnapshotExists: the snapshot name is taken by another root.
	ErrSnapshotExists = errors.New("layout: snapshot name taken")
	// ErrTooManySnapshots: HEAD already holds MaxSnapshots.
	ErrTooManySnapshots = errors.New("layout: snapshot limit reached")
)

// CommitRequest is what a snapshot session commits: the image at
// Position, as a manifest, and optionally a snapshot name for it. Prev is
// the manifest of HEAD's current root (empty if none) that the session
// computed what is new against.
type CommitRequest struct {
	Session  string
	Prev     map[uint64]Ref
	Next     map[uint64]Ref
	Position uint64
	Snapshot string // "" = no named snapshot
}

// rootOf names the root of a manifest.
func rootOf(m map[uint64]Ref) (RootName, map[SegName][]byte, error) {
	root, segs, err := Build(m)
	if err != nil {
		return RootName{}, nil, err
	}
	rn, _, err := NameRoot(root)
	return rn, segs, err
}

// newOwn returns the entries of next held in vol's pool whose name is not
// at the same index of prev: the only chunks a commit must look up (every
// name prev's root names is already in its owner's pool).
func newOwn(vol string, prev, next map[uint64]Ref) map[uint64]Ref {
	out := map[uint64]Ref{}
	for i, r := range next {
		if r.Owner == vol && prev[i] != r {
			out[i] = r
		}
	}
	return out
}

// Commit is transition 4 as a pure step on HEAD: for the holder at HEAD's
// epoch with snap(session) open, current = root, position, the optional
// snapshot added, activity idle. A HEAD that already shows this commit
// applied (idle, current = root, position, the snapshot) is a replay:
// unchanged, changed=false.
func (h Head) Commit(me HolderID, session string, root RootName, position uint64, snapshot string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: Commit needs a session id")
	}
	if !h.heldBy(me) {
		return Head{}, false, fmt.Errorf("%w: HEAD epoch %d holder %q, caller epoch %d holder %q", ErrNotHolder, h.Epoch, h.Holder, me.Epoch, me.Holder)
	}
	if h.Activity.Kind == ActIdle && h.Current != nil && *h.Current == root && h.Position == position {
		if s, ok := h.Snapshots[snapshot]; snapshot == "" || ok && s.Root == root {
			return h, false, nil
		}
	}
	if h.Activity != (Activity{Kind: ActSnap, Session: session}) {
		return Head{}, false, fmt.Errorf("%w: HEAD has %s(%s), not snap(%s)", ErrNoSession, h.Activity.Kind, h.Activity.Session, session)
	}
	next = h.clone()
	if snapshot != "" {
		if s, ok := next.Snapshots[snapshot]; ok {
			if s.Root != root {
				return Head{}, false, fmt.Errorf("%w: %q", ErrSnapshotExists, snapshot)
			}
		} else {
			if len(next.Snapshots) >= MaxSnapshots {
				return Head{}, false, fmt.Errorf("%w: %d", ErrTooManySnapshots, MaxSnapshots)
			}
			if next.Snapshots == nil {
				next.Snapshots = map[string]Snapshot{}
			}
			next.Snapshots[snapshot] = Snapshot{Root: root}
		}
	}
	rn := root
	next.Current = &rn
	next.Position = position
	next.Activity = Activity{Kind: ActIdle}
	return next, true, nil
}

// RunCommit checks that every object the new root reaches is written (the
// new chunks of this volume against HEAD's current root, every segment,
// the root), then commits by one conditional update. Prev must build to
// HEAD's current root; a re-run after a lost reply finds the commit
// applied and writes nothing.
func RunCommit(ctx context.Context, st HeadStore, look Lookup, volRoot, vol string, me HolderID, req CommitRequest) (Head, error) {
	root, segs, err := rootOf(req.Next)
	if err != nil {
		return Head{}, err
	}
	return runTransition(ctx, st, func(h Head) (Head, bool, error) {
		if next, changed, err := h.Commit(me, req.Session, root, req.Position, req.Snapshot); err != nil || !changed {
			return next, changed, err // refused, or already applied: no lookups
		}
		var base *RootName
		if len(req.Prev) > 0 {
			rn, _, err := rootOf(req.Prev)
			if err != nil {
				return Head{}, false, err
			}
			base = &rn
		}
		if (base == nil) != (h.Current == nil) || base != nil && *base != *h.Current {
			return Head{}, false, ErrStaleBase
		}
		missing, err := MissingOwn(ctx, look, volRoot, vol, newOwn(vol, req.Prev, req.Next))
		if err != nil {
			return Head{}, false, err
		}
		if len(missing) > 0 {
			return Head{}, false, fmt.Errorf("%w: %d chunks, first %s", ErrNotPrepared, len(missing), missing[0])
		}
		names := make([]SegName, 0, len(segs))
		for n := range segs {
			names = append(names, n)
		}
		sort.Slice(names, func(a, b int) bool { return names[a].Shard < names[b].Shard })
		for _, n := range names {
			if err := present(ctx, look, volRoot, n.Path()); err != nil {
				return Head{}, false, err
			}
		}
		if err := present(ctx, look, volRoot, root.Path()); err != nil {
			return Head{}, false, err
		}
		return h.Commit(me, req.Session, root, req.Position, req.Snapshot)
	})
}

func present(ctx context.Context, look Lookup, volRoot, rel string) error {
	ok, err := Present(ctx, look, volRoot, rel)
	if err != nil {
		return fmt.Errorf("layout: look up %s: %w", rel, err)
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotPrepared, rel)
	}
	return nil
}
