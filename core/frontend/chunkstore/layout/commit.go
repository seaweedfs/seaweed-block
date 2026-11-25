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
	// ErrPositionBehind: a commit's position is below HEAD's. HEAD's position
	// only rises (an equal one names an unchanged image); a lower commit
	// has no legitimate source, so it is fenced, never retried.
	ErrPositionBehind = errors.New("layout: commit position below HEAD's")
	// ErrSnapshotExists: the snapshot name is taken by another root.
	ErrSnapshotExists = errors.New("layout: snapshot name taken")
	// ErrTooManySnapshots: HEAD already holds MaxSnapshots.
	ErrTooManySnapshots = errors.New("layout: snapshot limit reached")
)

// CommitRequest is what a snapshot session commits: the image at
// Position, as a manifest, and optionally a snapshot name for it. Prev is
// the manifest of HEAD's current root that the session computed what is
// new against: empty when HEAD has no current root, and empty also when
// the current root is that of an empty image.
// StateDetached commits the final image of a stop: Position must be the local
// synced position taken after the caller closed its guest-write barrier.
type CommitRequest struct {
	Session  string
	Prev     map[uint64]Ref
	Next     map[uint64]Ref
	Position uint64
	Snapshot string       // "" = no named snapshot
	Meta     SnapshotMeta // the new snapshot's retention facts
	State    string       // StateLive, or StateDetached for the final synced image
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
// snapshot added, activity idle, and state live or detached. Detached is the
// clean-stop proof and refuses every later holder operation at this epoch.
// A live HEAD that already shows this commit
// applied (idle, current = root, position, the snapshot) is a replay:
// unchanged, changed=false.
func (h Head) Commit(me HolderID, session string, root RootName, position uint64, snapshot string, meta SnapshotMeta, state string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: Commit needs a session id")
	}
	if err := h.holderErr(me); err != nil {
		return Head{}, false, err
	}
	if state != StateLive && state != StateDetached {
		return Head{}, false, fmt.Errorf("%w: invalid commit state %q", ErrMalformed, state)
	}
	if h.State == state && h.Activity.Kind == ActIdle && h.Current != nil && *h.Current == root && h.Position == position {
		if s, ok := h.Snapshots[snapshot]; snapshot == "" || ok && s.Root == root && s.Position == position {
			return h, false, nil
		}
	}
	if h.Activity != (Activity{Kind: ActSnap, Session: session}) {
		return Head{}, false, fmt.Errorf("%w: HEAD has %s(%s), not snap(%s)", ErrNoSession, h.Activity.Kind, h.Activity.Session, session)
	}
	// The position is produced here: never one with no room for a next
	// LSN (a HEAD already there keeps it, e.g. to stop).
	if position == ^uint64(0) && position != h.Position {
		return Head{}, false, fmt.Errorf("%w: commit at %d", ErrNoNextPosition, position)
	}
	if position < h.Position {
		return Head{}, false, fmt.Errorf("%w: commit at %d, HEAD at %d", ErrPositionBehind, position, h.Position)
	}
	next = h.clone()
	if snapshot != "" {
		if s, ok := next.Snapshots[snapshot]; ok {
			// the same name at another root or another position is another
			// commit's snapshot, never this one's replay
			if s.Root != root || s.Position != position {
				return Head{}, false, fmt.Errorf("%w: %q", ErrSnapshotExists, snapshot)
			}
		} else {
			if len(next.Snapshots) >= MaxSnapshots {
				return Head{}, false, fmt.Errorf("%w: at most %d per volume; delete a snapshot, or let retention prune", ErrTooManySnapshots, MaxSnapshots)
			}
			if next.Snapshots == nil {
				next.Snapshots = map[string]Snapshot{}
			}
			if len(snapshot) > MaxSnapshotNameBytes {
				return Head{}, false, fmt.Errorf("%w: snapshot name %q is %d bytes, at most %d", ErrMalformed, snapshot, len(snapshot), MaxSnapshotNameBytes)
			}
			if err := meta.check(snapshot); err != nil {
				return Head{}, false, err
			}
			next.Snapshots[snapshot] = Snapshot{Root: root, Position: position, Meta: meta.clone()}
		}
	}
	rn := root
	next.Current = &rn
	next.Position = position
	next.Activity = Activity{Kind: ActIdle}
	next.State = state
	return next, true, nil
}

// RunCommit checks that every object the new root reaches is written by
// this session and unmarked (the new chunks of this volume against HEAD's
// current root, every segment, the root: see EnsureObject), then commits
// by one conditional update. An object present but written by another
// session, or marked for removal, is not prepared: EnsureObject reuses it
// first. Prev must build to HEAD's current root; a re-run after a lost
// reply finds a live commit applied and writes nothing. For a stop, the caller
// reads HEAD after an unknown outcome: detached at its claim means stopped;
// it never resumes serving without that read.
// model: SnapshotRemoval.Commit
func RunCommit(ctx context.Context, st HeadStore, os ObjectStore, vol string, me HolderID, req CommitRequest) (Head, error) {
	root, segs, err := rootOf(req.Next)
	if err != nil {
		return Head{}, err
	}
	return runTransition(ctx, st, func(h Head) (Head, bool, error) {
		if next, changed, err := h.Commit(me, req.Session, root, req.Position, req.Snapshot, req.Meta, req.State); err != nil || !changed {
			return next, changed, err // refused, or already applied: no lookups
		}
		// The base is HEAD's current root. Before any commit there is none
		// and the previous manifest must be empty; after one (even of an
		// empty image, which is a real root) the previous manifest must
		// build to it.
		if h.Current == nil {
			if len(req.Prev) > 0 {
				return Head{}, false, ErrStaleBase
			}
		} else {
			base, _, err := rootOf(req.Prev)
			if err != nil {
				return Head{}, false, err
			}
			if base != *h.Current {
				return Head{}, false, ErrStaleBase
			}
		}
		var rels []string
		chunks := newOwn(vol, req.Prev, req.Next)
		idx := make([]uint64, 0, len(chunks))
		for i := range chunks {
			idx = append(idx, i)
		}
		sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
		for _, i := range idx {
			rels = append(rels, chunks[i].Chunk.Path())
		}
		names := make([]SegName, 0, len(segs))
		for n := range segs {
			names = append(names, n)
		}
		sort.Slice(names, func(a, b int) bool { return names[a].Shard < names[b].Shard })
		for _, n := range names {
			rels = append(rels, n.Path())
		}
		rels = append(rels, root.Path())
		for _, rel := range rels {
			ok, err := ownedBySession(ctx, os, rel, req.Session)
			if err != nil {
				return Head{}, false, fmt.Errorf("layout: look up %s: %w", rel, err)
			}
			if !ok {
				return Head{}, false, fmt.Errorf("%w: %s (absent, written by another session, or marked)", ErrNotPrepared, rel)
			}
		}
		return h.Commit(me, req.Session, root, req.Position, req.Snapshot, req.Meta, req.State)
	})
}

func (m SnapshotMeta) clone() SnapshotMeta {
	if m.Labels != nil {
		l := make(map[string]string, len(m.Labels))
		for k, v := range m.Labels {
			l[k] = v
		}
		m.Labels = l
	}
	return m
}
