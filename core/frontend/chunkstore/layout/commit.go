package layout

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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
	// ErrForeignRefAdded: the new manifest holds another volume's chunk at
	// an index where the previous manifest did not hold that same chunk. A
	// restore target keeps or drops its source's references, never gains
	// one, so a dependency is never deeper than one step.
	ErrForeignRefAdded = errors.New("layout: commit adds a reference to another volume's chunk")
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

// addedForeign returns an index of next holding another volume's chunk
// that prev does not hold at that index (the smallest, for a stable
// refusal). Another volume is told by identity, name and id: a volume
// re-created under this name is another volume.
func addedForeign(vol string, id VolumeID, prev, next map[uint64]Ref) (uint64, bool) {
	var found bool
	var at uint64
	for i, r := range next {
		own := r.Owner == vol && r.OwnerID == id
		if !own && prev[i] != r && (!found || i < at) {
			found, at = true, i
		}
	}
	return at, found
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
		if i, ok := addedForeign(vol, h.VolumeID, req.Prev, req.Next); ok {
			return Head{}, false, fmt.Errorf("%w: index %d names %s's chunk", ErrForeignRefAdded, i, req.Next[i].Owner)
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
		if err := allOwnedBySession(ctx, os, rels, req.Session); err != nil {
			return Head{}, false, err
		}
		return h.Commit(me, req.Session, root, req.Position, req.Snapshot, req.Meta, req.State)
	})
}

// commitVerifyParallel bounds the object lookups one commit runs at once.
// A first snapshot of a large volume names one new chunk per 4 MiB (about
// 262k for 1 TiB); checked one round trip at a time that outlasts the cut's
// whole budget, on every retry.
const commitVerifyParallel = 32

// allOwnedBySession checks that every rel exists, is written by session and
// is unmarked, with at most commitVerifyParallel lookups in flight. It is
// all-or-nothing, like a serial check: the first failing rel in rels order
// decides the error (a lookup error before a "not prepared"), so the result
// does not depend on which lookup finished first. Once any lookup fails no
// new one starts: lookups start in rels order, so every rel before a failure
// has already started and is waited for, and the first failure in order is
// still the one reported.
func allOwnedBySession(ctx context.Context, os ObjectStore, rels []string, session string) error {
	type outcome struct {
		done bool
		ok   bool
		err  error
	}
	got := make([]outcome, len(rels))
	sem := make(chan struct{}, commitVerifyParallel)
	var wg sync.WaitGroup
	var failed atomic.Bool
	for i, rel := range rels {
		sem <- struct{}{}
		if failed.Load() {
			<-sem
			break
		}
		wg.Add(1)
		go func(i int, rel string) {
			defer wg.Done()
			defer func() { <-sem }()
			ok, err := ownedBySession(ctx, os, rel, session)
			got[i] = outcome{done: true, ok: ok, err: err}
			if err != nil || !ok {
				failed.Store(true)
			}
		}(i, rel)
	}
	wg.Wait()
	for i, rel := range rels {
		if !got[i].done {
			break // not started: the failure that stopped the starts came earlier
		}
		if got[i].err != nil {
			return fmt.Errorf("layout: look up %s: %w", rel, got[i].err)
		}
		if !got[i].ok {
			return fmt.Errorf("%w: %s (absent, written by another session, or marked)", ErrNotPrepared, rel)
		}
	}
	if failed.Load() {
		return errors.New("layout: commit object check stopped without its failure") // unreachable: refuse, never pass
	}
	return nil
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
