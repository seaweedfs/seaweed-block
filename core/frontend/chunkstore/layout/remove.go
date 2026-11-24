package layout

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrLeaseLost: the remover's own lease is no longer valid; nothing more
// is removed.
var ErrLeaseLost = errors.New("layout: remover's lease is not valid")

// LeaseHeld is the remover's half of the time premise: it returns nil only
// while the caller's own lease is valid. Remove calls it immediately before
// every delete.
type LeaseHeld interface {
	StillHeld(ctx context.Context) error
}

// Pool deletes one object of the volume by its path relative to the volume
// root; deleting an absent object succeeds.
type Pool interface {
	DeleteObject(ctx context.Context, rel string) error
}

// RemoveUnreachable is transition 7 for one pool shard: for the holder
// with clean(session) open at HEAD's epoch, every candidate chunk of shard
// that no root in HEAD (current and every snapshot) names in this
// volume's pool is deleted, each delete preceded by a check of the
// remover's own lease. HEAD is read once, after BeginClean: while clean is
// open no commit can add a root, and a claim that voids the cleaning waits
// for this lease to end, which the check before every delete sees. It
// returns the names it deleted, in index order.
func RemoveUnreachable(ctx context.Context, st HeadStore, r *Reader, pool Pool, lease LeaseHeld, me HolderID, session string, shard uint64, candidates []ChunkName) ([]ChunkName, error) {
	if lease == nil || pool == nil || r == nil {
		return nil, errors.New("layout: removal needs a reader, a pool and a lease check")
	}
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrHeadAbsent
	}
	if !h.heldBy(me) {
		return nil, fmt.Errorf("%w: HEAD epoch %d holder %q", ErrNotHolder, h.Epoch, h.Holder)
	}
	if h.Activity != (Activity{Kind: ActClean, Session: session}) {
		return nil, fmt.Errorf("%w: HEAD has %s(%s), not clean(%s)", ErrNoSession, h.Activity.Kind, h.Activity.Session, session)
	}
	live, err := reachable(ctx, r, h, shard)
	if err != nil {
		return nil, err
	}
	sorted := append([]ChunkName(nil), candidates...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a].Index < sorted[b].Index })
	var removed []ChunkName
	for _, c := range sorted {
		if Shard(c.Index) != shard || live[c] {
			continue
		}
		if err := lease.StillHeld(ctx); err != nil {
			return removed, fmt.Errorf("%w: %v", ErrLeaseLost, err)
		}
		if err := pool.DeleteObject(ctx, c.Path()); err != nil {
			return removed, fmt.Errorf("layout: delete %s: %w", c, err)
		}
		removed = append(removed, c)
	}
	return removed, nil
}

// reachable returns this volume's chunk names in shard that any root in h
// names. A root or segment that cannot be read is an error: nothing is
// removed on a partial view.
func reachable(ctx context.Context, r *Reader, h Head, shard uint64) (map[ChunkName]bool, error) {
	roots := map[RootName]bool{}
	if h.Current != nil {
		roots[*h.Current] = true
	}
	for _, s := range h.Snapshots {
		roots[s.Root] = true
	}
	live := map[ChunkName]bool{}
	for rn := range roots {
		img, err := r.Open(ctx, rn)
		if err != nil {
			return nil, err
		}
		seg, ok, err := img.Segment(ctx, shard)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for _, ref := range seg.Chunks {
			if ref.Owner == r.vol {
				live[ref.Chunk] = true
			}
		}
	}
	return live, nil
}
