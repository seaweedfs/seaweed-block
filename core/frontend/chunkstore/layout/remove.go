package layout

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// RemoveUnreachable is the cleaning's removal (transition 7') for one pool
// shard: for the holder with its own clean(session) open at HEAD's epoch,
// each candidate chunk that no root in HEAD (current or any snapshot)
// names in this volume's pool is removed in two conditional writes on its
// own entry: MARK, expected the metadata read when the plan was made and
// unmarked, sets the mark to this session; DELETE, expected that mark,
// deletes it. A writer that reuses the object in between (EnsureObject:
// new session, mark cleared) makes the mark or the delete fail its own
// condition, however late it lands; no lease reasoning is needed. A
// candidate already marked by another session, or moved, is left alone.
// Chunks a restore target names stay live here: the source's snapshot
// root names them, and that snapshot stays in HEAD while it has dependents.
// It returns the names it deleted, in index order.
func RemoveUnreachable(ctx context.Context, st HeadStore, r *Reader, os ObjectStore, me HolderID, session string, shard uint64, candidates []ChunkName) ([]ChunkName, error) {
	if os == nil || r == nil {
		return nil, errors.New("layout: removal needs a reader and an object store")
	}
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrHeadAbsent
	}
	if err := h.holderErr(me); err != nil {
		return nil, err
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
		plan, found, err := os.Stat(ctx, c.Path())
		if err != nil {
			return removed, err
		}
		if !found || plan.Mark != "" {
			continue
		}
		if err := os.Mark(ctx, c.Path(), plan, session); err != nil {
			if errors.Is(err, ErrPrecondition) {
				continue // reused or marked since the plan
			}
			return removed, fmt.Errorf("layout: mark %s: %w", c, err)
		}
		if err := os.DeleteMarked(ctx, c.Path(), session); err != nil {
			if errors.Is(err, ErrPrecondition) {
				continue // reused since the mark
			}
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
	own, err := r.VolumeID(ctx, r.vol)
	if err != nil {
		return nil, err
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
			if ref.OwnerID == own {
				live[ref.Chunk] = true
			}
		}
	}
	return live, nil
}
