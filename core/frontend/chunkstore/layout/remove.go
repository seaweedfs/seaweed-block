package layout

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// RemoveUnreachable is the cleaning's removal (transition 7') for one pool
// shard, for the holder with its own clean(session) open at HEAD's epoch,
// in an order that makes its plan atomic with its HEAD view
// (read-validate-write):
//  1. read the tag (writer session, mark) of every candidate;
//  2. only then read HEAD, require it is still this holder's with
//     clean(session), and compute what any root in it (current or
//     snapshot) names in this volume's pool;
//  3. MARK each candidate no root names, expected the tag from step 1;
//  4. DELETE it, expected that mark.
//
// If a successor acted before step 2, HEAD is no longer this holder's and
// nothing is written. If it acted after, every tag from step 1 predates
// its writes: a session that names a chunk no root named at its Begin
// writes that chunk's entry (create, or reuse with a new tag), so the MARK
// or the DELETE fails its condition, however late it lands. No lease
// reasoning is needed. A candidate an earlier session marked and did not
// delete (it died in between) is planned again: its tag carries that
// mark, so the MARK replaces it, and the earlier session's late DELETE,
// conditional on its own mark, fails. Every unreachable name is therefore
// removed by the next completed cleaning. Chunks a restore target names stay live: the source's snapshot
// root names them while it has dependents. It returns the names it
// deleted, in index order.
func RemoveUnreachable(ctx context.Context, st HeadStore, r *Reader, os ObjectStore, me HolderID, session string, shard uint64, candidates []ChunkName) ([]ChunkName, error) {
	if os == nil || r == nil {
		return nil, errors.New("layout: removal needs a reader and an object store")
	}
	sorted := append([]ChunkName(nil), candidates...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a].Index < sorted[b].Index })
	plan := map[ChunkName]ObjectMeta{}
	for _, c := range sorted {
		if Shard(c.Index) != shard {
			continue
		}
		meta, found, err := os.Stat(ctx, c.Path())
		if err != nil {
			return nil, err
		}
		if found {
			plan[c] = meta
		}
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
	var removed []ChunkName
	for _, c := range sorted {
		tag, planned := plan[c]
		if !planned || live[c] {
			continue
		}
		if err := os.Mark(ctx, c.Path(), tag, session); err != nil {
			if errors.Is(err, ErrPrecondition) {
				continue // written since the plan
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
	own, err := r.VolumeID(ctx, r.vol)
	if err != nil {
		return nil, err
	}
	live := map[ChunkName]bool{}
	for _, rn := range HeadRoots(h) {
		if err := r.EachRef(ctx, rn, shard, func(_ uint64, ref Ref) bool {
			if ref.OwnerID == own {
				live[ref.Chunk] = true
			}
			return true
		}); err != nil {
			return nil, err
		}
	}
	return live, nil
}
