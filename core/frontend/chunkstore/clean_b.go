package chunkstore

import (
	"context"
	"errors"
	"path"
	"strconv"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// cleanPage is the number of pool names one removal step plans at once
// (a variable only so tests can page with a small one).
var cleanPage uint32 = 1000

// CleanResult is what one cleaning pass removed.
type CleanResult struct {
	Shards  int // pool shards listed
	Planned int // candidate names read
	Removed int // names deleted
}

// Clean is the holder's cleaning of v (transitions 6, 7', 8): it opens
// clean(session) on HEAD, then for every pool shard, page by page, removes
// the names no root in HEAD names (layout.RemoveUnreachable validates HEAD
// after reading each page's tags, so a successor's writes are never
// removed), and closes the cleaning. Only the holder at HEAD's epoch can
// clean: any other caller is refused at Begin and nothing is removed. The
// cleaning is closed on every return after a successful Begin, so a failed
// pass never leaves snapshots blocked; a session that dies before closing
// it is closed by the next pass with the same session id, or replaced by
// the holder's next claim.
func (c *BCatalog) Clean(ctx context.Context, v VolumeRef, me layout.HolderID, session string) (res CleanResult, err error) {
	s, _, err := c.open(v)
	if err != nil {
		return res, err
	}
	if _, err := layout.RunBeginClean(ctx, s, me, session); err != nil {
		return res, err
	}
	defer func() {
		if _, endErr := layout.RunEndClean(context.WithoutCancel(ctx), s, me, session); endErr != nil {
			err = errors.Join(err, endErr)
		}
	}()
	return c.RemoveUnreachableAll(ctx, v, me, session)
}

// RemoveUnreachableAll is the cleaning's removal step over every pool shard
// of v, page by page, for a holder whose clean(session) is open on HEAD
// (opened by Clean, or by the engine's remote between its sessions):
// layout.RemoveUnreachable validates HEAD after reading each page's tags,
// so it removes nothing unless clean(session) is this holder's.
func (c *BCatalog) RemoveUnreachableAll(ctx context.Context, v VolumeRef, me layout.HolderID, session string) (res CleanResult, err error) {
	s, r, err := c.open(v)
	if err != nil {
		return res, err
	}
	shards, err := s.List(ctx, path.Join(c.base, v.Dir(), layout.PoolDir))
	if err != nil {
		return res, err
	}
	for _, name := range shards {
		shard, perr := strconv.ParseUint(name, 10, 64)
		if perr != nil || strconv.FormatUint(shard, 10) != name {
			return res, errors.Join(layout.ErrCorrupt, errors.New("chunkstore: pool shard "+strconv.Quote(name)))
		}
		res.Shards++
		after := ""
		for {
			page, err := s.ListChunkCandidates(ctx, shard, after, cleanPage)
			if err != nil {
				return res, err
			}
			if len(page) == 0 {
				break
			}
			res.Planned += len(page)
			removed, err := layout.RemoveUnreachable(ctx, s, r, s, me, session, shard, page)
			res.Removed += len(removed)
			if err != nil {
				return res, err
			}
			if uint32(len(page)) < cleanPage {
				break
			}
			after = page[len(page)-1].String()
		}
	}
	return res, nil
}
