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

// RemoveUnreachableAll is the cleaning's removal step over every pool shard
// of v, page by page, for a holder whose clean(session) is open on HEAD
// (opened by the holder's layout remote, LayoutRemote.Clean):
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
