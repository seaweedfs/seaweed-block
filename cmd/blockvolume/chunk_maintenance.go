package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// errNoLayoutHolder: the process has no holder identity for the volume yet
// (it is not attached, or Start has not returned), so it does not clean.
var errNoLayoutHolder = errors.New("blockvolume: no layout holder identity for the volume")

// newChunkMaintenance is the holder's periodic pass over its one volume ref:
// retention, then, when collect is set, the cleaning through the holder's
// own layout remote (remote.Clean with the catalog's removal step), the one
// cleaning entry: a cut or a stop cancels it through the remote's state.
// cleanerOf returns the remote Start established; its Begin refuses unless
// HEAD is still that holder's at its epoch, so a process that lost the
// volume removes nothing. Automatic cleaning stays default-off; retention runs either way.
// layoutCleaner is the holder's layout remote as the maintenance cleans
// through it (chunkstore.LayoutRemote).
type layoutCleaner interface {
	Clean(ctx context.Context, run func(ctx context.Context, me layout.HolderID, session string) error) error
}

func newChunkMaintenance(cat *chunkstore.BCatalog, ref chunkstore.VolumeRef, cleanerOf func(context.Context) (layoutCleaner, error), collect bool, now func() time.Time) func(context.Context, string) error {
	return func(ctx context.Context, vol string) error {
		if cat == nil || vol != ref.Name || ref.ID.IsZero() {
			return fmt.Errorf("%w: maintenance of %q, process holds %q", layout.ErrIdentity, vol, ref.Name)
		}
		if _, err := cat.Retain(ctx, ref, now(), false); err != nil {
			return fmt.Errorf("retain: %w", err)
		}
		if !collect {
			return nil
		}
		remote, err := cleanerOf(ctx)
		if err != nil {
			return err
		}
		var res chunkstore.CleanResult
		var session string
		err = remote.Clean(ctx, func(ctx context.Context, me layout.HolderID, s string) error {
			session = s
			var rerr error
			res, rerr = cat.RemoveUnreachableAll(ctx, ref, me, s)
			return rerr
		})
		log.Printf("blockvolume: maintenance volume=%s clean=%s shards=%d planned=%d removed=%d", vol, session, res.Shards, res.Planned, res.Removed)
		// A cleaning refused because HEAD moved away from this holder is
		// the same fence a cut would see: classified so the provider latches.
		return chunkstore.LayoutErr(err)
	}
}
