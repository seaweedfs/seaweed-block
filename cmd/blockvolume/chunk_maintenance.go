package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
// retention, then, when collect is set, the cleaning (chunkstore
// BCatalog.Clean). The cleaning runs only as the holder: holderOf returns
// the identity Start established, and Begin refuses it unless HEAD is still
// that holder's at its epoch, so a process that lost the volume removes
// nothing. Automatic cleaning stays default-off; retention runs either way.
func newChunkMaintenance(cat *chunkstore.BCatalog, ref chunkstore.VolumeRef, holderOf func(context.Context) (layout.HolderID, error), collect bool, now func() time.Time) func(context.Context, string) error {
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
		me, err := holderOf(ctx)
		if err != nil {
			return err
		}
		session, err := cleanSessionID()
		if err != nil {
			return err
		}
		res, err := cat.Clean(ctx, ref, me, session)
		log.Printf("blockvolume: maintenance volume=%s clean=%s shards=%d planned=%d removed=%d", vol, session, res.Shards, res.Planned, res.Removed)
		// A cleaning refused because HEAD moved away from this holder is
		// the same fence a cut would see: classified so the provider latches.
		return chunkstore.LayoutErr(err)
	}
}

// cleanSessionID is a fresh cleaning session id per pass.
func cleanSessionID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "clean-" + hex.EncodeToString(b[:]), nil
}
