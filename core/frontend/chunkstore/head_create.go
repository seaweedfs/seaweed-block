package chunkstore

// Every exclusive create of a volume's HEAD where none was read goes through
// createHead (or, for the fork publish, which needs createResolved's
// outcome handling, through headCreateCheck right before its create). An
// absent HEAD is unknown, never empty (INV-GC-HEAD-ABSENT-001): a HEAD
// created over an earlier image's keys would publish a different image and
// let GC reclaim those keys. TestHeadCreate_SourceGuard holds every HEAD
// creator to this file.

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// headCreateKey marks a context whose CreateEntry may create a HEAD:
// recordFiler refuses any other create of an entry named HEAD.
type headCreateKey struct{}

// headCreateCtx is ctx allowed to create a HEAD; only createHead and the
// fork publish (after headCreateCheck) use it.
func headCreateCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, headCreateKey{}, true)
}

// errHeadCreateBypass: a HEAD create that did not go through createHead.
var errHeadCreateBypass = errors.New("chunkstore: a HEAD is created only through createHead (INV-GC-HEAD-ABSENT-001)")

// headCreate is why a HEAD is created where none was read.
type headCreate int

const (
	// headCreateInitial: attach's empty gen-1 HEAD of a new volume.
	headCreateInitial headCreate = iota
	// headCreateFork: a fork's or restore's first HEAD at its target.
	headCreateFork
	// headCreateCAS: ChunkRemote.CASHead from generation 0.
	headCreateCAS
	// headCreateDelete: an explicit volume-delete's deleting HEAD; it
	// releases the keys on purpose and is never reverted to active.
	headCreateDelete
)

// errHeadMissingUnderVolume: a CAS from generation 0 under a VOLUME entry.
var errHeadMissingUnderVolume = errors.New("chunkstore: HEAD absent under a VOLUME entry: unknown, never recreated by a CAS (head_missing)")

// headCreateCheck refuses creating the HEAD at root for why when the absent
// HEAD there is unknown: for an initial HEAD, keys/ holds any binding; for a
// fork's HEAD, keys/ holds a binding no intent at the target names (a fork
// from a legacy snapshot binds its own upgraded segments, under its intent,
// before it publishes); for a CAS, a VOLUME entry exists (keys are not
// checked: there attach always made VOLUME and the initial HEAD first). An
// explicit volume-delete is allowed (createHead checks that its HEAD is
// deleting). blobs reads chunked intents (nil: inline only).
func headCreateCheck(ctx context.Context, f Filer, blobs Blobs, root string, why headCreate) error {
	refuse := func() error {
		return catErr(CodeHeadMissing, false, nil, "volume %s has chunk keys but no HEAD: its image is unknown and is not recreated; restore its HEAD, or delete it with sw-block ops chunk volume-delete", path.Base(root))
	}
	switch why {
	case headCreateInitial:
		keys, err := f.ListEntries(ctx, path.Join(root, keysDir), "", "", 1)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return remoteErr(err, "list the chunk keys of %s", path.Base(root))
		}
		if len(keys) != 0 {
			return refuse()
		}
	case headCreateFork:
		if blobs == nil {
			blobs = nilBlobs{}
		}
		r, err := NewChunkRemote(f, blobs, root)
		if err != nil {
			return err
		}
		named := map[string]bool{}
		intents, err := r.ListIntents(ctx)
		if err != nil {
			return remoteErr(err, "list the intents of %s", path.Base(root))
		}
		for _, in := range intents {
			for _, k := range in.Keys {
				if dir, n, err := r.keyLocation(k); err == nil && dir == r.keyDir() {
					named[n] = true
				}
			}
		}
		after := ""
		for {
			page, err := f.ListEntries(ctx, r.keyDir(), "", after, listPage)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return remoteErr(err, "list the chunk keys of %s", path.Base(root))
			}
			for _, e := range page {
				if !named[e.GetName()] {
					return refuse()
				}
				after = e.GetName()
			}
			if len(page) < listPage {
				break
			}
		}
	case headCreateCAS:
		if _, err := f.LookupEntry(ctx, root, volumeName); err == nil {
			return fmt.Errorf("%w: %w", storage.ErrChunkHeadPrecondition, errHeadMissingUnderVolume)
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
	case headCreateDelete:
	default:
		return fmt.Errorf("chunkstore: unknown HEAD create reason %d", why)
	}
	return nil
}

// createHead checks why (headCreateCheck) and then exclusive-creates the
// HEAD entry e at root.
func createHead(ctx context.Context, f Filer, blobs Blobs, root string, e *filerpb.Entry, why headCreate) error {
	if e.GetName() != headName {
		return fmt.Errorf("chunkstore: createHead given %q", e.GetName())
	}
	if why == headCreateDelete {
		h, err := decodeHeadDoc(path.Join(root, headName), e.GetContent())
		if err != nil || h.State != storage.ChunkHeadDeleting {
			return fmt.Errorf("chunkstore: a volume-delete creates only a deleting HEAD (got %q, %v)", h.State, err)
		}
	}
	if err := headCreateCheck(ctx, f, blobs, root, why); err != nil {
		return err
	}
	return f.CreateEntry(headCreateCtx(ctx), root, e, true)
}
