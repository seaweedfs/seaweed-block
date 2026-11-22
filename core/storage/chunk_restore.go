package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/holder"
)

// ClaimChunkHead transfers only HEAD ownership, never chunk bodies. Epoch is
// supplied by the master's projection, not allocated here. The caller must
// persist lazy-read absence before exposing the local volume for I/O.
// No WALStore, layer-state or provider-map lock is held across remote calls;
// the provider may hold its per-volume attach serialization lock.
// On a proven owner fence, the returned HEAD is the exact observed proof;
// it is not an installed claim and must never be used to serve I/O.
func ClaimChunkHead(ctx context.Context, remote ChunkRemote, cfg ChunkLayerConfig) (ChunkHead, error) {
	if remote == nil || cfg.Epoch == 0 || cfg.Holder == "" {
		return ChunkHead{}, errors.New("storage: HEAD claim needs master epoch and holder")
	}
	if err := validateHeadCoolingWindow(cfg.HeadCoolingWindow); err != nil {
		return ChunkHead{}, err
	}
	allowed := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cfg.CaptureAllowed != nil {
			return cfg.CaptureAllowed()
		}
		return nil
	}
	if err := allowed(); err != nil {
		return ChunkHead{}, err
	}
	head, err := remote.GetHead(ctx)
	if err != nil {
		return ChunkHead{}, err
	}
	// Any root of this holder may claim (a fresh root takes over from HEAD);
	// an existing root's lineage is checked by PrepareRestore
	// (INV-WS-LOCAL-ROOT-IDENTITY-001).
	if headSupersedes(head, cfg.Epoch, cfg.Holder, cfg.HolderRoot, anyRootOfHolder) {
		return copyHead(head), ErrChunkLayerFenced
	}
	// INV-WS-SEALED-REOPEN-001: the sealing incarnation cannot reopen its
	// committed handoff proof. A higher-epoch takeover remains a separate claim.
	if detachedProofOf(head, cfg.Epoch, cfg.Holder) {
		return copyHead(head), ErrChunkLayerDetached
	}
	// Preparation uses exactly the validated image this CAS will protect.
	// A failing seed cannot fence the old holder before this host is ready.
	if cfg.PrepareRestore != nil {
		if err := cfg.PrepareRestore(copyHead(head)); err != nil {
			return ChunkHead{}, fmt.Errorf("storage: prepare HEAD restore: %w", err)
		}
	}
	if err := allowed(); err != nil {
		return ChunkHead{}, err
	}
	// INV-WS-HEAD-BELT-001: even the same owner must replace the generation
	// before serving. A killed incarnation may have left a CAS in flight;
	// readback alone cannot invalidate that request's original precondition.
	// An empty HEAD also needs an exclusive create before an old first cut.
	next := copyHead(head)
	next.Epoch, next.Holder, next.HolderRoot = cfg.Epoch, cfg.Holder, cfg.HolderRoot
	next.HolderRootGen = rootGenFor(cfg.HolderRoot, head.Gen+1)
	// A new holder's takeover starts serving the same image as active. The
	// previous holder's final-cut proof belongs only to its detached HEAD.
	next.State, next.DetachedBy = chunkHeadActive, nil
	var intents []string
	settled := true // no root RPC sent yet
	defer func() {
		if settled {
			for _, id := range intents {
				_ = remote.DeleteIntent(ctx, id)
			}
		}
	}()
	if builder, ok := remote.(ChunkManifestBuilder); ok && next.Segments == nil && len(next.Chunks) != 0 {
		if cfg.RestoreChunkBytes == 0 {
			return ChunkHead{}, errors.New("storage: legacy HEAD claim needs validated chunk geometry")
		}
		next, intents, err = builder.PrepareManifest(ctx, head, next, cfg.RestoreChunkBytes)
		if err != nil {
			return ChunkHead{}, err
		}
	}
	if err := allowed(); err != nil {
		return ChunkHead{}, err
	}
	settled = false // an ambiguous CAS keeps its segment intents
	gen, casErr := casChunkHead(ctx, remote, cfg.BeforeHeadCAS, ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, next)
	if casErr == nil {
		next.Gen = gen
		if err := confirmChunkTakeover(ctx, remote, next, cfg.HeadCoolingWindow); err != nil {
			return ChunkHead{}, err
		}
		if err := allowed(); err != nil {
			return ChunkHead{}, err
		}
		settled = true
		return next, nil
	}
	// Never blindly replay a possibly committed takeover. A re-read must
	// prove the exact proposal; otherwise the volume remains nonoperational.
	current, readErr := readChunkHead(ctx, remote)
	if readErr != nil {
		return ChunkHead{}, fmt.Errorf("storage: HEAD claim unresolved: %w", errors.Join(casErr, readErr))
	}
	if current.Gen > head.Gen && sameProposal(current, next) {
		if err := confirmChunkTakeover(ctx, remote, current, cfg.HeadCoolingWindow); err != nil {
			return ChunkHead{}, err
		}
		if err := allowed(); err != nil {
			return ChunkHead{}, err
		}
		settled = true
		return current, nil
	}
	// A foreign publication is a fence, not permission to clean our roots.
	if current.Gen != 0 && (current.Epoch != head.Epoch || !holder.Same(current.Holder, current.HolderRoot, head.Holder, head.HolderRoot)) {
		return copyHead(current), fmt.Errorf("storage: HEAD claim displaced: %w", ErrChunkLayerFenced)
	}
	settled = errors.Is(casErr, ErrChunkHeadPrecondition) || current.Gen > head.Gen
	return ChunkHead{}, fmt.Errorf("storage: HEAD claim not installed: %w", casErr)
}
