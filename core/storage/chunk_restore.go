package storage

import (
	"context"
	"errors"
	"fmt"
)

// ClaimChunkHead transfers only HEAD ownership, never chunk bodies. Epoch is
// supplied by the master's projection, not allocated here. The caller must
// persist lazy-read absence before exposing the local volume for I/O.
// No WALStore, layer-state or provider-map lock is held across remote calls;
// the provider may hold its per-volume attach serialization lock.
func ClaimChunkHead(ctx context.Context, remote ChunkRemote, cfg ChunkLayerConfig) (ChunkHead, error) {
	if remote == nil || cfg.Epoch == 0 || cfg.Holder == "" {
		return ChunkHead{}, errors.New("storage: HEAD claim needs master epoch and holder")
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
	if head.Epoch > cfg.Epoch || (head.Gen != 0 && head.Epoch == cfg.Epoch && head.Holder != cfg.Holder) {
		return ChunkHead{}, ErrChunkLayerFenced
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
	// No HEAD is a new volume, not a restore: there is no old holder to
	// fence. The first content cut keeps exclusive-create semantics.
	if head.Gen == 0 {
		return head, nil
	}
	if head.Gen != 0 && head.Epoch == cfg.Epoch && head.Holder == cfg.Holder && !chunkHeadIsDetached(head) && head.DetachedBy == nil {
		return head, allowed() // idempotent restart or settled prior claim
	}
	next := copyHead(head)
	next.Epoch, next.Holder = cfg.Epoch, cfg.Holder
	// A new holder's takeover starts serving the same image as active. The
	// previous holder's final-cut proof belongs only to its detached HEAD.
	next.State, next.DetachedBy = chunkHeadActive, nil
	var intents []string
	settled := true // no root RPC sent yet
	defer func() {
		if settled {
			for _, id := range intents {
				_ = remote.DeleteIntent(context.WithoutCancel(ctx), id)
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
	gen, casErr := remote.CASHead(ctx, ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, next)
	if casErr == nil {
		settled = true
		next.Gen = gen
		return next, allowed()
	}
	// Never blindly replay a possibly committed takeover. A re-read must
	// prove the exact proposal; otherwise the volume remains nonoperational.
	current, readErr := remote.GetHead(ctx)
	if readErr != nil {
		return ChunkHead{}, fmt.Errorf("storage: HEAD claim unresolved: %w", errors.Join(casErr, readErr))
	}
	settled = errors.Is(casErr, ErrChunkHeadPrecondition) || current.Gen > head.Gen
	if current.Gen > head.Gen && sameProposal(current, next) {
		return current, allowed()
	}
	return ChunkHead{}, fmt.Errorf("storage: HEAD claim not installed: %w", casErr)
}
