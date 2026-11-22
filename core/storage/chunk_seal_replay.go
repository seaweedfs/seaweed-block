package storage

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/holder"
)

// ReadDetachedChunkHead implements INV-WS-SEAL-REPLAY-001. A re-sent seal
// succeeds only on this remote observation, never on a remembered result or
// a generation comparison with a previously observed root. It writes nothing.
// root is the asking local root's nonce: the proof must be that root's (a
// pre-release HEAD without a root still matches; INV-WS-LOCAL-ROOT-IDENTITY-001).
func ReadDetachedChunkHead(ctx context.Context, remote ChunkRemote, epoch uint64, owner, root string) (ChunkHead, error) {
	if remote == nil || epoch == 0 || owner == "" {
		return ChunkHead{}, fmt.Errorf("%w: seal replay needs remote and assigned owner", ErrChunkCaptureRefused)
	}
	head, err := readChunkHead(ctx, remote)
	if err != nil {
		return ChunkHead{}, err
	}
	if head.Epoch != epoch || !holder.Claimable(head.Holder, head.HolderRoot, owner, root) {
		return ChunkHead{}, ErrChunkLayerFenced
	}
	if !detachedProofOf(head, epoch, owner) {
		return ChunkHead{}, fmt.Errorf("%w: HEAD is not this owner's exact detached proof", ErrChunkCASOutcomeUnknown)
	}
	return copyHead(head), nil
}

// detachedProofOf reports whether head is owner's exact detached proof at
// epoch: a detached HEAD whose DetachedBy names its own generation, epoch
// and owner (holder and root). The one sealed-reopen test (V1-SPEC r2.39).
func detachedProofOf(head chunkHead, epoch uint64, owner string) bool {
	proof := head.DetachedBy
	return head.Gen != 0 && chunkHeadIsDetached(head) && head.Epoch == epoch && holder.SameAssignment(head.Holder, owner) &&
		proof != nil && proof.Gen == head.Gen && proof.Epoch == epoch && holder.Same(proof.Holder, proof.HolderRoot, head.Holder, head.HolderRoot)
}

// anyRootOfHolder is headSupersedes' owner rule for a claim.
func anyRootOfHolder(curHolder, _, holderID, _ string) bool {
	return holder.SameAssignment(curHolder, holderID)
}

// rootGenFor is the HolderRootGen a publisher stamps: the generation its CAS
// installs when it has a local root, 0 (no root identity) otherwise.
func rootGenFor(root string, gen uint64) uint64 {
	if root == "" {
		return 0
	}
	return gen
}
