package storage

import (
	"context"
	"fmt"
)

// ReadDetachedChunkHead implements INV-WS-SEAL-REPLAY-001. A re-sent seal
// succeeds only on this remote observation, never on a remembered result or
// a generation comparison with a previously observed root. It writes nothing.
func ReadDetachedChunkHead(ctx context.Context, remote ChunkRemote, epoch uint64, holder string) (ChunkHead, error) {
	if remote == nil || epoch == 0 || holder == "" {
		return ChunkHead{}, fmt.Errorf("%w: seal replay needs remote and assigned owner", ErrChunkCaptureRefused)
	}
	head, err := readChunkHead(ctx, remote)
	if err != nil {
		return ChunkHead{}, err
	}
	if head.Epoch != epoch || head.Holder != holder {
		return ChunkHead{}, ErrChunkLayerFenced
	}
	proof := head.DetachedBy
	if head.Gen == 0 || !chunkHeadIsDetached(head) || proof == nil || proof.Gen != head.Gen || proof.Epoch != epoch || proof.Holder != holder {
		return ChunkHead{}, fmt.Errorf("%w: HEAD is not this owner's exact detached proof", ErrChunkCASOutcomeUnknown)
	}
	return copyHead(head), nil
}
