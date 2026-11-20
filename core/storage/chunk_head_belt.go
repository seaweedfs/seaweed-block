package storage

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Matches the filer lock ring's prior-owner cooling window. This is a
// detection belt, not a substitute for a single HEAD serialization point.
const chunkHeadCoolingWindow = 5 * time.Second

// ChunkHeadRootReader confirms publication identity without resolving every
// immutable manifest. The returned Chunks need not be populated.
type ChunkHeadRootReader interface {
	GetHeadRoot(context.Context) (ChunkHead, error)
}

func confirmChunkHead(ctx context.Context, remote chunkRemote, want chunkHead) error {
	var got chunkHead
	var err error
	if reader, ok := remote.(ChunkHeadRootReader); ok {
		got, err = reader.GetHeadRoot(ctx)
	} else {
		got, err = remote.GetHead(ctx)
	}
	if err != nil {
		return fmt.Errorf("storage: HEAD publication confirmation: %w: %w", errChunkCASOutcomeUnknown, err)
	}
	if got.Gen != want.Gen || got.Epoch != want.Epoch || got.Holder != want.Holder {
		log.Printf("ALARM: HEAD publication displaced: wanted gen=%d epoch=%d holder=%q, read gen=%d epoch=%d holder=%q", want.Gen, want.Epoch, want.Holder, got.Gen, got.Epoch, got.Holder)
		return fmt.Errorf("%w: HEAD publication displaced", errChunkLayerFenced)
	}
	return nil
}

// Every successful CAS is confirmed, including calls near or beyond a lease
// horizon. No lease renewal or guard lock is needed across these remote calls.
func casAndConfirmChunkHead(ctx context.Context, remote chunkRemote, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	gen, err := remote.CASHead(ctx, expected, next)
	if err != nil {
		return gen, err
	}
	next.Gen = gen
	return gen, confirmChunkHead(ctx, remote, next)
}

func confirmChunkTakeover(ctx context.Context, remote chunkRemote, head chunkHead) error {
	timer := time.NewTimer(chunkHeadCoolingWindow)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return confirmChunkHead(ctx, remote, head)
}

// A failed CAS must not retry over a different publication owner, even if a
// delayed write from another serialization point has lowered the remote epoch.
func (l *chunkLayerImpl) publicationOwnerChanged(cur chunkHead) bool {
	l.stMu.Lock()
	last := l.head
	l.stMu.Unlock()
	return l.supersededBy(cur) || (last.Gen != 0 && cur.Gen != 0 && (last.Epoch != cur.Epoch || last.Holder != cur.Holder))
}

func (l *chunkLayerImpl) fencePublication(cur chunkHead) {
	l.stMu.Lock()
	l.fenced = true
	l.stMu.Unlock()
	log.Printf("ALARM: chunk publisher fenced: local epoch=%d holder=%q, observed gen=%d epoch=%d holder=%q; keeping local data and intents", l.cfg.Epoch, l.cfg.Holder, cur.Gen, cur.Epoch, cur.Holder)
	l.abortCOW(errChunkLayerFenced)
}
