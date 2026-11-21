package storage

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

const (
	chunkHeadOperationTimeout = 2 * time.Second
	chunkHeadSettleTime       = 100 * time.Millisecond
)

func validateHeadCoolingWindow(window time.Duration) error {
	if window != 0 && window < chunkHeadOperationTimeout+chunkHeadSettleTime {
		return fmt.Errorf("storage: HEAD cooling window must be zero (single serialization point) or at least %s", chunkHeadOperationTimeout+chunkHeadSettleTime)
	}
	return nil
}

type chunkCASCountKey struct{}

// WithChunkCASAccounting counts actual CAS attempts through final publication
// and its settlement. Reusing its context preserves the same operation scope.
func WithChunkCASAccounting(ctx context.Context) (context.Context, func() uint64) {
	counter, _ := ctx.Value(chunkCASCountKey{}).(*atomic.Uint64)
	if counter == nil {
		counter = &atomic.Uint64{}
		ctx = context.WithValue(ctx, chunkCASCountKey{}, counter)
	}
	return ctx, counter.Load
}

func casChunkHead(ctx context.Context, remote chunkRemote, expected chunkHeadVersion, next chunkHead) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, chunkHeadOperationTimeout)
	defer cancel()
	if counter, _ := ctx.Value(chunkCASCountKey{}).(*atomic.Uint64); counter != nil {
		counter.Add(1)
	}
	return remote.CASHead(ctx, expected, next)
}

func readChunkHead(ctx context.Context, remote chunkRemote) (chunkHead, error) {
	ctx, cancel := context.WithTimeout(ctx, chunkHeadOperationTimeout)
	defer cancel()
	return remote.GetHead(ctx)
}

// ChunkHeadRootReader confirms publication identity without resolving every
// immutable manifest. The returned Chunks need not be populated.
type ChunkHeadRootReader interface {
	GetHeadRoot(context.Context) (ChunkHead, error)
}

func confirmChunkHead(ctx context.Context, remote chunkRemote, want chunkHead) error {
	ctx, cancel := context.WithTimeout(ctx, chunkHeadOperationTimeout)
	defer cancel()
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
	return casAndConfirmChunkHeadMeasured(ctx, remote, expected, next, nil)
}

func casAndConfirmChunkHeadMeasured(ctx context.Context, remote chunkRemote, expected chunkHeadVersion, next chunkHead, st *chunkCutStats) (uint64, error) {
	start := time.Now()
	gen, err := casChunkHead(ctx, remote, expected, next)
	if st != nil {
		st.CASWriteNs += time.Since(start).Nanoseconds()
	}
	if err != nil {
		return gen, err
	}
	next.Gen = gen
	start = time.Now()
	err = confirmChunkHead(ctx, remote, next)
	if st != nil {
		st.HeadReadbackNs += time.Since(start).Nanoseconds()
	}
	return gen, err
}

func confirmChunkTakeover(ctx context.Context, remote chunkRemote, head chunkHead, window time.Duration) error {
	if window == 0 {
		return confirmChunkHead(ctx, remote, head)
	}
	timer := time.NewTimer(window)
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
	if last.Gen == 0 {
		last.Epoch, last.Holder = l.cfg.Epoch, l.cfg.Holder
	}
	l.stMu.Unlock()
	return l.supersededBy(cur) || (cur.Gen != 0 && (last.Epoch != cur.Epoch || last.Holder != cur.Holder))
}

func (l *chunkLayerImpl) fencePublication(cur chunkHead) {
	l.stMu.Lock()
	l.fenced = true
	if cur.Gen != 0 {
		// Preserve the existing hydration identity invalidation. This is not
		// settlement of our cut: remoteLSN and captured-map dirt stay unchanged.
		l.head = copyHead(cur)
	}
	l.stMu.Unlock()
	log.Printf("ALARM: chunk publisher fenced: local epoch=%d holder=%q, observed gen=%d epoch=%d holder=%q; keeping local data and intents", l.cfg.Epoch, l.cfg.Holder, cur.Gen, cur.Epoch, cur.Holder)
	l.abortCOW(errChunkLayerFenced)
}
