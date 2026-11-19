package storage

import (
	"context"
	"sync"
)

// uploadBatch owns and joins all Put calls before its caller settles intents.
// No storage/publication lock is acquired by workers. Cancellation stops new
// admission; a remote ignoring cancellation remains joined, never declared done.
func (l *chunkLayerImpl) uploadBatch(ctx context.Context, n int, put func(context.Context, int) error) error {
	l.uploadPending.Store(int64(n))
	defer l.uploadPending.Store(0) // after the join, also on failed batches
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	next := 0
	var first error
	var wg sync.WaitGroup
	workers := min(l.cfg.UploadConcurrency, n)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if first != nil || next == n || ctx.Err() != nil {
					mu.Unlock()
					return
				}
				i := next
				next++
				mu.Unlock()
				if err := put(ctx, i); err != nil {
					mu.Lock()
					if first == nil {
						first = err
						cancel()
					}
					mu.Unlock()
					return
				}
				l.uploadPending.Add(-1)
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
