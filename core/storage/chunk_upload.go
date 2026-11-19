package storage

import (
	"bytes"
	"context"
	"sync"
	"time"
)

// putCaptured retries one immutable object, never a whole batch or HEAD CAS.
// A lost binding reply can turn the next attempt into KeyExists: only exact
// bytes read from that binding prove completion; a collision is terminal.
func (l *chunkLayerImpl) putCaptured(ctx context.Context, key string, data []byte) error {
	backoff := 100 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := l.remote.Put(ctx, key, data)
		if err == nil {
			return nil
		}
		// Resolve a possibly committed binding before replaying the Put.
		got, readErr := l.remote.Get(ctx, key)
		if readErr == nil {
			if bytes.Equal(got, data) {
				return nil
			}
			return errChunkKeyExists
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(2*time.Second, 2*backoff)
	}
}

// uploadBatch owns and joins all Put calls before its caller settles intents.
// No storage/publication lock is acquired by workers. Cancellation stops new
// admission; a remote ignoring cancellation remains joined, never declared done.
func (l *chunkLayerImpl) uploadBatch(ctx context.Context, n int, put func(context.Context, int) error) error {
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
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
