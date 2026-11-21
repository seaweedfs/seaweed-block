package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

// putCaptured retries one immutable object, never a whole batch or HEAD CAS.
// A lost binding reply can turn the next attempt into KeyExists: only exact
// bytes read from that binding prove completion; a collision is terminal.
// Keep the point and frozen buffer through transient failures until the cut
// context ends (deadline, Close or window abort), never a fresh point.
func (l *chunkLayerImpl) putCaptured(ctx context.Context, key string, data []byte) error {
	return l.putCapturedWith(ctx, key, data, func(ctx context.Context) error { return l.remote.Put(ctx, key, data) })
}

// putCapturedWith is putCaptured over put, one object's slot in an upload
// round: every retry sends through the same put.
func (l *chunkLayerImpl) putCapturedWith(ctx context.Context, key string, data []byte, put func(context.Context) error) error {
	// The per-object retry deadline also bounds a contextless cut. It retains
	// the same point and buffer through a remote outage, never re-reads live data.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	backoff := 100 * time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(lastErr, err)
		}
		err := put(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
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
			return errors.Join(lastErr, ctx.Err())
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
	uploadWorkers, _, _ := l.cutPayloadWorkers()
	workers := min(uploadWorkers, n)
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
