package durable

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The public layer counters report completed staging even when its cut times out.
type progressLoopLayer struct {
	storage.ChunkLayer
	staged atomic.Uint64
	calls  atomic.Int32
}

func (l *progressLoopLayer) Fenced() bool { return false }
func (l *progressLoopLayer) Stats() storage.ChunkLayerStats {
	return storage.ChunkLayerStats{StagedChunks: l.staged.Load()}
}
func (l *progressLoopLayer) Cut(context.Context) (storage.ChunkCutResult, error) {
	l.calls.Add(1)
	l.staged.Add(1)
	return storage.ChunkCutResult{}, context.DeadlineExceeded
}

func TestCutLoopProgressRetriesWithoutBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		layer := &progressLoopLayer{}
		p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go p.chunkLoop(ctx, "progress", layer, done, nil, nil)
		defer func() { cancel(); <-done }()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if n := layer.calls.Load(); n < 3 {
			t.Fatalf("only %d attempts despite new staged chunks; progress must retry at the next poll, without backoff or minimum spacing", n)
		}
	})
}

func TestScheduledSnapshotProgressRetriesWithoutBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		layer := &progressLoopLayer{}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		calls := 0
		p := &DurableProvider{cfg: ProviderConfig{ChunkScheduledSnapshot: func(context.Context, string, time.Time, func(context.Context) (storage.ChunkHead, error)) error {
			calls++
			if calls < 3 {
				layer.staged.Add(1)
				return context.DeadlineExceeded
			}
			return nil
		}}}
		slot := &snapshotSlot{s: newSnapshotSchedule("v1", 24*time.Hour, time.Now())}
		slot.s.next = time.Now()
		go func() {
			defer close(done)
			p.chunkSnapshotLoop(ctx, "v1", layer, func() bool { return true }, slot, nil)
		}()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		if calls != 3 {
			t.Fatalf("got %d attempts, want two progress retries then success without advancing the clock", calls)
		}
	})
}

type firstAttemptLayer struct {
	progressLoopLayer
	entered chan struct{}
	release chan struct{}
}

func (l *firstAttemptLayer) Stats() storage.ChunkLayerStats {
	return storage.ChunkLayerStats{DirtyChunks: 1, CutsFailed: uint64(l.calls.Load())}
}
func (l *firstAttemptLayer) Cut(ctx context.Context) (storage.ChunkCutResult, error) {
	select {
	case l.entered <- struct{}{}:
	default:
	}
	select {
	case <-l.release:
	case <-ctx.Done():
	}
	l.calls.Add(1)
	return storage.ChunkCutResult{}, context.DeadlineExceeded
}
func TestDirtyStartCleaningWaitsForFirstCutAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		layer := &firstAttemptLayer{entered: make(chan struct{}, 1), release: make(chan struct{})}
		var cleaned atomic.Int32
		p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond, ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: func(context.Context, string) error { cleaned.Add(1); return nil }}}
		kick := make(chan struct{}, 1)
		kick <- struct{}{}
		go p.chunkBackground(ctx, cancel, "dirty", layer, done, func() bool { return true }, nil, nil, kick)
		defer func() { cancel(); <-done }()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		select {
		case <-layer.entered:
		default:
			t.Fatal("first cut did not start")
		}
		if n := cleaned.Load(); n != 0 {
			t.Fatalf("%d cleaning passes ran before the dirty start's first cut returned", n)
		}
		close(layer.release)
		time.Sleep(2 * chunkSchedulePoll)
		synctest.Wait()
		if cleaned.Load() == 0 {
			t.Fatal("cleaning never ran after the first cut attempt")
		}
	})
}

type yieldingLayer struct {
	progressLoopLayer
	progressAt int32
	absent     int
	err        error
	discarded  atomic.Int32
}

func (l *yieldingLayer) Stats() storage.ChunkLayerStats {
	return storage.ChunkLayerStats{DirtyChunks: 1, StagedChunks: l.staged.Load(), CutsFailed: uint64(l.calls.Load()), AbsentChunks: l.absent}
}
func (l *yieldingLayer) Cut(context.Context) (storage.ChunkCutResult, error) {
	if l.calls.Add(1) == l.progressAt {
		l.staged.Add(1)
	}
	if l.err != nil {
		return storage.ChunkCutResult{}, l.err
	}
	return storage.ChunkCutResult{}, context.DeadlineExceeded
}
func (l *yieldingLayer) DiscardStagedIfUnchanged(staged, seq uint64) bool {
	l.discarded.Add(1)
	return true
}
func TestStalledCutYieldsSessionToCleaning(t *testing.T) {
	for _, progress := range []bool{false, true} {
		t.Run(map[bool]string{false: "K_zero_progress", true: "progress_resets_K"}[progress], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				layer := &yieldingLayer{}
				if progress {
					layer.progressAt = chunkZeroProgressLimit
				}
				var cleaned atomic.Int32
				p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond, ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: func(context.Context, string) error {
					if layer.discarded.Load() == 0 {
						t.Error("cleaning ran with the stalled session still open")
					}
					cleaned.Add(1)
					return nil
				}}}
				kick := make(chan struct{}, 1)
				go p.chunkBackground(ctx, cancel, "stalled", layer, done, func() bool { return true }, nil, nil, kick)
				defer func() { cancel(); <-done }()
				time.Sleep(3500 * time.Millisecond)
				synctest.Wait()
				if progress {
					if layer.discarded.Load() != 0 || cleaned.Load() != 0 {
						t.Fatal("K-1 stalls followed by progress discarded a live session")
					}
				} else if layer.discarded.Load() != 1 || cleaned.Load() != 1 {
					t.Fatalf("discarded=%d cleaning=%d; K stalls must release the session and kick a pass", layer.discarded.Load(), cleaned.Load())
				}
			})
		})
	}
}

func TestDirtyStartWithPeriodicCutsOffWaitsForRequestOrInterval(t *testing.T) {
	for _, request := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			layer := &firstAttemptLayer{entered: make(chan struct{}, 1), release: make(chan struct{})}
			var cleaned atomic.Int32
			p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Second, ChunkMaintenance: func(context.Context, string) error { cleaned.Add(1); return nil }}}
			kick := make(chan struct{}, 1)
			kick <- struct{}{}
			go p.chunkBackground(ctx, cancel, "request", layer, done, func() bool { return true }, nil, nil, kick)
			defer func() { cancel(); <-done }()
			synctest.Wait()
			if request {
				go layer.Cut(ctx)
			}
			time.Sleep(400 * time.Millisecond)
			synctest.Wait()
			if cleaned.Load() != 0 {
				t.Fatal("cleaning ran before any request cut finished or the interval elapsed")
			}
			if request {
				close(layer.release)
			}
			time.Sleep(2 * chunkSchedulePoll)
			synctest.Wait()
			if request && cleaned.Load() != 1 {
				t.Fatal("cleaning did not observe the request cut's completion")
			}
			if !request {
				if cleaned.Load() != 0 {
					t.Fatal("no-attempt fallback ran before the interval")
				}
				time.Sleep(500 * time.Millisecond)
				synctest.Wait()
				if cleaned.Load() != 1 {
					t.Fatal("no-attempt fallback did not run after one interval")
				}
			}
		})
	}
}

func TestUnadmittedAttemptsDoNotDiscardStaging(t *testing.T) {
	for _, skip := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			layer := &yieldingLayer{err: storage.ErrChunkCaptureRefused}
			p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond, ChunkMaintenanceInterval: 30 * 24 * time.Hour, ChunkMaintenance: func(context.Context, string) error { return nil }}}
			var snapshots *snapshotSlot
			wait := 8 * time.Second
			if skip {
				p.cfg.ChunkCaptureInterval = 0
				layer.absent = 1
				wait = 4 * time.Hour
				p.cfg.ChunkScheduledSnapshot = func(context.Context, string, time.Time, func(context.Context) (storage.ChunkHead, error)) error {
					t.Error("restoring snapshot reached store")
					return nil
				}
				snapshots = &snapshotSlot{s: newSnapshotSchedule("skip", 24*time.Hour, time.Now())}
				snapshots.s.next = time.Now()
			}
			go p.chunkBackground(ctx, cancel, "unadmitted", layer, done, func() bool { return true }, snapshots, nil, make(chan struct{}, 1))
			defer func() { cancel(); <-done }()
			time.Sleep(wait)
			synctest.Wait()
			if skip {
				snapshots.mu.Lock()
				n := snapshots.s.skipped[SnapshotSkipRestoring]
				snapshots.mu.Unlock()
				if n < 3 {
					t.Fatal("did not exercise three snapshot skips")
				}
			} else if layer.calls.Load() < 3 {
				t.Fatal("did not exercise three capture refusals")
			}
			if layer.discarded.Load() != 0 {
				t.Fatal("unadmitted attempts discarded an unfinished session")
			}
		})
	}
}
