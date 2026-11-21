package durable_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestMaintenanceCloseCancelsAndJoinsWithoutGuestLock(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor:           func(string) (storage.ChunkRemote, error) { return remote, nil },
		ChunkMaintenanceInterval: time.Millisecond,
		ChunkMaintenance: func(ctx context.Context, vol string) error {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			<-release
			return ctx.Err()
		},
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { finish(); _ = p.Close() })
	s := openRecovered(t, p)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not start with cut timer off")
	}
	writeDone := make(chan error, 1)
	writerSettled := make(chan struct{})
	go func() { defer close(writerSettled); _, err := s.Write(1, block(0x51)); writeDone <- err }()
	t.Cleanup(func() {
		finish()
		_ = p.Close()
		select {
		case <-writerSettled:
		case <-time.After(5 * time.Second):
			t.Error("writer cleanup did not settle")
		}
	})
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance held a guest lock")
	}
	closed := make(chan error, 1)
	closeSettled := make(chan struct{})
	go func() { defer close(closeSettled); closed <- p.Close() }()
	t.Cleanup(func() {
		finish()
		select {
		case <-closeSettled:
		case <-time.After(5 * time.Second):
			t.Error("Close cleanup did not settle")
		}
	})
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel maintenance")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close released resources before worker join: %v", err)
	default:
	}
	finish()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join")
	}
}

func TestMaintenanceAuthorityLossCancelsHeldPass(t *testing.T) {
	view := v1View()
	entered, cancelled := make(chan struct{}), make(chan struct{})
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor:           func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
		ChunkMaintenanceInterval: time.Millisecond,
		ChunkMaintenance: func(ctx context.Context, _ string) error {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		},
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	openRecovered(t, p)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no pass")
	}
	view.set(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("lost authority did not cancel held pass")
	}
}

func TestMaintenancePassHasOneDeadline(t *testing.T) {
	entered := make(chan error, 1)
	three := make(chan struct{})
	var completed atomic.Int32
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor:           func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
		ChunkMaintenanceInterval: time.Millisecond, ChunkMaintenanceTimeout: 10 * time.Millisecond,
		ChunkMaintenance: func(ctx context.Context, _ string) error {
			<-ctx.Done()
			select {
			case entered <- ctx.Err():
			default:
			}
			if completed.Add(1) == 3 {
				close(three)
			}
			return ctx.Err()
		},
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = p.Close(); close(done) }()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-entered:
			case <-timer.C:
				t.Error("deadline callback cleanup failed to join")
				return
			}
		}
	})
	openRecovered(t, p)
	// Deliberately leave the one-slot observation full across recurring passes.
	select {
	case <-three:
	case <-time.After(5 * time.Second):
		t.Fatal("recurring callback blocked with delayed receiver")
	}
	select {
	case err := <-entered:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	default:
		t.Fatal("no deadline observation")
	}
}

type pendingMaintenancePut struct{ *storage.MemChunkRemote }

func (r pendingMaintenancePut) Put(ctx context.Context, _ string, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}
func TestCutDeadlineIncludesUploadWithBackgroundCaller(t *testing.T) {
	remote := pendingMaintenancePut{storage.NewMemChunkRemote()}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }, ChunkCutTimeout: 10 * time.Millisecond,
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s := openRecovered(t, p)
	claimed := headHolder(t, remote.MemChunkRemote)
	if _, err = s.Write(1, block(0x48)); err != nil {
		t.Fatal(err)
	}
	_, err = p.CaptureVolume(context.Background(), "v1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("whole cut did not expire: %v", err)
	}
	h, err := remote.GetHead(context.Background())
	if err != nil || h.Gen != claimed.Gen {
		t.Fatalf("expired upload published: %+v %v", h, err)
	}
}

func TestMaintenanceCoalescesWhileOnePassIsHeld(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	var calls atomic.Int32
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor:           func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
		ChunkMaintenanceInterval: time.Millisecond,
		ChunkMaintenance: func(ctx context.Context, _ string) error {
			if calls.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return ctx.Err()
		},
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { finish(); _ = p.Close() })
	openRecovered(t, p)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no pass")
	}
	time.Sleep(30 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("held pass started %d workers", got)
	}
	// Close cancels the held pass before release; there is no queued retry to drain.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("Close dispatched %d elapsed ticks", got)
	}
	finish()
}

func TestMaintenancePeriodicNoopDoesNotChurnHead(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p := schedProvider(t, remote, time.Millisecond)
	s := openRecovered(t, p)
	lsn, err := s.Write(1, block(0x91))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var first storage.ChunkHead
	for {
		first, err = remote.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if first.RemoteLSN >= lsn {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	var before int64
	for before == 0 {
		before = p.DurableStatuses()[0].Chunk.Stats.LastCutNs
		select {
		case <-ctx.Done():
			t.Fatal("first cut did not settle", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	for {
		status := p.DurableStatuses()[0]
		if status.Chunk.Stats.LastCutNs > before {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("no periodic noop", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	after, err := remote.GetHead(ctx)
	if err != nil || after.Gen != first.Gen {
		t.Fatalf("noop churned HEAD: before=%d after=%d err=%v", first.Gen, after.Gen, err)
	}
}

func TestMaintenanceHealthyLineageChangeCancelsHeldPass(t *testing.T) {
	for _, field := range []string{"epoch", "replica", "endpoint"} {
		t.Run(field, func(t *testing.T) {
			view := v1View()
			entered, cancelled := make(chan struct{}), make(chan struct{})
			var enterOnce, cancelOnce sync.Once
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
				ChunkRemoteFor:           func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
				ChunkMaintenanceInterval: time.Millisecond,
				ChunkMaintenance: func(ctx context.Context, _ string) error {
					enterOnce.Do(func() { close(entered) })
					<-ctx.Done()
					cancelOnce.Do(func() { close(cancelled) })
					return ctx.Err()
				},
			}, view)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				done := make(chan struct{})
				go func() { _ = p.Close(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("lineage cleanup did not join")
				}
			})
			openRecovered(t, p)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("no held pass")
			}
			cur := view.Projection()
			switch field {
			case "epoch":
				cur.Epoch++
			case "replica":
				cur.ReplicaID = "r2"
			case "endpoint":
				cur.EndpointVersion++
			}
			view.set(cur)
			select {
			case <-cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("healthy foreign lineage did not cancel pass")
			}
		})
	}
}
