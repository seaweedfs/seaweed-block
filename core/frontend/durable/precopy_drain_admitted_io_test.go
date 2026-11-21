package durable

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// heldReadStore parks each ReadContext call on its LBA until the test releases
// that LBA, so every Read stays an admitted I/O for as long as the test needs.
type heldReadStore struct {
	storage.LogicalStorage
	entered chan uint32
	release map[uint32]chan struct{}
}

func newHeldReadStore(lbas ...uint32) *heldReadStore {
	s := &heldReadStore{entered: make(chan uint32, len(lbas)), release: make(map[uint32]chan struct{}, len(lbas))}
	for _, lba := range lbas {
		s.release[lba] = make(chan struct{})
	}
	return s
}

func (s *heldReadStore) ReadContext(ctx context.Context, lba uint32) ([]byte, error) {
	s.entered <- lba
	select {
	case <-s.release[lba]:
		return make([]byte, 4096), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *heldReadStore) Read(lba uint32) ([]byte, error) {
	return s.ReadContext(context.Background(), lba)
}

func (s *heldReadStore) NextLSN() uint64 { return 1 }

// precopyTestBackend is an operational backend over store with a matching
// healthy projection, so admission depends only on the state under test.
func precopyTestBackend(store storage.LogicalStorage) *StorageBackend {
	id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
	b := &StorageBackend{storage: store, blockSize: 4096, id: id, view: readinessProjection{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, Healthy: true}}
	b.SetOperational(true, "serving")
	return b
}

// startHeldReads starts one Read per LBA and returns once every one of them is
// admitted and parked inside storage. Cleanup releases and joins them.
func startHeldReads(t *testing.T, b *StorageBackend, store *heldReadStore, lbas ...uint32) <-chan error {
	t.Helper()
	done := make(chan error, len(lbas))
	for _, lba := range lbas {
		go func(off int64) {
			_, err := b.Read(context.Background(), off, make([]byte, 4096))
			done <- err
		}(int64(lba) * 4096)
	}
	t.Cleanup(func() {
		for _, ch := range store.release {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
		_ = b.Close()
	})
	for range lbas {
		select {
		case <-store.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("read never reached storage")
		}
	}
	return done
}

// installedPause waits until BeginPrecopyPause has installed its pause and
// returns the pause's drain channel.
func installedPause(t *testing.T, b *StorageBackend) <-chan struct{} {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b.mu.Lock()
		p := b.precopy
		b.mu.Unlock()
		if p != nil {
			return p.drained
		}
		if time.Now().After(deadline) {
			t.Fatal("pre-copy pause was never installed")
		}
		time.Sleep(time.Millisecond)
	}
}

type pauseResult struct {
	resume func()
	err    error
}

func beginPauseAsync(ctx context.Context, b *StorageBackend) <-chan pauseResult {
	out := make(chan pauseResult, 1)
	go func() {
		resume, err := b.BeginPrecopyPause(ctx)
		out <- pauseResult{resume, err}
	}()
	return out
}

// TestPrecopyDrainWaitsForEveryAdmittedIO pins that a pre-copy pause reports
// its drain done only after every I/O admitted before it has finished, not
// when the first of several completes.
func TestPrecopyDrainWaitsForEveryAdmittedIO(t *testing.T) {
	store := newHeldReadStore(0, 1)
	b := precopyTestBackend(store)
	reads := startHeldReads(t, b, store, 0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pause := beginPauseAsync(ctx, b)
	drained := installedPause(t, b)

	close(store.release[0])
	if err := <-reads; err != nil {
		t.Fatalf("first admitted read: %v", err)
	}
	// The first read has returned, so its admission release has run.
	select {
	case <-drained:
		t.Fatal("drain reported done while a second admitted I/O is still in flight")
	default:
	}
	select {
	case r := <-pause:
		t.Fatalf("pause returned while a second admitted I/O is still in flight: err=%v", r.err)
	default:
	}

	close(store.release[1])
	if err := <-reads; err != nil {
		t.Fatalf("second admitted read: %v", err)
	}
	select {
	case r := <-pause:
		if r.err != nil {
			t.Fatalf("drain after the last admitted I/O: %v", r.err)
		}
		r.resume()
	case <-time.After(3 * time.Second):
		t.Fatal("drain not reported done after the last admitted I/O finished")
	}
	if _, err := b.Read(context.Background(), 0, nil); err != nil {
		t.Fatalf("resume did not reopen admission: %v", err)
	}
}
