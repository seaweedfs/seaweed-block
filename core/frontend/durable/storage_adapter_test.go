// T3a adapter tests — matrix-parameterized per Addendum A #1.
// EVERY test runs against walstore, smartwal.Store, and parallelwal.Store so
// variant skew is caught at L1 instead of prod.
//
// Acceptance-criteria mapping (mini plan §2):
//   #1  TestT3a_StorageBackend_ImplementsBackend
//   #2  TestT3a_StorageBackend_ByteLBATranslation_Matrix
//   #3  TestT3a_StorageBackend_FenceCheck_{Epoch,EV,Replica,Healthy}
//   #4  TestT3a_StorageBackend_OperationalGate_{BeforeOpen,FlipBack,Evidence}
//   #5  TestT3a_StorageBackend_Sync_DispatchesToStorage
//   #6  implicitly: all Matrix tests run N×2 via logicalStorageFactories
//   #7,#8 covered by core/storage superblock tests (not this file)

package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
	"github.com/seaweedfs/seaweed-block/core/storage/parallelwal"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

type writeObserverFunc struct {
	observe func(context.Context, uint32, uint64, []byte) error
	sync    func(context.Context, uint64) error
}

func (f writeObserverFunc) Observe(ctx context.Context, lba uint32, lsn uint64, data []byte) error {
	if f.observe != nil {
		return f.observe(ctx, lba, lsn, data)
	}
	return nil
}

func (f writeObserverFunc) Sync(ctx context.Context, targetLSN uint64) error {
	if f.sync != nil {
		return f.sync(ctx, targetLSN)
	}
	return nil
}

type batchWriteObserverFunc struct {
	writeObserverFunc
	observeBatch func(context.Context, uint32, []uint64, [][]byte) error
}

type partialBatchStorage struct {
	storage.LogicalStorage
	err error
}

func (s partialBatchStorage) WriteBatch(_ uint32, _ [][]byte) ([]uint64, error) {
	return []uint64{1}, s.err
}

func (f batchWriteObserverFunc) ObserveBatch(
	ctx context.Context,
	startLBA uint32,
	lsns []uint64,
	blocks [][]byte,
) error {
	if f.observeBatch != nil {
		return f.observeBatch(ctx, startLBA, lsns, blocks)
	}
	return nil
}

func TestStorageBackend_StrictAckUsesBatchObserver(t *testing.T) {
	b, _, s := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)

	var singleCalls atomic.Int64
	var batchCalls atomic.Int64
	b.SetWriteObserver(batchWriteObserverFunc{
		writeObserverFunc: writeObserverFunc{
			observe: func(context.Context, uint32, uint64, []byte) error {
				singleCalls.Add(1)
				return nil
			},
		},
		observeBatch: func(_ context.Context, startLBA uint32, lsns []uint64, blocks [][]byte) error {
			batchCalls.Add(1)
			if startLBA != 0 || len(lsns) != 2 || len(blocks) != 2 {
				t.Fatalf("batch start=%d lsns=%v blocks=%d", startLBA, lsns, len(blocks))
			}
			if lsns[0]+1 != lsns[1] {
				t.Fatalf("batch LSNs are not contiguous: %v", lsns)
			}
			return nil
		},
	})

	payload := make([]byte, 2*4096)
	payload[0] = 0x41
	payload[4096] = 0x42
	n, err := b.Write(context.Background(), 0, payload)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) {
		t.Fatalf("Write n=%d want %d", n, len(payload))
	}
	if batchCalls.Load() != 1 || singleCalls.Load() != 0 {
		t.Fatalf("batch calls=%d single calls=%d want 1/0", batchCalls.Load(), singleCalls.Load())
	}
	for lba, marker := range []byte{0x41, 0x42} {
		block, readErr := s.Read(uint32(lba))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if block[0] != marker {
			t.Fatalf("LBA %d marker=%x want %x", lba, block[0], marker)
		}
	}
}

func TestStorageBackend_PartialBatchObservesOnlyCommittedPrefix(t *testing.T) {
	_, view, base := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	batchErr := errors.New("partial batch failure")
	s := partialBatchStorage{LogicalStorage: base, err: batchErr}
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	b := durable.NewStorageBackend(s, view, id)
	b.SetOperational(true, "test")
	b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)

	var observedLSNs []uint64
	var observedBlocks int
	b.SetWriteObserver(batchWriteObserverFunc{
		observeBatch: func(_ context.Context, startLBA uint32, lsns []uint64, blocks [][]byte) error {
			if startLBA != 0 {
				t.Fatalf("startLBA=%d want 0", startLBA)
			}
			observedLSNs = append(observedLSNs, lsns...)
			observedBlocks = len(blocks)
			return nil
		},
	})

	payload := make([]byte, 2*4096)
	n, err := b.Write(context.Background(), 0, payload)
	if !errors.Is(err, batchErr) {
		t.Fatalf("error=%v want partial batch failure", err)
	}
	if n != 4096 {
		t.Fatalf("written=%d want committed prefix 4096", n)
	}
	if len(observedLSNs) != 1 || observedLSNs[0] != 1 || observedBlocks != 1 {
		t.Fatalf("observed LSNs=%v blocks=%d want committed prefix [1]/1", observedLSNs, observedBlocks)
	}
}

// ------- Matrix factory (Addendum A #1) -------

// logicalStorageFactory is the shared factory contract; every
// adapter test iterates over the list returned by
// logicalStorageFactories() so adding a new impl later requires
// one edit here, not N edits scattered across test files.
type logicalStorageFactory struct {
	name string
	// make returns a fresh LogicalStorage backed by a tempdir.
	// numBlocks / blockSize are the geometry for the test.
	make func(t *testing.T, numBlocks uint32, blockSize int) storage.LogicalStorage
}

func logicalStorageFactories() []logicalStorageFactory {
	return []logicalStorageFactory{
		{
			name: "walstore",
			make: func(t *testing.T, numBlocks uint32, blockSize int) storage.LogicalStorage {
				t.Helper()
				path := filepath.Join(t.TempDir(), "walstore.bin")
				s, err := storage.CreateWALStore(path, numBlocks, blockSize)
				if err != nil {
					t.Fatalf("CreateWALStore: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
		{
			name: "smartwal",
			make: func(t *testing.T, numBlocks uint32, blockSize int) storage.LogicalStorage {
				t.Helper()
				path := filepath.Join(t.TempDir(), "smartwal.bin")
				s, err := smartwal.CreateStore(path, numBlocks, blockSize)
				if err != nil {
					t.Fatalf("smartwal.CreateStore: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
		{
			name: "parallelwal",
			make: func(t *testing.T, numBlocks uint32, blockSize int) storage.LogicalStorage {
				t.Helper()
				path := filepath.Join(t.TempDir(), "parallelwal.bin")
				s, err := parallelwal.CreateStoreWithConfig(path, parallelwal.Config{
					NumBlocks:    numBlocks,
					BlockSize:    blockSize,
					LaneCount:    4,
					StripeBlocks: 1,
					SlotsPerLane: 128,
					QueueDepth:   64,
				})
				if err != nil {
					t.Fatalf("parallelwal.CreateStoreWithConfig: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
	}
}

type firstReadGateStorage struct {
	storage.LogicalStorage
	reads   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

type firstReadGateBatchStorage struct {
	*firstReadGateStorage
}

func (s *firstReadGateBatchStorage) WriteBatch(startLBA uint32, blocks [][]byte) ([]uint64, error) {
	return s.LogicalStorage.(storage.WriteBatcher).WriteBatch(startLBA, blocks)
}

func (s *firstReadGateStorage) Read(lba uint32) ([]byte, error) {
	if s.reads.Add(1) == 1 {
		close(s.entered)
		<-s.release
	}
	return s.LogicalStorage.Read(lba)
}

func TestStorageBackend_SerializesPartialRMWPerLBA(t *testing.T) {
	base := storage.NewBlockStore(2, 4096)
	gated := &firstReadGateStorage{
		LogicalStorage: base,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := newStubView(healthyProj(id))
	b := durable.NewStorageBackend(gated, view, id)
	b.SetOperational(true, "test")

	results := make(chan error, 2)
	go func() {
		_, err := b.Write(context.Background(), 0, []byte{0x11})
		results <- err
	}()
	<-gated.entered
	go func() {
		_, err := b.Write(context.Background(), 1, []byte{0x22})
		results <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if got := gated.reads.Load(); got != 1 {
		t.Fatalf("partial RMW reads=%d before first write completed; want serialized single read", got)
	}
	close(gated.release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	block, err := base.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if block[0] != 0x11 || block[1] != 0x22 {
		t.Fatalf("partial RMW lost update: first bytes=%02x %02x", block[0], block[1])
	}
}

func TestStorageBackend_SerializesPartialRMWAgainstFullBlockWrite(t *testing.T) {
	base := storage.NewBlockStore(2, 4096)
	gated := &firstReadGateStorage{
		LogicalStorage: base,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	b := durable.NewStorageBackend(gated, newStubView(healthyProj(id)), id)
	b.SetOperational(true, "test")

	partialDone := make(chan error, 1)
	go func() {
		_, err := b.Write(context.Background(), 1, []byte{0x22})
		partialDone <- err
	}()
	<-gated.entered
	fullDone := make(chan error, 1)
	go func() {
		_, err := b.Write(context.Background(), 0, bytes.Repeat([]byte{0x55}, 4096))
		fullDone <- err
	}()
	select {
	case err := <-fullDone:
		t.Fatalf("full-block write bypassed in-flight RMW: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gated.release)
	if err := <-partialDone; err != nil {
		t.Fatal(err)
	}
	if err := <-fullDone; err != nil {
		t.Fatal(err)
	}
	block, err := base.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(block, bytes.Repeat([]byte{0x55}, 4096)) {
		t.Fatal("stale partial RMW overwrote the later full-block write")
	}
}

func TestStorageBackend_SerializesPartialRMWAgainstBatchWrite(t *testing.T) {
	base := memorywal.NewStore(4, 4096)
	gated := &firstReadGateBatchStorage{firstReadGateStorage: &firstReadGateStorage{
		LogicalStorage: base,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}}
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	b := durable.NewStorageBackend(gated, newStubView(healthyProj(id)), id)
	b.SetOperational(true, "test")

	partialDone := make(chan error, 1)
	go func() {
		_, err := b.Write(context.Background(), 1, []byte{0x22})
		partialDone <- err
	}()
	<-gated.entered
	batchDone := make(chan error, 1)
	go func() {
		payload := append(bytes.Repeat([]byte{0x55}, 4096), bytes.Repeat([]byte{0x66}, 4096)...)
		_, err := b.Write(context.Background(), 0, payload)
		batchDone <- err
	}()
	select {
	case err := <-batchDone:
		t.Fatalf("batch write bypassed in-flight RMW: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gated.release)
	if err := <-partialDone; err != nil {
		t.Fatal(err)
	}
	if err := <-batchDone; err != nil {
		t.Fatal(err)
	}
	for lba, want := range []byte{0x55, 0x66} {
		block, err := base.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if block[0] != want {
			t.Fatalf("LBA %d byte=%02x want=%02x", lba, block[0], want)
		}
	}
}

// ------- ProjectionView stub -------

// stubView is a minimal ProjectionView whose state can be flipped
// at runtime to exercise fence-drift behavior.
type stubView struct {
	proj atomic.Value // frontend.Projection
}

func newStubView(p frontend.Projection) *stubView {
	v := &stubView{}
	v.proj.Store(p)
	return v
}

func (v *stubView) Projection() frontend.Projection {
	return v.proj.Load().(frontend.Projection)
}

func (v *stubView) set(p frontend.Projection) { v.proj.Store(p) }

func healthyProj(id frontend.Identity) frontend.Projection {
	return frontend.Projection{
		VolumeID:        id.VolumeID,
		ReplicaID:       id.ReplicaID,
		Epoch:           id.Epoch,
		EndpointVersion: id.EndpointVersion,
		Healthy:         true,
	}
}

// newTestBackend builds a backend at operational=true (to skip
// the gate in tests that exercise I/O). Individual operational-
// gate tests flip it back off.
func newTestBackend(t *testing.T, f logicalStorageFactory, numBlocks uint32, blockSize int) (*durable.StorageBackend, *stubView, storage.LogicalStorage) {
	t.Helper()
	s := f.make(t, numBlocks, blockSize)
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := newStubView(healthyProj(id))
	b := durable.NewStorageBackend(s, view, id)
	b.SetOperational(true, "test")
	return b, view, s
}

// ------- #1 ImplementsBackend -------

func TestT3a_StorageBackend_ImplementsBackend(t *testing.T) {
	// Compile-time check via var _ in the package under test
	// covers the interface conformance; this test proves the
	// zero-value constructor runs + returns a live backend.
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 16, 4096)
			if b.Identity().VolumeID != "v1" {
				t.Errorf("Identity().VolumeID=%q want v1", b.Identity().VolumeID)
			}
			var _ frontend.Backend = b
		})
	}
}

// ------- #2 Byte ↔ LBA translation matrix -------

func TestT3a_StorageBackend_ByteLBATranslation_Matrix(t *testing.T) {
	cases := []struct {
		name      string
		offset    int64
		writeLen  int
		blockSize int
	}{
		{"aligned_full_block", 0, 4096, 4096},
		{"aligned_multi_block", 0, 8192, 4096},
		{"aligned_partial_tail", 0, 100, 4096},
		{"misaligned_offset_partial", 123, 500, 4096},
		{"misaligned_offset_spanning", 4000, 1000, 4096},
		{"spanning_3_blocks", 1000, 8500, 4096},
	}

	for _, f := range logicalStorageFactories() {
		for _, c := range cases {
			f, c := f, c
			t.Run(f.name+"/"+c.name, func(t *testing.T) {
				b, _, _ := newTestBackend(t, f, 8, c.blockSize)

				// Write seed-derived bytes.
				w := make([]byte, c.writeLen)
				for i := range w {
					w[i] = byte((i * 31) & 0xFF)
				}
				n, err := b.Write(context.Background(), c.offset, w)
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
				if n != c.writeLen {
					t.Fatalf("Write n=%d want %d", n, c.writeLen)
				}

				// Read back and verify bit-exact.
				r := make([]byte, c.writeLen)
				nr, err := b.Read(context.Background(), c.offset, r)
				if err != nil {
					t.Fatalf("Read: %v", err)
				}
				if nr != c.writeLen {
					t.Fatalf("Read n=%d want %d", nr, c.writeLen)
				}
				if !bytes.Equal(r, w) {
					t.Fatalf("Read bytes don't match Write (first diff idx=%d)", firstDiffIdx(r, w))
				}

				// Outside the write range must still read zeros —
				// confirms RMW didn't spill.
				if c.offset > 0 {
					prefix := make([]byte, c.offset)
					if _, err := b.Read(context.Background(), 0, prefix); err != nil {
						t.Fatalf("prefix Read: %v", err)
					}
					if !isAllZeros(prefix) {
						t.Fatalf("prefix [0,%d) expected zeros, got non-zero", c.offset)
					}
				}
			})
		}
	}
}

func firstDiffIdx(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

func isAllZeros(p []byte) bool {
	for _, x := range p {
		if x != 0 {
			return false
		}
	}
	return true
}

// ------- #3 Fence check (4 facets) -------

func TestT3a_StorageBackend_FenceCheck_Epoch(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, view, _ := newTestBackend(t, f, 4, 4096)
			// Drift Epoch.
			p := view.Projection()
			p.Epoch = 999
			view.set(p)
			if _, err := b.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("Read after Epoch drift: want ErrStalePrimary, got %v", err)
			}
			if _, err := b.Write(context.Background(), 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("Write after Epoch drift: want ErrStalePrimary, got %v", err)
			}
		})
	}
}

func TestT3a_StorageBackend_FenceCheck_EV(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, view, _ := newTestBackend(t, f, 4, 4096)
			p := view.Projection()
			p.EndpointVersion = 999
			view.set(p)
			if _, err := b.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("want ErrStalePrimary on EV drift, got %v", err)
			}
		})
	}
}

func TestT3a_StorageBackend_FenceCheck_Replica(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, view, _ := newTestBackend(t, f, 4, 4096)
			p := view.Projection()
			p.ReplicaID = "different-replica"
			view.set(p)
			if _, err := b.Write(context.Background(), 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("want ErrStalePrimary on replica drift, got %v", err)
			}
		})
	}
}

func TestT3a_StorageBackend_FenceCheck_Healthy(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, view, _ := newTestBackend(t, f, 4, 4096)
			p := view.Projection()
			p.Healthy = false
			view.set(p)
			if _, err := b.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("want ErrStalePrimary on unhealthy, got %v", err)
			}
		})
	}
}

// ------- #4 Operational gate -------

func TestT3a_StorageBackend_OperationalGate_BeforeOpen(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t, 4, 4096)
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			view := newStubView(healthyProj(id))
			b := durable.NewStorageBackend(s, view, id)
			// DO NOT flip operational — every I/O must be ErrNotReady.

			if _, err := b.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Read before SetOperational(true): want ErrNotReady, got %v", err)
			}
			if _, err := b.Write(context.Background(), 0, []byte("x")); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Write before SetOperational(true): want ErrNotReady, got %v", err)
			}
			if err := b.Sync(context.Background()); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Sync before SetOperational(true): want ErrNotReady, got %v", err)
			}
		})
	}
}

func TestT3a_StorageBackend_OperationalGate_FlipBack(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 4, 4096)
			// I/O works while operational.
			if _, err := b.Write(context.Background(), 0, []byte("hello")); err != nil {
				t.Fatalf("initial Write: %v", err)
			}

			// Flip to non-operational → I/O must reject.
			b.SetOperational(false, "shutting down")
			if _, err := b.Write(context.Background(), 0, []byte("x")); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Write after flip off: want ErrNotReady, got %v", err)
			}

			// Flip back on → I/O succeeds again.
			b.SetOperational(true, "back")
			if _, err := b.Write(context.Background(), 0, []byte("x")); err != nil {
				t.Fatalf("Write after flip-back on: %v", err)
			}
		})
	}
}

func TestT3a_StorageBackend_OperationalGate_Evidence(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t, 4, 4096)
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			b := durable.NewStorageBackend(s, newStubView(healthyProj(id)), id)
			b.SetOperational(false, "local epoch ahead of assignment")

			_, err := b.Read(context.Background(), 0, make([]byte, 4))
			if !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("want ErrNotReady, got %v", err)
			}
			// Evidence should appear in the wrapped error message.
			if !contains(err.Error(), "local epoch ahead of assignment") {
				t.Errorf("err=%q does not include evidence", err)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && anyIndex(s, sub) >= 0)
}

func anyIndex(s, sub string) int {
outer:
	for i := 0; i+len(sub) <= len(s); i++ {
		for j := 0; j < len(sub); j++ {
			if s[i+j] != sub[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}

// ------- #5 Sync dispatches to storage -------

func TestT3a_StorageBackend_Sync_DispatchesToStorage(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, s := newTestBackend(t, f, 4, 4096)

			// Pre-sync state.
			preR, preS, _ := s.Boundaries()
			// Write to advance H; Sync should bring R up.
			if _, err := b.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := b.Sync(context.Background()); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			// Post-sync: the synced frontier (R) must not go
			// backward and must match or exceed the newest LSN
			// captured before Sync (accounting for impl-specific
			// Sync semantics; we only pin "R doesn't regress").
			postR, _, _ := s.Boundaries()
			if postR < preR {
				t.Fatalf("R regressed: pre=%d post=%d", preR, postR)
			}
			_ = preS
		})
	}
}

// ------- G9A ACK profile seam -------

func TestG9A_StorageBackend_WriteAckPolicy_BestEffortObserverErrorStillACKs(t *testing.T) {
	observerErr := errors.New("replica recovering")
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 4, 4096)
			b.SetWriteObserver(writeObserverFunc{
				observe: func(context.Context, uint32, uint64, []byte) error {
					return observerErr
				},
			})

			n, err := b.Write(context.Background(), 0, make([]byte, 4096))
			if err != nil {
				t.Fatalf("best-effort Write must not fail on observer error, got %v", err)
			}
			if n != 4096 {
				t.Fatalf("Write n=%d want 4096", n)
			}
		})
	}
}

func TestG9A_StorageBackend_WriteAckPolicy_RequireObserverAckFailsWithoutObserver(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 4, 4096)
			b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)

			n, err := b.Write(context.Background(), 0, make([]byte, 4096))
			if !errors.Is(err, durable.ErrReplicationAckUnavailable) {
				t.Fatalf("strict Write without observer: want ErrReplicationAckUnavailable, got n=%d err=%v", n, err)
			}
		})
	}
}

func TestG9A_StorageBackend_WriteAckPolicy_DisablesBatchWithoutObserver(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 4, 4096)
			b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)

			n, err := b.Write(context.Background(), 0, make([]byte, 8192))
			if !errors.Is(err, durable.ErrReplicationAckUnavailable) {
				t.Fatalf("strict batch-shaped Write without observer: want ErrReplicationAckUnavailable, got n=%d err=%v", n, err)
			}
			if n != 0 {
				t.Fatalf("strict write must fail before local batch write, n=%d", n)
			}
		})
	}
}

func TestG9A_StorageBackend_WriteAckPolicy_RequireObserverAckPropagatesObserverError(t *testing.T) {
	observerErr := errors.New("replica recovering")
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 4, 4096)
			b.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)
			b.SetWriteObserver(writeObserverFunc{
				observe: func(context.Context, uint32, uint64, []byte) error {
					return observerErr
				},
			})

			n, err := b.Write(context.Background(), 0, make([]byte, 4096))
			if !errors.Is(err, durable.ErrReplicationAckUnavailable) {
				t.Fatalf("strict Write observer error: want ErrReplicationAckUnavailable, got n=%d err=%v", n, err)
			}
			if !errors.Is(err, observerErr) {
				t.Fatalf("strict Write should wrap observer cause, got %v", err)
			}
		})
	}
}

// ------- Close behavior -------

func TestT3a_StorageBackend_Close_ReturnsErrBackendClosed(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, _, _ := newTestBackend(t, f, 4, 4096)
			if err := b.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if _, err := b.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrBackendClosed) {
				t.Fatalf("Read after Close: want ErrBackendClosed, got %v", err)
			}
			if _, err := b.Write(context.Background(), 0, []byte("x")); !errors.Is(err, frontend.ErrBackendClosed) {
				t.Fatalf("Write after Close: want ErrBackendClosed, got %v", err)
			}
			if err := b.Sync(context.Background()); !errors.Is(err, frontend.ErrBackendClosed) {
				t.Fatalf("Sync after Close: want ErrBackendClosed, got %v", err)
			}
		})
	}
}

// Stop-rule sanity — ensure every test ran against every registered impl.
func TestT3a_MatrixCoverage(t *testing.T) {
	names := []string{}
	for _, f := range logicalStorageFactories() {
		names = append(names, f.name)
	}
	got := fmt.Sprint(names)
	want := "[walstore smartwal parallelwal]"
	if got != want {
		t.Fatalf("matrix drifted: got %s want %s", got, want)
	}
}

// G5-5 test: SetIdentity latches a zero-Epoch backend (the
// pre-assignment shape that EnsureStorage produces — VolumeID +
// ReplicaID set from CLI config, Epoch=0+EV=0). Same-replica
// authority advances are accepted, while backward or cross-replica
// drift is rejected so lineageCheck fail-closed still catches stale
// writers.
func TestG5_5_SetIdentity_LatchFromZeroEpoch(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			s := f.make(t, 16, 4096)
			preAssignment := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 0, EndpointVersion: 0}
			view := newStubView(frontend.Projection{
				VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, Healthy: true,
			})
			b := durable.NewStorageBackend(s, view, preAssignment)
			if got := b.Identity(); got.Epoch != 0 || got.VolumeID != "v1" {
				t.Fatalf("pre-latch: expected {v1, r1, 0, 0}, got %+v", got)
			}
			id1 := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			if !b.SetIdentity(id1) {
				t.Fatal("SetIdentity from Epoch=0 must return true")
			}
			if got := b.Identity(); got != id1 {
				t.Fatalf("post-latch identity: got %+v want %+v", got, id1)
			}
			if b.SetIdentity(id1) {
				t.Error("SetIdentity after latch must return false")
			}
			id2 := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1}
			if !b.SetIdentity(id2) {
				t.Error("SetIdentity same-replica epoch advance must return true")
			}
			if got := b.Identity(); got != id2 {
				t.Errorf("epoch advance did not mutate identity: got %+v want %+v", got, id2)
			}
			id3 := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 2}
			if !b.SetIdentity(id3) {
				t.Error("SetIdentity same-epoch endpoint-version advance must return true")
			}
			idOld := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 9}
			if b.SetIdentity(idOld) {
				t.Error("SetIdentity backward epoch must return false")
			}
			idOtherReplica := frontend.Identity{VolumeID: "v1", ReplicaID: "r2", Epoch: 3, EndpointVersion: 1}
			if b.SetIdentity(idOtherReplica) {
				t.Error("SetIdentity cross-replica drift must return false")
			}
			if got := b.Identity(); got != id3 {
				t.Errorf("rejected drift mutated identity: got %+v want %+v", got, id3)
			}
		})
	}
}
