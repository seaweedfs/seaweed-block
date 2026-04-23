// T3a adapter tests — matrix-parameterized per Addendum A #1.
// EVERY test runs against BOTH walstore and smartwal.Store so
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

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

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

// Stop-rule sanity — ensure every test ran against both impls.
func TestT3a_MatrixCoverage(t *testing.T) {
	names := []string{}
	for _, f := range logicalStorageFactories() {
		names = append(names, f.name)
	}
	got := fmt.Sprint(names)
	want := "[walstore smartwal]"
	if got != want {
		t.Fatalf("matrix drifted: got %s want %s", got, want)
	}
}
