package durable_test

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// V1-SPEC r2.39: AdmitsIO is the one "may serve" function, the I/O gate
// itself: an operational backend whose guarded projection is not this
// lineage's healthy primary does not admit, nor does a backend that is not
// operational; both together admit.
func TestStorageBackend_AdmitsIOIsTheIOGate(t *testing.T) {
	for _, f := range logicalStorageFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			b, view, _ := newTestBackend(t, f, 4, 4096)
			if err := b.AdmitsIO(); err != nil {
				t.Fatalf("healthy and operational: %v, want nil", err)
			}
			p := view.Projection()
			p.Healthy = false
			view.set(p)
			if err := b.AdmitsIO(); !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("operational, projection not healthy: %v, want ErrStalePrimary", err)
			}
			p.Healthy = true
			view.set(p)
			b.SetOperational(false, "test: attach pending")
			if err := b.AdmitsIO(); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("healthy, not operational: %v, want ErrNotReady", err)
			}
		})
	}
}
