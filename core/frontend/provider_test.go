// Ownership: QA (from sketch-approved test spec v3-phase-15-t1-test-spec.md for T1 Frontend Contract Smoke).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t1-test-spec.md
// Maps to ledger rows: INV-FRONTEND-001
//
// Test layer: Unit
// Bad-state family: PCDD-FRONTEND-NOT-READY-001 (T1.L0.2)
// Bounded fate: Provider returns backend only when projection is
// healthy for the requested volume; returns ErrNotReady under
// deadline expiry.
//
// Spec adaptation note: test-spec §4 uses
// engine.ReplicaProjection{VolumeID, ReplicaID, Healthy}, which
// are fields the current engine type does not carry. Sketch §5
// permits adjusting the imported projection type; the frontend
// package defines frontend.Projection for that purpose. Tests
// below use frontend.Projection instead of engine.ReplicaProjection.
package frontend_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
)

func TestProvider_OpenHealthyProjection_ReturnsBackend(t *testing.T) {
	projView := newFakeProjectionView(frontend.Projection{
		VolumeID:        "v1",
		ReplicaID:       "r1",
		Epoch:           5,
		EndpointVersion: 3,
		Healthy:         true,
	})

	provider := memback.NewProvider(projView)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	backend, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open: unexpected error: %v", err)
	}
	if backend == nil {
		t.Fatal("Open: backend is nil")
	}
	defer backend.Close()

	id := backend.Identity()
	if id.VolumeID != "v1" || id.ReplicaID != "r1" || id.Epoch != 5 || id.EndpointVersion != 3 {
		t.Fatalf("Identity mismatch: got %+v", id)
	}
}

func TestProvider_OpenNotReady_ReturnsErrNotReady(t *testing.T) {
	projView := newFakeProjectionView(frontend.Projection{
		VolumeID: "v1",
		Healthy:  false,
	})
	provider := memback.NewProvider(projView)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	backend, err := provider.Open(ctx, "v1")
	if backend != nil {
		t.Fatal("Open: expected nil backend when projection not ready")
	}
	if !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("Open: expected ErrNotReady, got %v", err)
	}
}
