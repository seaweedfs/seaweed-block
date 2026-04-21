// Ownership: QA (from sketch-approved test spec v3-phase-15-t1-test-spec.md for T1 Frontend Contract Smoke).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t1-test-spec.md
// Maps to ledger rows: INV-FRONTEND-002
//
// Test layer: Unit
// Bad-state families: PCDD-STALE-PRIMARY-READ-001,
//   PCDD-STALE-PRIMARY-WRITE-001, PCDD-BACKEND-CLOSED-USE-001
// Bounded fate: Every Read/Write re-validates the full lineage
// against the current projection; any drift or post-Close use
// returns a named sentinel without side effects.
//
// Spec adaptation note: test-spec §4 uses
// engine.ReplicaProjection literals; see provider_test.go
// header for the frontend.Projection adjustment.
package frontend_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
)

func TestBackend_ReadWrite_CurrentLineage(t *testing.T) {
	projView := newFakeProjectionView(frontend.Projection{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3, Healthy: true,
	})
	provider := memback.NewProvider(projView)
	ctx := context.Background()
	backend, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer backend.Close()

	payload := []byte("hello-t1")
	n, err := backend.Write(ctx, 0, payload)
	if err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("Write: short write, got %d want %d", n, len(payload))
	}

	got := make([]byte, len(payload))
	n, err = backend.Read(ctx, 0, got)
	if err != nil {
		t.Fatalf("Read: unexpected error: %v", err)
	}
	if !bytes.Equal(got[:n], payload) {
		t.Fatalf("Read: got %q want %q", got[:n], payload)
	}
}

func TestBackend_ReadStaleLineage_ReturnsErrStalePrimary(t *testing.T) {
	projView := newFakeProjectionView(frontend.Projection{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3, Healthy: true,
	})
	provider := memback.NewProvider(projView)
	ctx := context.Background()
	backend, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer backend.Close()

	_, _ = backend.Write(ctx, 0, []byte("pre-stale"))

	projView.set(frontend.Projection{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, Healthy: true,
	})

	buf := make([]byte, 10)
	_, err = backend.Read(ctx, 0, buf)
	if !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("Read: expected ErrStalePrimary after lineage advance, got %v", err)
	}
}

func TestBackend_WriteStaleLineage_ReturnsErrStalePrimary(t *testing.T) {
	baseLineage := frontend.Projection{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3, Healthy: true,
	}
	cases := []struct {
		name  string
		drift frontend.Projection
	}{
		{"epoch_advance", func() frontend.Projection { p := baseLineage; p.Epoch = 6; return p }()},
		{"endpoint_version_advance", func() frontend.Projection { p := baseLineage; p.EndpointVersion = 4; return p }()},
		{"replica_change", func() frontend.Projection { p := baseLineage; p.ReplicaID = "r2"; return p }()},
		{"health_loss", func() frontend.Projection { p := baseLineage; p.Healthy = false; return p }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projView := newFakeProjectionView(baseLineage)
			provider := memback.NewProvider(projView)
			ctx := context.Background()
			backend, err := provider.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer backend.Close()

			projView.set(tc.drift)

			_, err = backend.Write(ctx, 0, []byte("stale-write"))
			if !errors.Is(err, frontend.ErrStalePrimary) {
				t.Fatalf("Write: expected ErrStalePrimary under %s, got %v", tc.name, err)
			}
		})
	}
}

func TestBackend_Close_ReturnsErrBackendClosed(t *testing.T) {
	projView := newFakeProjectionView(frontend.Projection{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3, Healthy: true,
	})
	provider := memback.NewProvider(projView)
	ctx := context.Background()
	backend, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := backend.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	_, err = backend.Read(ctx, 0, make([]byte, 4))
	if !errors.Is(err, frontend.ErrBackendClosed) {
		t.Fatalf("Read after Close: expected ErrBackendClosed, got %v", err)
	}
	_, err = backend.Write(ctx, 0, []byte("post-close"))
	if !errors.Is(err, frontend.ErrBackendClosed) {
		t.Fatalf("Write after Close: expected ErrBackendClosed, got %v", err)
	}
	// Second Close idempotent (nil acceptable per spec).
	_ = backend.Close()
}
