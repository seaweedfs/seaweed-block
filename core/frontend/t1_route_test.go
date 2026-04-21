// Ownership: QA (from sketch-approved test spec v3-phase-15-t1-test-spec.md for T1 Frontend Contract Smoke).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t1-test-spec.md
// Maps to ledger rows: INV-FRONTEND-001 + INV-FRONTEND-002 + INV-AUTH-001 (consume)
//
// Test layer: Component (L1)
// Bounded fate: Over the in-process P14 publication route feeding
// a REAL VolumeReplicaAdapter (not a lineage-only fake view),
// frontend readiness derives from engine.DeriveProjection ->
// ModeHealthy, not from raw AssignmentInfo receipt. Authority
// movement (Epoch bump via IntentReassign) causes the adapter's
// engine projection to advance, AdapterProjectionView reflects
// the new lineage, the old backend's per-op fence rejects, and
// a new backend can open on the new lineage.
//
// Scope deviation vs test-spec §5 (sw review finding, routed to
// Discovery Bridge): the spec's T1.L1.1 asserts ReplicaID change
// across the move. Cross-replica authorship requires
// TopologyController-driven reassign, not StaticDirective. This
// L1 test proves the per-operation lineage fence against Epoch
// drift — the invariant the fence actually defends. ReplicaID-
// change coverage moves to L2 over the real product route.
package frontend_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
)

func TestT1Route_HealthyOpenWriteStaleRejectNewOpen(t *testing.T) {
	route := newT1Route(t, "v1")

	// PHASE 1: bind r1. Real adapter + HealthyPathExecutor drives
	// engine through Probe -> Fence -> ModeHealthy. The
	// AdapterProjectionView reports Healthy only AFTER the engine
	// actually reaches ModeHealthy (FencedEpoch advance).
	route.bindReplica(t, "r1", "data-r1-v1", "ctrl-r1-v1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	initial := route.waitUntilHealthy(t, "r1", 2*time.Second)
	if initial.ReplicaID != "r1" || initial.Epoch != 1 || initial.EndpointVersion != 1 {
		t.Fatalf("initial projection: %+v", initial)
	}

	// PHASE 2: open backend; write pre-move payload.
	provider := memback.NewProvider(route.view("r1"))
	oldBackend, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open initial: %v", err)
	}
	prePayload := []byte("pre-move-data")
	if _, err := oldBackend.Write(ctx, 0, prePayload); err != nil {
		t.Fatalf("Write pre-move: %v", err)
	}

	// PHASE 3: drive authority move through the publication path.
	// IntentReassign bumps Epoch for the same (volume, replica).
	// Engine sees new Identity.Epoch, re-probes, re-fences, and
	// Mode stays/returns to Healthy at the new Epoch. Old backend
	// still holds old Epoch — its per-op fence must reject.
	route.reassignReplica("r1", "data-r1-v1", "ctrl-r1-v1")
	newProj := route.waitUntilEpochAdvances(t, "r1", initial.Epoch, 3*time.Second)
	if newProj.Epoch <= initial.Epoch {
		t.Fatalf("post-move epoch must advance: old=%d new=%d", initial.Epoch, newProj.Epoch)
	}

	// PHASE 4: old backend rejects read and write.
	if _, err := oldBackend.Read(ctx, 0, make([]byte, len(prePayload))); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("old Read: expected ErrStalePrimary, got %v", err)
	}
	if _, err := oldBackend.Write(ctx, 16, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("old Write: expected ErrStalePrimary, got %v", err)
	}

	// PHASE 5: open new backend on new lineage; verify identity.
	newBackend, err := provider.Open(ctx, "v1")
	if err != nil {
		t.Fatalf("Open post-move: %v", err)
	}
	defer newBackend.Close()
	id := newBackend.Identity()
	if id.Epoch != newProj.Epoch || id.EndpointVersion != newProj.EndpointVersion {
		t.Fatalf("new backend Identity: %+v want Epoch=%d EV=%d", id, newProj.Epoch, newProj.EndpointVersion)
	}

	// PHASE 6: new backend serves post-move writes/reads.
	postPayload := []byte("post-move-data")
	if _, err := newBackend.Write(ctx, 0, postPayload); err != nil {
		t.Fatalf("Write post-move: %v", err)
	}
	got := make([]byte, len(postPayload))
	if _, err := newBackend.Read(ctx, 0, got); err != nil {
		t.Fatalf("Read post-move: %v", err)
	}
	if !bytes.Equal(got, postPayload) {
		t.Fatalf("Read post-move mismatch: got %q want %q", got, postPayload)
	}
	_ = oldBackend.Close()

	// EXPLICIT NON-CLAIM: pre-move bytes are NOT expected to
	// survive on the new lineage. That is G8 territory, not G2.
}
