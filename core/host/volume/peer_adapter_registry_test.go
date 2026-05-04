package volume

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// G5-5C Batch #7 PeerAdapterRegistry tests:
//   - OnPeerAdded constructs adapter and primes identity
//   - AdapterFor returns the right adapter (per-peer routing)
//   - OnPeerRemoved drops the adapter
//   - Self-replicaID guard refuses host-own-slot registration
//   - Lineage-bump teardown + recreate produces a fresh adapter
//   - Idempotent double-add is a no-op
//   - Close drops everything

func newPeerForRegistryTest(t *testing.T, id string, epoch, ev uint64) *replication.ReplicaPeer {
	t.Helper()
	addr := "127.0.0.1:1" // unreachable; registry tests don't call wire ops
	target := replication.ReplicaTarget{
		ReplicaID:       id,
		DataAddr:        addr,
		ControlAddr:     addr,
		Epoch:           epoch,
		EndpointVersion: ev,
	}
	primary := storage.NewBlockStore(8, 4096)
	exec := transport.NewBlockExecutor(primary, addr)
	peer, err := replication.NewReplicaPeer(target, exec)
	if err != nil {
		t.Fatalf("NewReplicaPeer: %v", err)
	}
	return peer
}

func TestPeerAdapterRegistry_OnPeerAdded_RegistersAndPrimes(t *testing.T) {
	reg := NewPeerAdapterRegistry("vol1", "r1") // self = r1

	peer := newPeerForRegistryTest(t, "r2", 1, 1)
	reg.OnPeerAdded(peer)

	if reg.Count() != 1 {
		t.Fatalf("Count after add: got %d, want 1", reg.Count())
	}
	adpt := reg.AdapterFor("r2")
	if adpt == nil {
		t.Fatal("AdapterFor(r2) returned nil after OnPeerAdded")
	}
	// Adapter must have received OnAssignment to prime engine identity
	// for r2 — projection's Epoch = 1 confirms the AssignmentObserved
	// event applied (engine state's Identity.Epoch advances on
	// AssignmentObserved). ReplicaProjection doesn't carry ReplicaID
	// directly, but the registry's keying-by-replicaID + this Epoch
	// check together pin the identity.
	proj := adpt.Projection()
	if proj.Epoch != 1 {
		t.Errorf("adapter projection Epoch = %d, want 1 (OnAssignment may not have fired)", proj.Epoch)
	}
	if proj.EndpointVersion != 1 {
		t.Errorf("adapter projection EndpointVersion = %d, want 1", proj.EndpointVersion)
	}
}

func TestPeerAdapterRegistry_OnPeerRemoved_DropsAdapter(t *testing.T) {
	reg := NewPeerAdapterRegistry("vol1", "r1")
	peer := newPeerForRegistryTest(t, "r2", 1, 1)
	reg.OnPeerAdded(peer)

	reg.OnPeerRemoved("r2")

	if reg.Count() != 0 {
		t.Errorf("Count after remove: got %d, want 0", reg.Count())
	}
	if reg.AdapterFor("r2") != nil {
		t.Error("AdapterFor(r2) returned non-nil after OnPeerRemoved")
	}
}

func TestPeerAdapterRegistry_SelfReplicaIDRefused(t *testing.T) {
	reg := NewPeerAdapterRegistry("vol1", "r1")
	// Try to register the host's own slot — must be refused as a
	// safety guard (host's own adapter is h.Adapter()).
	peer := newPeerForRegistryTest(t, "r1", 1, 1)
	reg.OnPeerAdded(peer)

	if reg.Count() != 0 {
		t.Errorf("Count after self-register attempt: got %d, want 0", reg.Count())
	}
	if reg.AdapterFor("r1") != nil {
		t.Error("AdapterFor(self) should be nil — registry must refuse self-register")
	}
}

func TestPeerAdapterRegistry_DoubleAdd_Idempotent(t *testing.T) {
	reg := NewPeerAdapterRegistry("vol1", "r1")
	peer := newPeerForRegistryTest(t, "r2", 1, 1)
	reg.OnPeerAdded(peer)
	first := reg.AdapterFor("r2")
	reg.OnPeerAdded(peer)
	second := reg.AdapterFor("r2")

	if reg.Count() != 1 {
		t.Errorf("Count after double-add: got %d, want 1", reg.Count())
	}
	if first != second {
		t.Error("double-add should NOT replace adapter — got different pointer second time")
	}
}

// TestPeerAdapterRegistry_LineageBumpTeardown verifies the lineage-
// bump teardown path: OnPeerRemoved followed by OnPeerAdded for the
// same ReplicaID with bumped (epoch, EV) produces a FRESH adapter
// (different pointer) primed at the new lineage.
func TestPeerAdapterRegistry_LineageBumpTeardown(t *testing.T) {
	reg := NewPeerAdapterRegistry("vol1", "r1")

	oldPeer := newPeerForRegistryTest(t, "r2", 1, 1)
	reg.OnPeerAdded(oldPeer)
	oldAdpt := reg.AdapterFor("r2")
	if oldAdpt == nil {
		t.Fatal("setup: oldAdpt nil")
	}
	if oldAdpt.Projection().Epoch != 1 {
		t.Fatalf("old projection Epoch = %d, want 1", oldAdpt.Projection().Epoch)
	}

	// Lineage bump: teardown then add at epoch=2.
	reg.OnPeerRemoved("r2")
	newPeer := newPeerForRegistryTest(t, "r2", 2, 1)
	reg.OnPeerAdded(newPeer)

	newAdpt := reg.AdapterFor("r2")
	if newAdpt == nil {
		t.Fatal("newAdpt nil after lineage bump add")
	}
	if newAdpt == oldAdpt {
		t.Fatal("lineage bump should produce a fresh adapter pointer")
	}
	if newAdpt.Projection().Epoch != 2 {
		t.Errorf("new projection Epoch = %d, want 2", newAdpt.Projection().Epoch)
	}
}

func TestPeerAdapterRegistry_Close_DropsAll(t *testing.T) {
	reg := NewPeerAdapterRegistry("vol1", "r1")
	reg.OnPeerAdded(newPeerForRegistryTest(t, "r2", 1, 1))
	reg.OnPeerAdded(newPeerForRegistryTest(t, "r3", 1, 1))
	if reg.Count() != 2 {
		t.Fatalf("setup: Count = %d, want 2", reg.Count())
	}

	reg.Close()
	if reg.Count() != 0 {
		t.Errorf("Count after Close: got %d, want 0", reg.Count())
	}
	// Add after Close is a no-op.
	reg.OnPeerAdded(newPeerForRegistryTest(t, "r4", 1, 1))
	if reg.Count() != 0 {
		t.Errorf("Count after Add-post-Close: got %d, want 0", reg.Count())
	}
	// Close is idempotent.
	reg.Close()
}

func TestPeerAdapterRegistry_NilSafe(t *testing.T) {
	var reg *PeerAdapterRegistry // nil
	// All accessors must be safe on nil receiver.
	reg.OnPeerAdded(newPeerForRegistryTest(t, "r2", 1, 1))
	reg.OnPeerRemoved("r2")
	if reg.AdapterFor("r2") != nil {
		t.Error("nil registry AdapterFor should return nil")
	}
	if reg.Count() != 0 {
		t.Error("nil registry Count should return 0")
	}
	reg.Close()
}
