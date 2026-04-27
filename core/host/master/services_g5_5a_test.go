// G5-5A — master peer-set construction must include topology slot
// members observed by heartbeat even when they don't have a bound
// authority publication. Architect ratification 2026-04-27 (round
// 54): Option A (observation fallback in collectPeers / resolvePeers).
//
// These tests pin the contract on `resolvePeers` (the pure helper
// extracted from `collectPeers`) so they exercise the full policy
// without constructing a real Host.

package master

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
)

// stubPub satisfies the peerLookup interface.
type stubPub struct {
	state map[[2]string]adapter.AssignmentInfo
}

func (s *stubPub) LastPublished(volumeID, replicaID string) (adapter.AssignmentInfo, bool) {
	info, ok := s.state[[2]string{volumeID, replicaID}]
	return info, ok
}

// stubObs satisfies the peerObservation interface.
type stubObs struct {
	state map[[2]string]authority.SlotFact
}

func (s *stubObs) SlotFact(volumeID, replicaID string) (authority.SlotFact, bool) {
	slot, ok := s.state[[2]string{volumeID, replicaID}]
	return slot, ok
}

// TestResolvePeers_R1Bound_R2ObservedOnly_R2InPeersWithHeartbeatAddr
// is the headline G5-5A pin per architect binding 2026-04-27:
//   "with r1 bound and r2 only heartbeat-observed as a supporting
//    slot member, the assignment fact delivered to r1 includes r2 in
//    peers with its heartbeat-reported DataAddr"
func TestResolvePeers_R1Bound_R2ObservedOnly_R2InPeersWithHeartbeatAddr(t *testing.T) {
	pub := &stubPub{state: map[[2]string]adapter.AssignmentInfo{
		{"v1", "r1"}: {VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "10.0.0.1:9220", CtrlAddr: "10.0.0.1:9210"},
	}}
	obs := &stubObs{state: map[[2]string]authority.SlotFact{
		{"v1", "r2"}: {VolumeID: "v1", ReplicaID: "r2", DataAddr: "10.0.0.2:9221", CtrlAddr: "10.0.0.2:9211"},
	}}

	peers := resolvePeers([]string{"r1", "r2"}, adapter.AssignmentInfo{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}, "v1", pub, obs)

	if len(peers) != 1 {
		t.Fatalf("expected exactly 1 peer (r2); got %d: %+v", len(peers), peers)
	}
	got := peers[0]
	if got.ReplicaId != "r2" {
		t.Errorf("ReplicaId: got %q want r2", got.ReplicaId)
	}
	if got.DataAddr != "10.0.0.2:9221" {
		t.Errorf("DataAddr: got %q want 10.0.0.2:9221 (heartbeat-reported)", got.DataAddr)
	}
	if got.CtrlAddr != "10.0.0.2:9211" {
		t.Errorf("CtrlAddr: got %q want 10.0.0.2:9211", got.CtrlAddr)
	}
	// Per architect ratification round 54: observation-derived peers
	// inherit the SUBSCRIBING primary's (Epoch, EV) — not zero —
	// because the live-ship lineage carries primary's authority,
	// which is how the supporting replica validates incoming frames.
	// Stubbed subscriber in this test is r1@1/EV=1.
	if got.Epoch != 1 {
		t.Errorf("Epoch: got %d want 1 (subscribing primary's epoch)", got.Epoch)
	}
	if got.EndpointVersion != 1 {
		t.Errorf("EndpointVersion: got %d want 1 (subscribing primary's EV)", got.EndpointVersion)
	}
}

// TestResolvePeers_BothBound — both slots have published authority
// lines; both contribute via path 1 with their authoritative
// epoch/EV/addrs. Confirms path-1 still works (no regression).
func TestResolvePeers_BothBound(t *testing.T) {
	pub := &stubPub{state: map[[2]string]adapter.AssignmentInfo{
		{"v1", "r1"}: {VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 2, DataAddr: "10.0.0.1:9220"},
		{"v1", "r2"}: {VolumeID: "v1", ReplicaID: "r2", Epoch: 4, EndpointVersion: 1, DataAddr: "10.0.0.2:9221"},
	}}
	obs := &stubObs{state: map[[2]string]authority.SlotFact{}}
	peers := resolvePeers([]string{"r1", "r2"}, adapter.AssignmentInfo{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}, "v1", pub, obs)
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer (r2, self r1 excluded); got %d", len(peers))
	}
	if peers[0].ReplicaId != "r2" || peers[0].Epoch != 4 || peers[0].EndpointVersion != 1 {
		t.Errorf("path-1 record: got %+v want {r2, Epoch=4, EV=1}", peers[0])
	}
}

// TestResolvePeers_NeitherBoundNorObserved — supporting replica that
// hasn't heartbeat in yet. Skipped fail-closed (caller never tries
// to dial an empty addr).
func TestResolvePeers_NeitherBoundNorObserved(t *testing.T) {
	pub := &stubPub{state: map[[2]string]adapter.AssignmentInfo{
		{"v1", "r1"}: {VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
	}}
	obs := &stubObs{state: map[[2]string]authority.SlotFact{}}
	peers := resolvePeers([]string{"r1", "r2"}, adapter.AssignmentInfo{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}, "v1", pub, obs)
	if len(peers) != 0 {
		t.Fatalf("expected empty peers when r2 unbound + unobserved; got %+v", peers)
	}
}

// TestResolvePeers_NilObservationStore — observation store may be
// nil at construction time. Must fall back to path-1 only.
func TestResolvePeers_NilObservationStore(t *testing.T) {
	pub := &stubPub{state: map[[2]string]adapter.AssignmentInfo{
		{"v1", "r1"}: {VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "10.0.0.1:9220"},
	}}
	peers := resolvePeers([]string{"r1", "r2"}, adapter.AssignmentInfo{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}, "v1", pub, nil)
	// Self r1 excluded; r2 path-2 fails (nil obs); peers empty.
	if len(peers) != 0 {
		t.Fatalf("expected empty peers with nil obs; got %+v", peers)
	}
}

// TestResolvePeers_PathOnePreferredOverPathTwo — when the same slot
// has BOTH a published line AND an observation, path-1 (authority)
// must win. The slot is the bound replica; observation is a stale
// heartbeat from before the bind.
func TestResolvePeers_PathOnePreferredOverPathTwo(t *testing.T) {
	pub := &stubPub{state: map[[2]string]adapter.AssignmentInfo{
		{"v1", "r1"}: {VolumeID: "v1", ReplicaID: "r1", Epoch: 7, EndpointVersion: 3, DataAddr: "AUTH:1"},
		{"v1", "r2"}: {VolumeID: "v1", ReplicaID: "r2", Epoch: 7, EndpointVersion: 3, DataAddr: "AUTH:2"},
	}}
	obs := &stubObs{state: map[[2]string]authority.SlotFact{
		{"v1", "r2"}: {VolumeID: "v1", ReplicaID: "r2", DataAddr: "OBS:2"},
	}}
	peers := resolvePeers([]string{"r1", "r2"}, adapter.AssignmentInfo{ReplicaID: "r1", Epoch: 7, EndpointVersion: 3}, "v1", pub, obs)
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer; got %d", len(peers))
	}
	if peers[0].DataAddr != "AUTH:2" {
		t.Errorf("DataAddr: got %q want AUTH:2 (path-1 must win over path-2)", peers[0].DataAddr)
	}
	if peers[0].Epoch != 7 {
		t.Errorf("Epoch: got %d want 7 (authority value)", peers[0].Epoch)
	}
}
