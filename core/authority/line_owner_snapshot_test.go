package authority

import (
	"testing"
	"time"
)

// ownerReader is the publisher view the snapshot builder reads: the line plus the server a move issued it to.
type ownerReader struct {
	*fakeReaderForHost
	owner map[string]PublishedLine
}

func (r ownerReader) PublishedAuthorityLine(volume string) (PublishedLine, bool) {
	pl, ok := r.owner[volume]
	return pl, ok
}

// #425 (V1-SPEC r2.40, INV-AUTH-HOLDER-IDENTITY-001): after a forced move s1 -> s2 the accepted topology still names
// s1 for r1. When s1 returns and reports r1 at its own endpoints, the controller's input for the line's replica must
// come from the line's owner s2, never s1: otherwise an endpoint refresh republishes the line with s1's endpoints.
func TestBuildSnapshotTakesTheMovedLinesReplicaFromItsOwner(t *testing.T) {
	start := time.Date(2026, 11, 22, 14, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 500 * time.Millisecond, PendingGrace: 100 * time.Millisecond}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{{VolumeID: "v1", Slots: []ExpectedSlot{{ReplicaID: "r1", ServerID: "s1"}}}}}
	at := func(addr string) SlotFact {
		s := healthySlot("v1", "r1")
		s.DataAddr, s.CtrlAddr = addr+":1", addr+":2"
		return s
	}
	_ = ingest(t, store, serverObs("s2", start, at("owner-s2")))
	_ = ingest(t, store, serverObs("s1", start, at("old-s1"))) // the old host, back with its own endpoints
	reader := ownerReader{fakeReaderForHost: newFakeReaderForHost(), owner: map[string]PublishedLine{
		"v1": {ReplicaID: "r1", Epoch: 2, IssuedHost: "s2", OwnerFromMove: true}}}
	reader.SetVolume("v1", AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 2, EndpointVersion: 1, DataAddr: "owner-s2:1", CtrlAddr: "owner-s2:2"})
	v, ok := findSupported(BuildSnapshot(store.Snapshot(), topo, reader), "v1")
	if !ok {
		t.Fatal("v1 not supported")
	}
	r1, ok := candidateByReplica(v, "r1")
	if !ok || r1.ServerID != "s2" || r1.DataAddr != "owner-s2:1" || r1.CtrlAddr != "owner-s2:2" {
		t.Fatalf("line replica candidate %+v ok=%v, want the owner s2 at owner-s2:1/2 (never the old host s1)", r1, ok)
	}
	// without a move-named owner the accepted topology stands (control)
	plain := ownerReader{fakeReaderForHost: reader.fakeReaderForHost, owner: map[string]PublishedLine{"v1": {ReplicaID: "r1", Epoch: 2}}}
	v, _ = findSupported(BuildSnapshot(store.Snapshot(), topo, plain), "v1")
	if r1, _ := candidateByReplica(v, "r1"); r1.ServerID != "s1" || r1.DataAddr != "old-s1:1" {
		t.Fatalf("control: no move-named owner, candidate %+v, want the topology's s1", r1)
	}
}
