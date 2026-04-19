package authority

import (
	"context"
	"sync"
	"testing"
	"time"
)

type basisReaderHolder struct {
	mu     sync.Mutex
	reader AuthorityBasisReader
}

func (h *basisReaderHolder) set(reader AuthorityBasisReader) {
	h.mu.Lock()
	h.reader = reader
	h.mu.Unlock()
}

func (h *basisReaderHolder) LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool) {
	h.mu.Lock()
	r := h.reader
	h.mu.Unlock()
	if r == nil {
		return AuthorityBasis{}, false
	}
	return r.LastAuthorityBasis(volumeID, replicaID)
}

func (h *basisReaderHolder) VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool) {
	h.mu.Lock()
	r := h.reader
	h.mu.Unlock()
	if r == nil {
		return AuthorityBasis{}, false
	}
	return r.VolumeAuthorityLine(volumeID)
}

func waitForLine(t *testing.T, pub *Publisher, volumeID string, pred func(AuthorityBasis) bool) AuthorityBasis {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, ok := pub.VolumeAuthorityLine(volumeID)
		if ok && pred(line) {
			return line
		}
		time.Sleep(10 * time.Millisecond)
	}
	line, _ := pub.VolumeAuthorityLine(volumeID)
	t.Fatalf("volume %s did not reach expected line, last=%+v", volumeID, line)
	return AuthorityBasis{}
}

func waitForConsumerAtLeast(t *testing.T, cons *recordingConsumer, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(cons.snapshot()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("consumer did not reach %d deliveries, got %d", n, len(cons.snapshot()))
}

func observedAuthorityFrom(pub *Publisher, volumeID string) AuthorityBasis {
	line, ok := pub.VolumeAuthorityLine(volumeID)
	if !ok {
		return AuthorityBasis{}
	}
	return line
}

func TestTopologyControllerToPublisher_E2E_MultiVolumePlacementAndFailover(t *testing.T) {
	holder := &basisReaderHolder{}
	ctrl := NewTopologyController(TopologyControllerConfig{}, holder)
	pub := NewPublisher(ctrl)
	holder.set(pub)

	v1Consumer := &recordingConsumer{}
	v2Consumer := &recordingConsumer{}
	v3Consumer := &recordingConsumer{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, v1Consumer, "v1", "v1r1", "v1r2", "v1r3")
	go VolumeBridge(ctx, pub, v2Consumer, "v2", "v2r1", "v2r2", "v2r3")
	go VolumeBridge(ctx, pub, v3Consumer, "v3", "v3r1", "v3r2", "v3r3")

	initial := ClusterSnapshot{
		CollectedRevision: 1,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1", AuthorityBasis{}, candidate("v1r1", "s1", "d11", "c11", 10), candidate("v1r2", "s2", "d12", "c12", 10), candidate("v1r3", "s3", "d13", "c13", 10)),
			clusterVolume("v2", AuthorityBasis{}, candidate("v2r1", "s1", "d21", "c21", 10), candidate("v2r2", "s2", "d22", "c22", 10), candidate("v2r3", "s3", "d23", "c23", 10)),
			clusterVolume("v3", AuthorityBasis{}, candidate("v3r1", "s1", "d31", "c31", 10), candidate("v3r2", "s2", "d32", "c32", 10), candidate("v3r3", "s3", "d33", "c33", 10)),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(initial); err != nil {
		t.Fatalf("SubmitClusterSnapshot initial: %v", err)
	}
	v1Line := waitForLine(t, pub, "v1", func(line AuthorityBasis) bool { return line.Assigned })
	v2Line := waitForLine(t, pub, "v2", func(line AuthorityBasis) bool { return line.Assigned })
	v3Line := waitForLine(t, pub, "v3", func(line AuthorityBasis) bool { return line.Assigned })

	waitForConsumerAtLeast(t, v1Consumer, 1)
	waitForConsumerAtLeast(t, v2Consumer, 1)
	waitForConsumerAtLeast(t, v3Consumer, 1)

	servers := map[string]string{
		"v1": mustReplicaServer(t, initial.Volumes[0], v1Line.ReplicaID),
		"v2": mustReplicaServer(t, initial.Volumes[1], v2Line.ReplicaID),
		"v3": mustReplicaServer(t, initial.Volumes[2], v3Line.ReplicaID),
	}
	seen := map[string]struct{}{servers["v1"]: {}, servers["v2"]: {}, servers["v3"]: {}}
	if len(seen) != 3 {
		t.Fatalf("initial placement should balance across 3 servers, got %+v", servers)
	}

	failover := initial
	failover.CollectedRevision = 2
	failover.Volumes[0].Authority = observedAuthorityFrom(pub, "v1")
	failover.Volumes[1].Authority = observedAuthorityFrom(pub, "v2")
	failover.Volumes[2].Authority = observedAuthorityFrom(pub, "v3")
	currentV1 := failover.Volumes[0].Authority.ReplicaID
	for i := range failover.Volumes[0].Slots {
		slot := &failover.Volumes[0].Slots[i]
		if slot.ReplicaID == currentV1 {
			slot.Reachable = false
			slot.ReadyForPrimary = false
		}
		if slot.ServerID == "s3" {
			slot.EvidenceScore = 50
		}
	}
	if err := ctrl.SubmitClusterSnapshot(failover); err != nil {
		t.Fatalf("SubmitClusterSnapshot failover: %v", err)
	}
	newV1 := waitForLine(t, pub, "v1", func(line AuthorityBasis) bool {
		return line.ReplicaID != currentV1 && line.Epoch == v1Line.Epoch+1
	})
	if newV1.ReplicaID == currentV1 {
		t.Fatalf("expected v1 to fail over, still on %s", currentV1)
	}
	waitForConsumerAtLeast(t, v1Consumer, 2)
}

func TestTopologyControllerToPublisher_E2E_Rebalance(t *testing.T) {
	holder := &basisReaderHolder{}
	ctrl := NewTopologyController(TopologyControllerConfig{RebalanceSkew: 1}, holder)
	pub := NewPublisher(ctrl)
	holder.set(pub)

	if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "v1r1", DataAddr: "d11", CtrlAddr: "c11", Intent: IntentBind}); err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	if err := pub.apply(AssignmentAsk{VolumeID: "v2", ReplicaID: "v2r1", DataAddr: "d21", CtrlAddr: "c21", Intent: IntentBind}); err != nil {
		t.Fatalf("seed v2: %v", err)
	}
	if err := pub.apply(AssignmentAsk{VolumeID: "v3", ReplicaID: "v3r2", DataAddr: "d32", CtrlAddr: "c32", Intent: IntentBind}); err != nil {
		t.Fatalf("seed v3: %v", err)
	}

	v1Consumer := &recordingConsumer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, v1Consumer, "v1", "v1r1", "v1r2", "v1r3")

	snap := ClusterSnapshot{
		CollectedRevision: 1,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1", observedAuthorityFrom(pub, "v1"),
				candidate("v1r1", "s1", "d11", "c11", 10),
				candidate("v1r2", "s2", "d12", "c12", 10),
				candidate("v1r3", "s3", "d13", "c13", 30),
			),
			clusterVolume("v2", observedAuthorityFrom(pub, "v2"),
				candidate("v2r1", "s1", "d21", "c21", 10),
				candidate("v2r2", "s2", "d22", "c22", 10),
				candidate("v2r3", "s3", "d23", "c23", 10),
			),
			clusterVolume("v3", observedAuthorityFrom(pub, "v3"),
				candidate("v3r1", "s1", "d31", "c31", 10),
				candidate("v3r2", "s2", "d32", "c32", 10),
				candidate("v3r3", "s3", "d33", "c33", 10),
			),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	line := waitForLine(t, pub, "v1", func(line AuthorityBasis) bool {
		return line.ReplicaID == "v1r3" && line.Epoch == 2
	})
	if line.ReplicaID != "v1r3" {
		t.Fatalf("expected rebalance to v1r3, got %+v", line)
	}
	waitForConsumerAtLeast(t, v1Consumer, 2)
}

func mustReplicaServer(t *testing.T, vol VolumeTopologySnapshot, replicaID string) string {
	t.Helper()
	slot, ok := candidateByReplica(vol, replicaID)
	if !ok {
		t.Fatalf("replica %s not found in volume %s", replicaID, vol.VolumeID)
	}
	return slot.ServerID
}
