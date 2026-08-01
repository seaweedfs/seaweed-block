package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG9G_ProductLoopPublishesVerifiedExistingReplica(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)

	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if result.PublishedAsks != 1 {
		t.Fatalf("result=%+v want one published ask", result)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-a")
	if line.ReplicaID != "r2" || line.Epoch != 1 || line.EndpointVersion != 1 {
		t.Fatalf("line=%+v want publisher-minted bind for r2", line)
	}
}

func TestG9G_ProductLoopDoesNotPublishUnverifiedPlacement(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID:  "vol-a",
			ServerID:  "node-a",
			ReplicaID: "r2",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}

	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if result.PublishedAsks != 0 {
		t.Fatalf("result=%+v want no published ask", result)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatal("unverified placement must not publish authority")
	}
}

func TestG9G_ProductLoopIsIdempotentForSameAuthorityLine(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("first product tick: %v", err)
	}
	first := waitAuthorityLine(t, h.Publisher(), "vol-a")
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("second product tick: %v", err)
	}
	if result.PublishedAsks != 0 {
		t.Fatalf("second tick result=%+v want no duplicate ask", result)
	}
	second, ok := h.Publisher().VolumeAuthorityLine("vol-a")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if second.ReplicaID != first.ReplicaID || second.Epoch != first.Epoch || second.EndpointVersion != first.EndpointVersion {
		t.Fatalf("line changed first=%+v second=%+v", first, second)
	}
}

func TestG9G_BlockvolumeSubscriptionReceivesProductLoopAssignment(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)
	ch, cancel := h.Publisher().Subscribe("vol-a", "r2")
	defer cancel()

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	got := recvAssignmentInfo(t, ch)
	if got.VolumeID != "vol-a" || got.ReplicaID != "r2" || got.Epoch != 1 || got.EndpointVersion != 1 {
		t.Fatalf("assignment=%+v want product-loop publisher bind", got)
	}
}

func TestMountedFailover_ProductLoopRF2UsesAuthorityControllerForFailover(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 2,
	})
	defer closeTestMaster(t, h)
	seedRF2Placement(t, h)
	ingestRF2Observation(t, h, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf2")
	if initial.ReplicaID != "r1" || initial.Epoch != 1 || initial.EndpointVersion != 1 {
		t.Fatalf("initial line=%+v want r1@1/1", initial)
	}

	ingestRF2Observation(t, h, false, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf2", "r2")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r2@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF2RefusesWhenNoCandidateReady(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 2,
	})
	defer closeTestMaster(t, h)
	seedRF2Placement(t, h)
	ingestRF2Observation(t, h, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf2")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF2Observation(t, h, false, false)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("refusal product tick: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	got, ok := h.Publisher().VolumeAuthorityLine("vol-rf2")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if got.ReplicaID != "r1" || got.Epoch != initial.Epoch || got.EndpointVersion != initial.EndpointVersion {
		t.Fatalf("unsafe failover occurred: got %+v want unchanged %+v", got, initial)
	}
}

func TestMountedFailover_ProductLoopRF2SnapshotIncludesAllConcretePlacements(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 2,
	})
	defer closeTestMaster(t, h)
	seedRF2PlacementForServers(t, h, "vol-rf2-a", "node-a1", "node-a2")
	seedRF2PlacementForServers(t, h, "vol-rf2-b", "node-b1", "node-b2")
	ingestRF2ObservationForServers(t, h, "vol-rf2-a", "node-a1", "node-a2", true, true)
	ingestRF2ObservationForServers(t, h, "vol-rf2-b", "node-b1", "node-b2", true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	for _, volumeID := range []string{"vol-rf2-a", "vol-rf2-b"} {
		line := waitAuthorityLine(t, h.Publisher(), volumeID)
		if line.ReplicaID != "r1" || line.Epoch != 1 || line.EndpointVersion != 1 {
			t.Fatalf("volume %s line=%+v want r1@1/1", volumeID, line)
		}
	}
}

func TestMountedFailover_ProductLoopRF3BlocksFailoverWithoutFreshPromotionProbe(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 1 {
		t.Fatalf("result=%+v want one RF3 promotion probe and one block", result)
	}
	time.Sleep(100 * time.Millisecond)
	got, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if got.ReplicaID != "r1" || got.Epoch != initial.Epoch || got.EndpointVersion != initial.EndpointVersion {
		t.Fatalf("RF3 failover must not use heartbeat readiness without fresh probe: got %+v want unchanged %+v", got, initial)
	}
}

func TestMountedFailover_ProductLoopRF3BlocksHealthyCurrentRebalanceWithoutProbe(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RebalanceSkew:          1,
	})
	defer closeTestMaster(t, h)
	seedRF3PlacementForServers(t, h, "vol-rf3-a", "node-a", "node-b", "node-c")
	seedRF3PlacementForServers(t, h, "vol-rf3-b", "node-a", "node-b", "node-c")
	ingestRF3ObservationForServers(t, h, "vol-rf3-a", "node-a", "node-b", "node-c", true, true, true)
	ingestRF3ObservationForServers(t, h, "vol-rf3-b", "node-a", "node-b", "node-c", true, true, true)

	for _, volumeID := range []string{"vol-rf3-a", "vol-rf3-b"} {
		if err := h.ctrl.SubmitAssignmentAsk(authority.AssignmentAsk{
			VolumeID:  volumeID,
			ReplicaID: "r1",
			DataAddr:  "127.0.0.1:19101",
			CtrlAddr:  "127.0.0.1:19102",
			Intent:    authority.IntentBind,
		}); err != nil {
			t.Fatalf("seed authority line for %s: %v", volumeID, err)
		}
	}
	initialA := waitAuthorityLine(t, h.Publisher(), "vol-rf3-a")
	initialB := waitAuthorityLine(t, h.Publisher(), "vol-rf3-b")
	if initialA.ReplicaID != "r1" || initialB.ReplicaID != "r1" {
		t.Fatalf("initial lines=%+v %+v want both r1", initialA, initialB)
	}

	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("rebalance product tick: %v", err)
	}
	if result.PromotionProbes != 2 || result.PromotionBlocked != 2 {
		t.Fatalf("result=%+v want RF3 rebalance candidates blocked without probe", result)
	}
	time.Sleep(100 * time.Millisecond)
	gotA, ok := h.Publisher().VolumeAuthorityLine("vol-rf3-a")
	if !ok {
		t.Fatal("authority line for vol-rf3-a disappeared")
	}
	gotB, ok := h.Publisher().VolumeAuthorityLine("vol-rf3-b")
	if !ok {
		t.Fatal("authority line for vol-rf3-b disappeared")
	}
	if gotA.ReplicaID != initialA.ReplicaID || gotA.Epoch != initialA.Epoch ||
		gotB.ReplicaID != initialB.ReplicaID || gotB.Epoch != initialB.Epoch {
		t.Fatalf("RF3 rebalance must not use heartbeat readiness without fresh probe: got %+v %+v want unchanged %+v %+v", gotA, gotB, initialA, initialB)
	}
}

func TestMountedFailover_ProductLoopRF3PromotesCandidateCoveringSyncAckProbe(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "sync-quorum",
			SyncAckLSN: 52,
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
				{ReplicaID: "r3", Ready: true, DurableLSN: 51},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want one successful RF3 promotion probe", result)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r2")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r2@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3PrefersHighestDurableLSNAboveRequired(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "sync-quorum",
			SyncAckLSN: 52,
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
				{ReplicaID: "r3", Ready: true, DurableLSN: 60},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want one successful RF3 promotion probe", result)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r3")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r3@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3UnknownRequiredLSNFallsBackToHighestDurableLSN(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "sync-quorum",
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 70},
				{ReplicaID: "r3", Ready: true, DurableLSN: 90},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want unknown required LSN to select highest durable survivor", result)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r3")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r3@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3BestEffortUnknownRequiredLSNCanSelectHighestDurableLSN(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "best-effort",
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 70},
				{ReplicaID: "r3", Ready: true, DurableLSN: 90},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want best-effort unknown required LSN to select highest durable survivor", result)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r3")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r3@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3PromotesWhenCurrentProbeIsDownDespiteStaleHeartbeat(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile:   "sync-quorum",
			SyncAckLSN:   52,
			CurrentKnown: true,
			Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: false},
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
				{ReplicaID: "r3", Ready: true, DurableLSN: 52},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	// Heartbeat state still says r1 is acceptable. The on-demand
	// current-primary probe is the fresher failure signal and must
	// force the authority decision to the proven survivor.
	ingestRF3Observation(t, h, true, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want one successful RF3 promotion probe", result)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r2")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r2@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3DoesNotPromoteWhenCurrentProbeIsReady(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile:   "sync-quorum",
			SyncAckLSN:   52,
			CurrentKnown: true,
			Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: true, DurableLSN: 52},
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	ingestRF3Observation(t, h, true, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("healthy product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want probe without blocked promotion", result)
	}
	time.Sleep(100 * time.Millisecond)
	got, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if got.ReplicaID != initial.ReplicaID || got.Epoch != initial.Epoch || got.EndpointVersion != initial.EndpointVersion {
		t.Fatalf("ready current primary must not be replaced: got %+v want unchanged %+v", got, initial)
	}
}

func TestMountedFailover_ProductLoopRF3ProbeCanAuthorizeStaleHeartbeatReadiness(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "sync-quorum",
			SyncAckLSN: 52,
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	ingestRF3ObservationForServersWithReady(t, h, "vol-rf3", "node-a", "node-b", "node-c",
		false, false,
		true, false,
		true, false)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 0 {
		t.Fatalf("result=%+v want one successful RF3 promotion probe", result)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r2")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want r2@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3PromotesWhenCurrentSlotMissingButSurvivorProbeReady(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "sync-quorum",
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 70},
				{ReplicaID: "r3", Ready: true, DurableLSN: 90},
			},
		},
	})
	seedRF3PlacementForServersWithAddrs(t, h, "vol-rf3", "node-a", "node-b", "node-c")
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}

	// K8s can keep the server/node fresh while the generated primary
	// blockvolume pod is gone. That produces PartialInventory and excludes
	// the volume from the normal supported snapshot; the product loop must
	// still use placement + probe evidence to promote an eligible survivor.
	ingestRF3PartialInventoryMissingCurrent(t, h, "vol-rf3", "node-a", "node-b", "node-c")
	placements := h.Lifecycle().Placements.ListPlacements()
	report := authority.SupportabilityReport{Unsupported: map[string]authority.VolumeUnsupportedEvidence{
		"vol-rf3": {VolumeID: "vol-rf3", Reasons: []string{authority.ReasonPartialInventory}},
	}}
	probes, blocked, asks := h.applyUnsupportedPromotionEvidenceGate(placements, report, nil)
	if probes != 1 || blocked != 0 || len(asks) != 1 {
		line, _ := h.Publisher().VolumeAuthorityLine("vol-rf3")
		t.Fatalf("probes=%d blocked=%d asks=%+v placements=%+v line=%+v want one direct unsupported promotion ask", probes, blocked, asks, placements, line)
	}
	if asks[0].ReplicaID != "r3" || asks[0].Intent != authority.IntentReassign {
		t.Fatalf("ask=%+v want highest durable survivor r3 reassign", asks[0])
	}
	if err := h.ctrl.SubmitAssignmentAsk(asks[0]); err != nil {
		t.Fatalf("submit unsupported promotion ask: %v", err)
	}
	failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r3")
	if failover.Epoch != 2 || failover.EndpointVersion != 1 {
		t.Fatalf("failover line=%+v want highest durable survivor r3@2/1", failover)
	}
}

func TestMountedFailover_ProductLoopRF3RejectsBestEffortProbeForHA(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "best-effort",
			SyncAckLSN: 52,
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 1 {
		t.Fatalf("result=%+v want best-effort probe blocked for RF3 HA", result)
	}
	time.Sleep(100 * time.Millisecond)
	got, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if got.ReplicaID != initial.ReplicaID || got.Epoch != initial.Epoch || got.EndpointVersion != initial.EndpointVersion {
		t.Fatalf("best-effort evidence must not satisfy RF3 HA promotion: got %+v want unchanged %+v", got, initial)
	}
}

func TestMountedFailover_ProductLoopRF3UnknownProbeCandidateCountsBlocked(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile: "sync-quorum",
			SyncAckLSN: 52,
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r9", Ready: true, DurableLSN: 52},
			},
		},
	})
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")

	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 1 || result.PromotionBlocked != 1 {
		t.Fatalf("result=%+v want unknown probe candidate counted as blocked", result)
	}
	time.Sleep(100 * time.Millisecond)
	got, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if got.ReplicaID != initial.ReplicaID || got.Epoch != initial.Epoch || got.EndpointVersion != initial.EndpointVersion {
		t.Fatalf("unknown probe candidate must not satisfy RF3 promotion: got %+v want unchanged %+v", got, initial)
	}
}

func TestG15c_ProductLoopReconcilesCreatedVolumeBeforePublishing(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create desired volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "node-a",
		DataAddr: "127.0.0.1:9202",
		CtrlAddr: "127.0.0.1:9102",
		Replicas: []lifecycle.ReplicaInventory{{
			VolumeID:  "pvc-a",
			ReplicaID: "r2",
			StoreUUID: "store-r2",
			SizeBytes: 1 << 20,
			State:     "existing",
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if err := h.ObservationHost().Ingest(authority.Observation{
		ServerID:   "node-a",
		ObservedAt: time.Now().UTC(),
		Slots: []authority.SlotFact{{
			VolumeID:  "pvc-a",
			ReplicaID: "r2",
			DataAddr:  "127.0.0.1:9202",
			CtrlAddr:  "127.0.0.1:9102",
		}},
	}); err != nil {
		t.Fatalf("ingest observation: %v", err)
	}

	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if result.ReconciledVolumes != 1 || result.PublishedAsks != 1 {
		t.Fatalf("result=%+v want reconciled+published", result)
	}
	if _, ok := stores.Placements.GetPlacement("pvc-a"); !ok {
		t.Fatal("placement intent not written from desired volume")
	}
	line := waitAuthorityLine(t, h.Publisher(), "pvc-a")
	if line.ReplicaID != "r2" {
		t.Fatalf("line=%+v want r2", line)
	}
}

func seedVerifiedExistingReplicaPlacement(t *testing.T, h *Host) {
	t.Helper()
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID:  "vol-a",
			ServerID:  "node-a",
			ReplicaID: "r2",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}
	if err := h.ObservationHost().Ingest(authority.Observation{
		ServerID:   "node-a",
		ObservedAt: time.Now().UTC(),
		Slots: []authority.SlotFact{{
			VolumeID:  "vol-a",
			ReplicaID: "r2",
			DataAddr:  "127.0.0.1:9202",
			CtrlAddr:  "127.0.0.1:9102",
		}},
	}); err != nil {
		t.Fatalf("ingest observation: %v", err)
	}
}

func seedRF2Placement(t *testing.T, h *Host) {
	t.Helper()
	seedRF2PlacementFor(t, h, "vol-rf2")
}

func seedRF3Placement(t *testing.T, h *Host) {
	t.Helper()
	seedRF3PlacementForServers(t, h, "vol-rf3", "node-a", "node-b", "node-c")
}

func seedRF2PlacementFor(t *testing.T, h *Host, volumeID string) {
	t.Helper()
	seedRF2PlacementForServers(t, h, volumeID, "node-a", "node-b")
}

func seedRF2PlacementForServers(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID string) {
	t.Helper()
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  volumeID,
		DesiredRF: 2,
		Candidates: []lifecycle.PlacementCandidate{
			{
				VolumeID:  volumeID,
				ServerID:  r1ServerID,
				ReplicaID: "r1",
				Source:    lifecycle.PlacementSourceExistingReplica,
			},
			{
				VolumeID:  volumeID,
				ServerID:  r2ServerID,
				ReplicaID: "r2",
				Source:    lifecycle.PlacementSourceExistingReplica,
			},
		},
	}); err != nil {
		t.Fatalf("apply RF2 placement: %v", err)
	}
}

func ingestRF2Observation(t *testing.T, h *Host, r1Ready, r2Ready bool) {
	t.Helper()
	ingestRF2ObservationFor(t, h, "vol-rf2", r1Ready, r2Ready)
}

func ingestRF3Observation(t *testing.T, h *Host, r1Ready, r2Ready, r3Ready bool) {
	t.Helper()
	ingestRF3ObservationForServers(t, h, "vol-rf3", "node-a", "node-b", "node-c", r1Ready, r2Ready, r3Ready)
}

func ingestRF2ObservationFor(t *testing.T, h *Host, volumeID string, r1Ready, r2Ready bool) {
	t.Helper()
	ingestRF2ObservationForServers(t, h, volumeID, "node-a", "node-b", r1Ready, r2Ready)
}

func ingestRF2ObservationForServers(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID string, r1Ready, r2Ready bool) {
	t.Helper()
	now := time.Now().UTC()
	for _, obs := range []authority.Observation{
		{
			ServerID:   r1ServerID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: r1Ready, Eligible: r1Ready},
			Slots: []authority.SlotFact{{
				VolumeID:        volumeID,
				ReplicaID:       "r1",
				DataAddr:        "127.0.0.1:19101",
				CtrlAddr:        "127.0.0.1:19102",
				Reachable:       r1Ready,
				ReadyForPrimary: r1Ready,
				Eligible:        r1Ready,
				EvidenceScore:   20,
			}},
		},
		{
			ServerID:   r2ServerID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: r2Ready, Eligible: r2Ready},
			Slots: []authority.SlotFact{{
				VolumeID:        volumeID,
				ReplicaID:       "r2",
				DataAddr:        "127.0.0.1:19201",
				CtrlAddr:        "127.0.0.1:19202",
				Reachable:       r2Ready,
				ReadyForPrimary: r2Ready,
				Eligible:        r2Ready,
				EvidenceScore:   10,
			}},
		},
	} {
		if err := h.ObservationHost().Ingest(obs); err != nil {
			t.Fatalf("ingest RF2 observation: %v", err)
		}
	}
}

func seedRF3PlacementForServers(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID, r3ServerID string) {
	t.Helper()
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  volumeID,
		DesiredRF: 3,
		Candidates: []lifecycle.PlacementCandidate{
			{VolumeID: volumeID, ServerID: r1ServerID, ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: volumeID, ServerID: r2ServerID, ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: volumeID, ServerID: r3ServerID, ReplicaID: "r3", Source: lifecycle.PlacementSourceExistingReplica},
		},
	}); err != nil {
		t.Fatalf("apply RF3 placement: %v", err)
	}
}

func seedRF3PlacementForServersWithAddrs(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID, r3ServerID string) {
	t.Helper()
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  volumeID,
		DesiredRF: 3,
		Candidates: []lifecycle.PlacementCandidate{
			{VolumeID: volumeID, ServerID: r1ServerID, ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica, DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102"},
			{VolumeID: volumeID, ServerID: r2ServerID, ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica, DataAddr: "127.0.0.1:19201", CtrlAddr: "127.0.0.1:19202"},
			{VolumeID: volumeID, ServerID: r3ServerID, ReplicaID: "r3", Source: lifecycle.PlacementSourceExistingReplica, DataAddr: "127.0.0.1:19301", CtrlAddr: "127.0.0.1:19302"},
		},
	}); err != nil {
		t.Fatalf("apply RF3 placement with addrs: %v", err)
	}
}

func TestPhase175AuthorityPlacementsStayClosedUntilRestoreCompletes(t *testing.T) {
	placements := []lifecycle.PlacementIntent{
		{VolumeID: "normal-a"},
		{VolumeID: "restored-a"},
	}
	volumes := []lifecycle.VolumeRecord{
		{Spec: lifecycle.VolumeSpec{VolumeID: "normal-a"}},
		{Spec: lifecycle.VolumeSpec{VolumeID: "restored-a", SourceSnapshotID: "snap-abc"}, RestoreState: lifecycle.VolumeRestorePending},
	}
	eligible, skipped := authorityEligiblePlacements(volumes, placements)
	if len(eligible) != 1 || eligible[0].VolumeID != "normal-a" || skipped != 1 {
		t.Fatalf("pending eligible=%+v skipped=%d", eligible, skipped)
	}
	volumes[1].RestoreState = lifecycle.VolumeRestoreComplete
	eligible, skipped = authorityEligiblePlacements(volumes, placements)
	if len(eligible) != 2 || skipped != 0 {
		t.Fatalf("complete eligible=%+v skipped=%d", eligible, skipped)
	}
}

func ingestRF3ObservationForServers(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID, r3ServerID string, r1Ready, r2Ready, r3Ready bool) {
	t.Helper()
	ingestRF3ObservationForServersWithReady(t, h, volumeID, r1ServerID, r2ServerID, r3ServerID,
		r1Ready, r1Ready,
		r2Ready, r2Ready,
		r3Ready, r3Ready)
}

func ingestRF3ObservationForServersWithReady(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID, r3ServerID string, r1Reachable, r1Ready, r2Reachable, r2Ready, r3Reachable, r3Ready bool) {
	t.Helper()
	now := time.Now().UTC()
	for i, obs := range []struct {
		serverID  string
		replicaID string
		reachable bool
		ready     bool
	}{
		{serverID: r1ServerID, replicaID: "r1", reachable: r1Reachable, ready: r1Ready},
		{serverID: r2ServerID, replicaID: "r2", reachable: r2Reachable, ready: r2Ready},
		{serverID: r3ServerID, replicaID: "r3", reachable: r3Reachable, ready: r3Ready},
	} {
		if err := h.ObservationHost().Ingest(authority.Observation{
			ServerID:   obs.serverID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: obs.reachable, Eligible: obs.reachable},
			Slots: []authority.SlotFact{{
				VolumeID:        volumeID,
				ReplicaID:       obs.replicaID,
				DataAddr:        "127.0.0.1:19" + string(rune('1'+i)) + "01",
				CtrlAddr:        "127.0.0.1:19" + string(rune('1'+i)) + "02",
				Reachable:       obs.reachable,
				ReadyForPrimary: obs.ready,
				Eligible:        obs.reachable,
				EvidenceScore:   uint64(30 - i),
			}},
		}); err != nil {
			t.Fatalf("ingest RF3 observation: %v", err)
		}
	}
}

func ingestRF3PartialInventoryMissingCurrent(t *testing.T, h *Host, volumeID, r1ServerID, r2ServerID, r3ServerID string) {
	t.Helper()
	now := time.Now().UTC()
	observations := []authority.Observation{
		{
			ServerID:   r1ServerID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: true, Eligible: true},
			Slots:      nil,
		},
		{
			ServerID:   r2ServerID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{
				VolumeID:        volumeID,
				ReplicaID:       "r2",
				DataAddr:        "127.0.0.1:19201",
				CtrlAddr:        "127.0.0.1:19202",
				Reachable:       true,
				ReadyForPrimary: true,
				Eligible:        true,
				EvidenceScore:   20,
			}},
		},
		{
			ServerID:   r3ServerID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{
				VolumeID:        volumeID,
				ReplicaID:       "r3",
				DataAddr:        "127.0.0.1:19301",
				CtrlAddr:        "127.0.0.1:19302",
				Reachable:       true,
				ReadyForPrimary: true,
				Eligible:        true,
				EvidenceScore:   10,
			}},
		},
	}
	for _, obs := range observations {
		if err := h.ObservationHost().Ingest(obs); err != nil {
			t.Fatalf("ingest RF3 partial inventory observation: %v", err)
		}
	}
}

type staticPromotionProbe struct {
	result PromotionProbeResult
	err    error
}

func (s staticPromotionProbe) ProbePromotionCandidates(string, authority.AuthorityBasis, []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	return s.result, s.err
}

func recvAssignmentInfo(t *testing.T, ch <-chan adapter.AssignmentInfo) adapter.AssignmentInfo {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for assignment")
		return adapter.AssignmentInfo{}
	}
}
