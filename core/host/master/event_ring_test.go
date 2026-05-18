package master

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMasterTimeline_RecordsPlacementVerified(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	timeline, err := newServices(h).GetVolumeTimeline(context.Background(), &control.GetVolumeTimelineRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	if !timelineHasEvent(timeline.GetEvents(), "placement_verified", "placement_verified") {
		t.Fatalf("timeline=%+v missing placement_verified", timeline.GetEvents())
	}
}

func TestMasterTimeline_RecordsPromotionCandidateEvaluation(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "pvc-a")
	h.SetPromotionEvidenceProvider(staticPromotionProbe{
		result: PromotionProbeResult{
			AckProfile:   "sync-quorum",
			SyncAckLSN:   52,
			CurrentKnown: true,
			Current: PromotionCandidateEvidence{
				ReplicaID:  initial.ReplicaID,
				Ready:      false,
				DurableLSN: 52,
			},
			Candidates: []PromotionCandidateEvidence{
				{ReplicaID: "r2", Ready: true, DurableLSN: 52},
				{ReplicaID: "r3", Ready: true, DurableLSN: 45},
			},
		},
	})
	ingestRF3ObservationForServersWithReady(t, h, "pvc-a", "m01", "m02", "tp01",
		false, false,
		true, true,
		true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("failover product tick: %v", err)
	}

	timeline, err := newServices(h).GetVolumeTimeline(context.Background(), &control.GetVolumeTimelineRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	if !timelineHasEvent(timeline.GetEvents(), "promotion_candidate_evaluated", "candidate_covers_required_frontier") {
		t.Fatalf("timeline=%+v missing promotion-ready candidate event", timeline.GetEvents())
	}
	if !timelineHasEvent(timeline.GetEvents(), "promotion_candidate_evaluated", "candidate_frontier_behind") {
		t.Fatalf("timeline=%+v missing behind candidate event", timeline.GetEvents())
	}
}

func TestMasterTimeline_RecordsAuthorityPublishedAfterMint(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	waitAuthorityLine(t, h.Publisher(), "pvc-a")

	timeline, err := newServices(h).GetVolumeTimeline(context.Background(), &control.GetVolumeTimelineRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	if !timelineHasEvent(timeline.GetEvents(), "authority_published", "candidate_covers_required_frontier") {
		t.Fatalf("timeline=%+v missing authority_published", timeline.GetEvents())
	}
}

func TestClusterEvidenceService_WatchClusterEventsCursor(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	waitAuthorityLine(t, h.Publisher(), "pvc-a")

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := control.NewClusterEvidenceServiceClient(conn)

	first, err := client.WatchClusterEvents(ctx, &control.WatchClusterEventsRequest{})
	if err != nil {
		t.Fatalf("first watch: %v", err)
	}
	firstEvents := drainWatchEvents(t, first)
	if len(firstEvents) == 0 {
		t.Fatal("first watch returned no events")
	}
	cursor := firstEvents[0].GetEventId()

	second, err := client.WatchClusterEvents(ctx, &control.WatchClusterEventsRequest{SinceEventId: cursor})
	if err != nil {
		t.Fatalf("second watch: %v", err)
	}
	secondEvents := drainWatchEvents(t, second)
	for _, event := range secondEvents {
		if event.GetEventId() == cursor {
			t.Fatalf("cursor event replayed: %s in %+v", cursor, secondEvents)
		}
	}
	if !containsEventType(secondEvents, "authority_published") {
		t.Fatalf("events after cursor=%+v missing authority_published", secondEvents)
	}
}

func timelineHasEvent(events []*control.ClusterEvent, eventType, reason string) bool {
	for _, event := range events {
		if event.GetEventType() == eventType && event.GetReasonCode() == reason && strings.TrimSpace(event.GetEventId()) != "" {
			return true
		}
	}
	return false
}

func drainWatchEvents(t *testing.T, stream control.ClusterEvidenceService_WatchClusterEventsClient) []*control.ClusterEvent {
	t.Helper()
	var out []*control.ClusterEvent
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("recv watch event: %v", err)
		}
		out = append(out, event)
	}
}

func containsEventType(events []*control.ClusterEvent, eventType string) bool {
	for _, event := range events {
		if event.GetEventType() == eventType {
			return true
		}
	}
	return false
}
