package master

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestProductLoop_MissingRequiredFrontierRefusesRF3Promotion(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
	defer closeTestMaster(t, h)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_ = json.NewEncoder(w).Encode(volume.StatusProjection{Projection: frontend.Projection{VolumeID: "vol-rf3", ReplicaID: "r2"}, ReplicationRole: "replica_ready"})
		case "/status/durable":
			_ = json.NewEncoder(w).Encode(struct{ Volumes []durable.VolumeStatus }{Volumes: []durable.VolumeStatus{{VolumeID: "vol-rf3", ReplicaID: "r2", FrontierKnown: true, DurableLSN: 51}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	// Mirrors cmd/blockmaster.runLifecycleLauncherTick: no RequiredLSNByVolume.
	// A candidate reporting a local frontier is not proof of covering prior ACKs.
	h.SetPromotionEvidenceProvider(NewWorkloadPlanPromotionEvidenceProvider([]lifecycle.BlockVolumeWorkloadPlan{{VolumeID: "vol-rf3", Protocol: "iscsi", Replicas: []lifecycle.BlockVolumeReplicaWorkload{{ReplicaID: "r2", ISCSIListenPort: 3260}}}}, WorkloadPlanPromotionProbeConfig{AckProfile: "sync-quorum", HTTPClient: rewriteHTTPClient(server.URL, "127.0.0.1:23260")}))
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	establishVolume(t, h, "vol-rf3")
	if initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v", initial)
	}
	ingestRF3Observation(t, h, false, true, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatal(err)
	}
	if result.PromotionBlocked != 1 {
		t.Fatalf("unknown required frontier admitted promotion: %+v; candidate durable=51, required coverage unknown", result)
	}
}
