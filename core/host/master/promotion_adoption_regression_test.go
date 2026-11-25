package master

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// The decision and adoption are separate events. A successful fresh probe
// cannot authorize an empty replacement store opened after the line was issued.
func TestPromotionRefusesReplicaMissingAcknowledgedWrite(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "retained_store"
		if replace {
			name = "recreated_after_decision"
		}
		t.Run(name, func(t *testing.T) {
			h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
			t.Cleanup(func() { closeTestMaster(t, h) })
			cluster := component.NewCluster(t, component.Walstore).WithReplicas(2).WithLiveShip().Start()
			cluster.Primary().RepVol.SetDurabilityMode(replication.DurabilitySyncAll)
			acknowledged := bytes.Repeat([]byte{0xa7}, 4096)
			cluster.PrimaryWriteViaBackend(7, acknowledged)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := cluster.Primary().Backend.Sync(ctx); err != nil {
				t.Fatalf("guest sync ACK: %v", err)
			}
			// Shipping to both peers is a legal extension of the one-peer ACK trace.
			// The product still uses its UNKNOWN-frontier survivor rule, not a test LSN.
			cluster.Primary().RepVol.SetDurabilityMode(replication.DurabilitySyncQuorum)
			for i := 0; i < 2; i++ {
				got, err := cluster.Replica(i).Store.Read(7)
				if err != nil || !bytes.Equal(got, acknowledged) {
					t.Fatalf("ACK fixture replica %d missing data: %v", i, err)
				}
			}
			seedRF3Placement(t, h)
			ingestRF3Observation(t, h, true, true, true)
			if _, err := h.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
			if initial.ReplicaID != "r1" {
				t.Fatalf("initial line=%+v", initial)
			}
			if err := cluster.Primary().RepVol.Close(); err != nil {
				t.Fatal(err)
			}
			servers := make([]*httptest.Server, 2)
			plans := []lifecycle.BlockVolumeWorkloadPlan{{VolumeID: "vol-rf3", Protocol: "iscsi"}}
			routes := map[string]string{}
			for i, id := range []string{"r2", "r3"} {
				store := cluster.Replica(i).Store
				servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/status":
						_ = json.NewEncoder(w).Encode(volume.StatusProjection{Projection: frontend.Projection{VolumeID: "vol-rf3", ReplicaID: id}, ReplicationRole: "replica_ready"})
					case "/status/durable":
						_, s, _ := store.Boundaries()
						_ = json.NewEncoder(w).Encode(struct{ Volumes []durable.VolumeStatus }{[]durable.VolumeStatus{{VolumeID: "vol-rf3", ReplicaID: id, FrontierKnown: true, DurableLSN: s}}})
					default:
						http.NotFound(w, r)
					}
				}))
				t.Cleanup(servers[i].Close)
				port := 3260 + i
				plans[0].Replicas = append(plans[0].Replicas, lifecycle.BlockVolumeReplicaWorkload{ReplicaID: id, ISCSIListenPort: port})
				host := "127.0.0.1:23260"
				if i == 1 {
					host = "127.0.0.1:23261"
				}
				routes[host] = servers[i].URL
			}
			h.SetPromotionEvidenceProvider(NewWorkloadPlanPromotionEvidenceProvider(plans, WorkloadPlanPromotionProbeConfig{AckProfile: "sync-quorum", HTTPClient: &http.Client{Transport: promotionTraceRoutes(routes), Timeout: time.Second}}))
			ingestRF3ObservationForServersWithReady(t, h, "vol-rf3", "node-a", "node-b", "node-c", false, false, true, true, true, true)
			if _, err := h.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			issued := initial
			publishDeadline := time.Now().Add(5 * time.Second)
			for issued.Epoch <= initial.Epoch && time.Now().Before(publishDeadline) {
				issued, _ = h.Publisher().VolumeAuthorityLine("vol-rf3")
				if issued.Epoch <= initial.Epoch {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if issued.Epoch <= initial.Epoch || (issued.ReplicaID != "r2" && issued.ReplicaID != "r3") {
				t.Fatalf("promotion did not publish a surviving replica: %+v", issued)
			}
			selected, serverID := 0, "node-b"
			if issued.ReplicaID == "r3" {
				selected, serverID = 1, "node-c"
			}
			targetStore := cluster.Replica(selected).Store
			// No target subscription exists yet: replacement occurs after publication,
			// before the actual blockvolume receives and adopts the published line.
			cluster.Replica(selected).Listener.StopHard()
			if replace {
				s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "replacement.wal"), 64, 4096)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				targetStore = s
			}
			rv := replication.NewReplicationVolume("vol-rf3", targetStore)
			t.Cleanup(func() { _ = rv.Close() })
			listener, err := transport.NewReplicaListener("127.0.0.1:0", targetStore)
			if err != nil {
				t.Fatal(err)
			}
			listener.Serve()
			t.Cleanup(listener.StopHard)
			target, err := volume.New(volume.Config{MasterAddr: h.Addr(), ServerID: serverID, VolumeID: "vol-rf3", ReplicaID: issued.ReplicaID, DataAddr: issued.DataAddr, CtrlAddr: issued.CtrlAddr, HeartbeatInterval: time.Second, EnableT1Readiness: true, ReplicationVolume: rv})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = target.Close() })
			if err := target.SetSelfFence(listener); err != nil {
				t.Fatal(err)
			}
			target.Start()
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if refused, reason := target.RoleAdmissionFailure(); refused {
					if !replace {
						t.Fatalf("retained-store control refused: %s", reason)
					}
					t.Logf("replacement refused: %s", reason)
					return
				}
				if p := target.Adapter().Projection(); p.Epoch >= issued.Epoch {
					got, err := targetStore.Read(7)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, acknowledged) {
						t.Fatalf("NoAckedLoss: issued epoch %d adopted after store replacement, acknowledged block missing", issued.Epoch)
					}
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("neither adoption nor explicit refusal: timeout is not safety evidence")
		})
	}
}

type promotionTraceRoutes map[string]string

func (routes promotionTraceRoutes) RoundTrip(r *http.Request) (*http.Response, error) {
	if base, ok := routes[r.URL.Host]; ok {
		return (rewriteRoundTripper{base: base, host: r.URL.Host}).RoundTrip(r)
	}
	return http.DefaultTransport.RoundTrip(r)
}
