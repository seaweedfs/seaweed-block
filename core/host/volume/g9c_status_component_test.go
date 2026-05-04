package volume

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestG9C_StatusProjection_DualLaneRecoveredReplicaReadyAfterPostCloseAck(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithDualLaneRecovery().
			Start()

		const seedN = 24
		c.PrimaryWriteN(seedN)
		c.PrimarySync()
		// Keep the session visible long enough for HTTP status polling to
		// observe the intermediate recovering state.
		c.BlockExecutor(0).SetStepDelay(5 * time.Millisecond)

		a := c.Adapter(0)
		h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "replica-0"}}
		h.view = NewAdapterProjectionView(a, "v1", "replica-0", h)
		// The current frontend authority line names another replica at the
		// same lineage. This process can become a supporting replica_ready,
		// but must never report frontend-primary-ready.
		h.recordOtherLine(&control.AssignmentFact{
			VolumeId:        "v1",
			ReplicaId:       "primary-r1",
			Epoch:           1,
			EndpointVersion: 1,
		})
		s := NewStatusServer(h.view)
		addr, err := s.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("status start: %v", err)
		}
		defer func() { _ = s.Close(context.Background()) }()

		c.DriveAssignment(0, adapter.AssignmentInfo{
			VolumeID:        "v1",
			ReplicaID:       "replica-0",
			Epoch:           1,
			EndpointVersion: 1,
			DataAddr:        c.Replica(0).Addr,
			CtrlAddr:        c.Replica(0).Addr,
		})
		c.DriveProbeResult(0, adapter.ProbeResult{
			ReplicaID:         "replica-0",
			Success:           true,
			EndpointVersion:   1,
			TransportEpoch:    1,
			ReplicaFlushedLSN: 0,
			PrimaryTailLSN:    1,
			PrimaryHeadLSN:    seedN,
		})

		recovering := waitG9CStatus(t, addr, 2*time.Second, func(p StatusProjection) bool {
			return p.ReplicationRole == ReplicationRoleRecovering && !p.FrontendPrimaryReady
		})
		if recovering.Healthy {
			t.Fatalf("recovering supporting replica must not be Healthy yet: %+v", recovering)
		}

		ready := waitG9CStatus(t, addr, 5*time.Second, func(p StatusProjection) bool {
			return p.ReplicationRole == ReplicationRoleReady && !p.FrontendPrimaryReady
		})
		if !ready.Healthy {
			t.Fatalf("replica_ready should expose Healthy=true for support consumers: %+v", ready)
		}
	})
}

func waitG9CStatus(t *testing.T, addr string, timeout time.Duration, ok func(StatusProjection) bool) StatusProjection {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last StatusProjection
	for time.Now().Before(deadline) {
		last = fetchG9CStatus(t, addr)
		if ok(last) {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("status condition not met within %v; last=%+v", timeout, last)
	return StatusProjection{}
}

func fetchG9CStatus(t *testing.T, addr string) StatusProjection {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("status get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code=%d want 200", resp.StatusCode)
	}
	var body StatusProjection
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("status decode: %v", err)
	}
	return body
}
