package nvme_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type syncPairView struct{ id frontend.Identity }

func (v syncPairView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: v.id.VolumeID, ReplicaID: v.id.ReplicaID, Epoch: v.id.Epoch, EndpointVersion: v.id.EndpointVersion, Healthy: true}
}

// Model the observed guest fsync's two Flush capsules, not a mounted guest.
func TestNVMePairedFlushSharesPhysicalWALSync(t *testing.T) {
	s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.DisableAutoFlushForRecoveryTest()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	backend := durable.NewStorageBackend(s, syncPairView{id}, id)
	backend.SetOperational(true, "test recovered store")
	tg := nvme.NewTarget(nvme.TargetConfig{Listen: "127.0.0.1:0", SubsysNQN: "nqn.2026-04.example.v3:subsys", VolumeID: "v1", Provider: testback.NewStaticProvider(backend)})
	addr, err := tg.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer tg.Close()
	c := dialAndConnect(t, addr)
	defer c.close()
	_ = c.io.SetDeadline(time.Now().Add(5 * time.Second))
	if status := c.writeCmd(t, 0, 8, make([]byte, 4096)); status != 0 {
		t.Fatalf("Write status=%x", status)
	}
	for i := 0; i < 2; i++ {
		cmd := nvme.CapsuleCommand{OpCode: 0, CID: uint16(90 + i), NSID: 1}
		if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
			t.Fatal(err)
		}
		resp := recvCapsuleResp(t, c.ioR)
		if resp.Status != 0 || resp.CID != cmd.CID {
			t.Fatalf("Flush %d completion=%+v", i, resp)
		}
	}
	if n := s.WALSyncCount(); n != 1 {
		t.Fatalf("one write + two Flush capsules: physical WAL syncs=%d want 1", n)
	}
	if n := backend.WriteProfile().BackendSyncOps; n != 2 {
		t.Fatalf("backend Sync calls=%d want 2 (retain gates)", n)
	}
	// Deduplication must not swallow the backend's operational gate.
	backend.SetOperational(false, "test readiness lost")
	if err := backend.Sync(context.Background()); err == nil {
		t.Fatal("cached flush bypassed readiness gate")
	}
}
