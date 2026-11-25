//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// This small initiator uses the same exported wire types and admin/IO queue
// handshake as core/frontend/nvme/client_support_test.go. It talks only to the
// real blockvolume in this harness; no kernel device or unrelated NQN is used.
// Calibration of the witness client only, not a systemd/re-grant proof.
func TestGAP1NVMeWitnessRoundTripsAcknowledgedBlock(t *testing.T) {
	backend := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1})
	nqn := "nqn.2026-11.test:gap1-witness-calibration"
	target := nvme.NewTarget(nvme.TargetConfig{Listen: "127.0.0.1:0", SubsysNQN: nqn, VolumeID: "v", Provider: testback.NewStaticProvider(backend), Handler: nvme.HandlerConfig{NSID: 1, BlockSize: 4096, VolumeSize: 64 * 4096}})
	addr, err := target.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	c := gap1ConnectNVMe(t, addr, nqn)
	want := bytes.Repeat([]byte{0x6d}, 4096)
	c.writeFlush(t, 8, want)
	c.readExact(t, 8, want)
	if backend.WriteCount() != 1 || backend.SyncCount() != 1 || backend.ReadCount() != 1 {
		t.Fatalf("witness bypassed backend write/flush/read: %d/%d/%d", backend.WriteCount(), backend.SyncCount(), backend.ReadCount())
	}
}
