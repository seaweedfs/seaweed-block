//go:build chunkstore_integration

package master

import (
	"context"
	"fmt"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

// Only replace MemChunkRemote with the actual local filer/blob implementation.
// The lease gate, 1GiB image, random writes/FUA/Flush and timer0 stay unchanged.
func TestChunkPressureLocalFilerDiagnostic(t *testing.T) {
	cluster := startWorkspaceWeed(t)
	remote := cluster.remote(t, "/swblock/diagnostic/v1")
	runChunkPressureRandomDiagnostic(t, []bool{true}, remote)
}

// Interaction control: four volumes share one real authority/lifecycle master
// and one loopback filer. This still does not create VMs or actual volume.Hosts.
func TestChunkPressureFourSharedMasterAndFilerDiagnostic(t *testing.T) {
	cluster := startWorkspaceWeed(t)
	h, conn, _ := reviewBindingHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := control.NewWorkspaceServiceClient(conn)
	var slots []*control.HeartbeatSlot
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("pressure-%d", i)
		if _, err := control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: id, SizeBytes: 1 << 30, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
		data, ctrl := fmt.Sprintf("127.0.0.1:%d", 22000+i), fmt.Sprintf("127.0.0.1:%d", 23000+i)
		if _, err := client.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: id, ServerId: "node-a", DataAddr: data, CtrlAddr: ctrl}); err != nil {
			t.Fatal(err)
		}
		if _, err := client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: id, ServerId: "node-a", ReplicaId: "r1"}); err != nil {
			t.Fatal(err)
		}
		slots = append(slots, &control.HeartbeatSlot{VolumeId: id, ReplicaId: "r1", DataAddr: data, CtrlAddr: ctrl})
	}
	if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true, Slots: slots}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	// Read all lines before parallel workers start; their records match1GiB geometry.
	for _, slot := range slots {
		waitAuthorityLine(t, h.Publisher(), slot.VolumeId)
	}
	for _, slot := range slots {
		id := slot.VolumeId
		remote := cluster.remote(t, "/swblock/diagnostic/"+id)
		t.Run(id, func(t *testing.T) { t.Parallel(); runChunkPressureRandomVolume(t, h, conn, id, []bool{true}, remote) })
	}
}
