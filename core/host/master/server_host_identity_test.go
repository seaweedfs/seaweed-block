package master

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// heartbeatFrom sends a heartbeat for server whose slot addresses are on host.
func (r *isolationRig) heartbeatFrom(server, host string) error {
	_, err := r.obs.ReportHeartbeat(r.ctx, &control.HeartbeatReport{ServerId: server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "hb-" + server, ReplicaId: "r-" + server, DataAddr: host + ":19611", CtrlAddr: host + ":19612"}}})
	return err
}

// INV-ASSIGNMENT-HOLDER-IDENTITY-001: OSS has no host authentication, so a server id names a host only if the master
// refuses that id from any host other than the one registered for it. Two processes started with the same --server-id
// on different hosts: the one on the registered host is accepted, the other is refused, and it never enters the
// observations (a copied config cannot borrow the registered host's holder identity).
func TestServerIDRefusedFromAHostOtherThanItsRegisteredOne(t *testing.T) {
	r := newIsolationRig(t, false, t.TempDir(), t.TempDir()) // node-a is registered on 127.0.0.1
	if err := r.heartbeatFrom("node-a", "127.0.0.1"); err != nil {
		t.Fatalf("heartbeat from node-a's registered host refused: %v", err)
	}
	err := r.heartbeatFrom("node-a", "127.0.0.2")
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "registered on host 127.0.0.1") {
		t.Fatalf("same --server-id from a second host: %v, want FailedPrecondition naming the registered host", err)
	}
	if slot, ok := r.m.ObservationHost().Store().SlotFactOnServer("node-a", "hb-node-a", "r-node-a"); !ok || !strings.HasPrefix(slot.DataAddr, "127.0.0.1:") {
		t.Fatalf("refused heartbeat reached the observations: %+v ok=%v", slot, ok)
	}
}

// INV-ASSIGNMENT-HOLDER-IDENTITY-001: the CSI portal (QueryVolumeStatus frontends) and the displayed primary
// (ClusterStatus primary node and publish target) come from the server on the volume's authority line, never from
// whichever host sent the freshest heartbeat for the same replica id. A stale old host that still reports the
// primary's replica, even more recently, changes neither.
func TestPortalAndPrimaryComeFromTheLineServerNotTheFreshestHeartbeat(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "pvc-a")
	holder := h.workspaceSourceHost("pvc-a", line.ReplicaID, line.Epoch)
	own, ok := h.ObservationHost().Store().SlotFactOnServer(holder, "pvc-a", line.ReplicaID)
	if holder == "" || !ok || len(own.Frontends) == 0 {
		t.Fatalf("line %+v names holder %q with no observed frontend", line, holder)
	}
	holderAddr := own.Frontends[0].Addr
	stale := ""
	for _, server := range []string{"m01", "m02", "tp01"} { // the fixture's placement servers
		if server != holder {
			stale = server
			break
		}
	}
	// the stale host reports the primary's replica id with a newer observation than the holder's
	if err := h.ObservationHost().Ingest(authority.Observation{ServerID: stale, ObservedAt: time.Now().UTC().Add(time.Second),
		Server: authority.ServerFact{Reachable: true, Eligible: true},
		Slots: []authority.SlotFact{{VolumeID: "pvc-a", ReplicaID: line.ReplicaID, DataAddr: "192.0.2.9:19101", CtrlAddr: "192.0.2.9:19102",
			Reachable: true, ReadyForPrimary: true, Eligible: true, EvidenceScore: 99,
			Frontends: []authority.FrontendTargetFact{{Protocol: "iscsi", Addr: "192.0.2.9:3260", IQN: "iqn.2026-05.io.seaweedfs:pvc-a", LUN: 1}}}}}); err != nil {
		t.Fatalf("ingest stale host: %v", err)
	}
	resp, err := newServices(h).QueryVolumeStatus(context.Background(), &control.StatusRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(resp.Frontends) == 0 || resp.Frontends[0].Addr != holderAddr {
		t.Fatalf("CSI portal: frontends=%v, want the line server %s's %s first (stale host %s must not win)", resp.Frontends, holder, holderAddr, stale)
	}
	for _, ft := range resp.Frontends {
		if ft.Addr == "192.0.2.9:3260" {
			t.Fatalf("CSI portal lists the stale host's frontend: %v", resp.Frontends)
		}
	}
	vol := h.ObservationSnapshot(time.Now().UTC()).Volumes[0]
	if vol.PrimaryNode != holder || vol.PublishTarget != holderAddr {
		t.Fatalf("displayed primary node=%q target=%q, want %q %q (line server, not the stale host %s)", vol.PrimaryNode, vol.PublishTarget, holder, holderAddr, stale)
	}
}
