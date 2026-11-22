package master

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/csi"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// INV-AUTH-HOLDER-IDENTITY-001: after an issued workspace move the authority line is on the new host while the placement
// slot may still name the old one. The displayed primary node, its publish target and the CSI portal follow the ISSUED
// holder; the old host, still reporting the replica id with a newer observation, wins none of them.
func TestDisplayedPrimaryIsTheIssuedHolderNotThePlacementSlot(t *testing.T) {
	heads := map[string]authority.WorkspaceHead{"a2": {Gen: 9, Epoch: 1, Holder: "node-b/r1", RemoteLSN: 42, Detached: true}}
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		WorkspaceHeadReader: func(_ context.Context, v string) (authority.WorkspaceHead, error) { return heads[v], nil }})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	defer closeTestMaster(t, m)
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ws, lc, obs := control.NewWorkspaceServiceClient(conn), control.NewLifecycleServiceClient(conn), control.NewObservationServiceClient(conn)
	for _, n := range []struct{ id, d, c string }{{"node-a", "127.0.0.1:19101", "127.0.0.1:19102"}, {"node-b", "127.0.0.1:19301", "127.0.0.1:19302"}} {
		if _, err := m.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: n.id, DataAddr: n.d, CtrlAddr: n.c}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "a2", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "a2", DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{VolumeID: "a2", ServerID: "node-b", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := obs.ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-b", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "a2", ReplicaId: "r1", DataAddr: "127.0.0.1:19301", CtrlAddr: "127.0.0.1:19302"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	a2 := waitAuthorityLine(t, m.Publisher(), "a2")
	id := &control.WorkspaceIdentity{ServerId: "node-b", VolumeId: "a2", ReplicaId: "r1", Epoch: a2.Epoch}
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	g, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "to-a", VolumeId: "a2", ExpectedReplicaId: "r1", ExpectedEpoch: a2.Epoch,
		TargetReplicaId: "r1", TargetServerId: "node-a", TargetDataAddr: "127.0.0.1:19111", TargetCtrlAddr: "127.0.0.1:19112", Kind: "clean"})
	if err != nil || !g.Issued || g.NewHost != "node-a" {
		t.Fatalf("move to node-a: %+v %v", g, err)
	}
	line, _ := m.Publisher().VolumeAuthorityLine("a2")
	if p, ok := m.Lifecycle().Placements.GetPlacementSnapshot("a2"); ok {
		t.Logf("placement after the move: %+v (the displayed primary must not depend on it)", p.Slots)
	}
	fact := func(server, host string, at time.Time) authority.Observation {
		return authority.Observation{ServerID: server, ObservedAt: at, Server: authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{VolumeID: "a2", ReplicaID: "r1", DataAddr: host + ":19111", CtrlAddr: host + ":19112",
				Reachable: true, ReadyForPrimary: true, Eligible: true, EvidenceScore: 10,
				Frontends: []authority.FrontendTargetFact{{Protocol: "iscsi", Addr: host + ":3260", IQN: "iqn.2026-05.io.seaweedfs:a2", LUN: 1}}}}}
	}
	now := time.Now().UTC()
	if holder := m.workspaceSourceHost("a2", line.ReplicaID, line.Epoch); holder != "node-a" {
		t.Fatalf("issued holder %q, want node-a (line %+v)", holder, line)
	}
	lookup := csi.NewControlStatusLookup(control.NewEvidenceServiceClient(conn))
	// only the old host reports the replica: no primary evidence from the issued holder, so no portal at all
	if err := m.ObservationHost().Ingest(fact("node-b", "192.0.2.9", now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if target, err := lookup.LookupPublishTarget(ctx, "a2", ""); !errors.Is(err, csi.ErrPublishTargetNotFound) {
		t.Fatalf("CSI lookup with only the old host's claim: target %+v err %v, want ErrPublishTargetNotFound", target, err)
	}
	if err := m.ObservationHost().Ingest(fact("node-a", "127.0.0.1", now)); err != nil {
		t.Fatal(err)
	}
	if target, err := lookup.LookupPublishTarget(ctx, "a2", ""); err != nil || !strings.Contains(fmt.Sprintf("%+v", target), "127.0.0.1:3260") || strings.Contains(fmt.Sprintf("%+v", target), "192.0.2.9") {
		t.Fatalf("CSI lookup: target %+v err %v, want the issued holder's 127.0.0.1:3260", target, err)
	}
	var vol *struct{ node, target, rowServer, rowNode, rowFrontend string }
	for _, v := range m.ObservationSnapshot(time.Now().UTC()).Volumes {
		if v.VolumeID == "a2" {
			vol = &struct{ node, target, rowServer, rowNode, rowFrontend string }{node: v.PrimaryNode, target: v.PublishTarget}
			for _, r := range v.Replicas {
				if r.ReplicaID == line.ReplicaID {
					vol.rowServer, vol.rowNode, vol.rowFrontend = r.ServerID, r.KubernetesNode, r.FrontendAddr
				}
			}
		}
	}
	// the whole primary row is the issued holder (codex01 P2): never placement's node-b carrying node-a's frontend
	if vol == nil || vol.node != "node-a" || vol.target != "127.0.0.1:3260" || vol.rowServer != "node-a" || vol.rowNode != "node-a" || vol.rowFrontend != "127.0.0.1:3260" {
		t.Fatalf("displayed primary %+v, want node-a at 127.0.0.1:3260 in the volume and its primary row (the issued holder, not the placement slot or the old host)", vol)
	}
	resp, err := newServices(m).QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: "a2"})
	if err != nil || len(resp.Frontends) == 0 || resp.Frontends[0].Addr != "127.0.0.1:3260" {
		t.Fatalf("CSI portal %v %v, want the issued holder's 127.0.0.1:3260", resp.GetFrontends(), err)
	}
}

// heartbeatFrom sends a heartbeat for server whose slot addresses are on host.
func (r *isolationRig) heartbeatFrom(server, host string) error {
	_, err := r.obs.ReportHeartbeat(r.ctx, &control.HeartbeatReport{ServerId: server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "hb-" + server, ReplicaId: "r-" + server, DataAddr: host + ":19611", CtrlAddr: host + ":19612"}}})
	return err
}

// INV-AUTH-HOLDER-IDENTITY-001: OSS has no host authentication, so a server id names a host only if the master
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

// INV-AUTH-HOLDER-IDENTITY-001: the CSI portal (QueryVolumeStatus frontends) and the displayed primary
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

// dialFrom opens a client connection to the rig's master whose source address is host (a real loopback address).
func (r *isolationRig) dialFrom(t *testing.T, host string) *grpc.ClientConn {
	t.Helper()
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(host)}}
	conn, err := grpc.NewClient(r.m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) { return d.DialContext(ctx, "tcp", addr) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// INV-AUTH-HOLDER-IDENTITY-001, workspace path: two processes with the same --server-id on two real loopback hosts. The
// one not on the registered host (127.0.0.2) is refused at lease renew, at the revocation ACK and at the assignment
// stream (so it never receives the line to adopt), and each refusal is logged; an unregistered server id is refused at
// its first renew; the registered host is unaffected. The check trusts the network peer address (no NAT or proxy
// between volume and master in OSS); it is not authentication.
func TestWorkspaceHolderRefusedFromAnotherHostAtRenewAckAndStream(t *testing.T) {
	r := newIsolationRig(t, false, t.TempDir(), t.TempDir()) // node-a registered on 127.0.0.1
	r.createVolume(t, "w1")
	if _, err := r.m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "w1", DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{VolumeID: "w1", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.obs.ReportHeartbeat(r.ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "w1", ReplicaId: "r1", DataAddr: "127.0.0.1:19121", CtrlAddr: "127.0.0.1:19122"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.m.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, r.m.Publisher(), "w1")
	id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "w1", ReplicaId: "r1", Epoch: line.Epoch}
	other := r.dialFrom(t, "127.0.0.2")
	wsOther, asnOther := control.NewWorkspaceServiceClient(other), control.NewAssignmentServiceClient(other)
	refusedFrom2 := func(what string, err error) {
		t.Helper()
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "registered on host 127.0.0.1; refused from 127.0.0.2") {
			t.Fatalf("%s from 127.0.0.2 as node-a: %v, want the host refusal", what, err)
		}
	}
	_, err := wsOther.RenewWorkspaceLease(r.ctx, id)
	refusedFrom2("renew", err)
	_, err = wsOther.AckWorkspaceRevoked(r.ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9})
	refusedFrom2("ACK", err)
	stream, err := asnOther.SubscribeAssignments(r.ctx, &control.SubscribeRequest{VolumeId: "w1", ReplicaId: "r1", ServerId: "node-a"})
	if err == nil {
		_, err = stream.Recv()
	}
	refusedFrom2("assignment stream", err)
	if n := strings.Count(r.logs.String(), "refused from 127.0.0.2"); n < 3 {
		t.Fatalf("refusals logged %d times, want 3:\n%s", n, r.logs.String())
	}
	// the registered host is unaffected: it renews and receives the line
	if _, err := r.ws.RenewWorkspaceLease(r.ctx, id); err != nil {
		t.Fatalf("renew from the registered host: %v", err)
	}
	own, err := control.NewAssignmentServiceClient(r.dialFrom(t, "127.0.0.1")).SubscribeAssignments(r.ctx, &control.SubscribeRequest{VolumeId: "w1", ReplicaId: "r1", ServerId: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	if fact, err := own.Recv(); err != nil || fact.GetEpoch() != line.Epoch {
		t.Fatalf("assignment stream from the registered host: %+v %v", fact, err)
	}
	// an unregistered server id is refused at its first renew; a stream naming no server id is refused too
	_, err = r.ws.RenewWorkspaceLease(r.ctx, &control.WorkspaceIdentity{ServerId: "node-z", VolumeId: "w1", ReplicaId: "r1", Epoch: line.Epoch})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), `server id "node-z" is not registered`) {
		t.Fatalf("renew by an unregistered server id: %v, want the not-registered refusal", err)
	}
	anon := r.dialFrom(t, "127.0.0.1")
	s2, err := control.NewAssignmentServiceClient(anon).SubscribeAssignments(r.ctx, &control.SubscribeRequest{VolumeId: "w1", ReplicaId: "r1"})
	if err == nil {
		_, err = s2.Recv()
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), `server id "" is not registered`) {
		t.Fatalf("assignment stream with no server id: %v, want the not-registered refusal", err)
	}
}

// loopbackPeer is ctx as an RPC from 127.0.0.1 arrives at the master (in-process service calls carry no peer).
func loopbackPeer(ctx context.Context) context.Context {
	return peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000}})
}

// registerWorkspaceHolder registers server on 127.0.0.1 (a workspace holder must be registered).
func registerWorkspaceHolder(t *testing.T, h *Host, server string) {
	t.Helper()
	if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:19901", CtrlAddr: "127.0.0.1:19902"}); err != nil {
		t.Fatal(err)
	}
}

// codex01 P1: a CSI client takes the first frontend as the primary. When the primary's evidence (from the line's server)
// is missing, the status lists NO frontend, even though supporting replicas report theirs; without the rule a
// supporting replica's frontend would be published under the primary identity.
func TestStatusListsNoFrontendWhenThePrimaryEvidenceIsMissing(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	for _, r := range []struct{ server, replica, host string }{{"node-b", "r2", "127.0.0.2"}, {"node-c", "r3", "127.0.0.3"}} {
		if err := obs.IngestHeartbeat(authority.HeartbeatMessage{ServerID: r.server, SentAt: time.Now().UTC(), Reachable: true, Eligible: true,
			Slots: []authority.HeartbeatSlot{{VolumeID: "v1", ReplicaID: r.replica, DataAddr: r.host + ":9201", CtrlAddr: r.host + ":9101",
				Frontends: []authority.FrontendTargetFact{{Protocol: "iscsi", Addr: r.host + ":3260", IQN: "iqn.2026-05.example:v1"}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	noPrimary := func(replicaID string) (authority.SlotFact, bool) {
		if replicaID == "r1" {
			return authority.SlotFact{}, false // the line's server has no evidence for the primary
		}
		return obs.Store().SlotFact("v1", replicaID)
	}
	if got := statusFrontendsFor(obs, "v1", []string{"r1", "r2", "r3"}, true, "r1", noPrimary); len(got) != 0 {
		t.Fatalf("primary evidence missing: frontends %v, want none (a supporting replica must not stand in for the primary)", got)
	}
	if got := statusFrontendsFor(obs, "v1", []string{"r1", "r2", "r3"}, true, "", noPrimary); len(got) != 2 {
		t.Fatalf("control: without the primary rule the supporting frontends are listed (%v): the hazard this test pins", got)
	}
}
