package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStatusFrontendsForAssignedLine_ReturnsObservedFrontendFacts(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	if err := obs.IngestHeartbeat(authority.HeartbeatMessage{
		ServerID:  "node-a",
		SentAt:    time.Now().UTC(),
		Reachable: true,
		Eligible:  true,
		Slots: []authority.HeartbeatSlot{{
			VolumeID:  "v1",
			ReplicaID: "r1",
			DataAddr:  "127.0.0.1:9201",
			CtrlAddr:  "127.0.0.1:9101",
			Frontends: []authority.FrontendTargetFact{{
				Protocol: "iscsi",
				Addr:     "127.0.0.1:3260",
				IQN:      "iqn.2026-05.example:v1",
				LUN:      0,
			}},
		}},
	}); err != nil {
		t.Fatalf("ingest heartbeat: %v", err)
	}

	got := statusFrontendsForAssignedLine(obs, "v1", "r1", true)
	if len(got) != 1 {
		t.Fatalf("frontends=%d want 1", len(got))
	}
	if got[0].GetProtocol() != "iscsi" || got[0].GetAddr() != "127.0.0.1:3260" || got[0].GetIqn() != "iqn.2026-05.example:v1" {
		t.Fatalf("frontend target=%+v", got[0])
	}
}

func TestStatusFrontendsForAssignedVolume_ReturnsObservedFrontendFactsForAllSlots(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	for _, hb := range []authority.HeartbeatMessage{
		frontendHeartbeat("node-a", "v1", "r1", "127.0.0.1:3260"),
		frontendHeartbeat("node-b", "v1", "r2", "127.0.0.1:3261"),
	} {
		if err := obs.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest heartbeat: %v", err)
		}
	}

	got := statusFrontendsForAssignedVolume(obs, "v1", []string{"r1", "r2"}, true)
	if len(got) != 2 {
		t.Fatalf("frontends=%d want 2", len(got))
	}
	if got[0].GetAddr() != "127.0.0.1:3260" || got[1].GetAddr() != "127.0.0.1:3261" {
		t.Fatalf("frontends=%+v", got)
	}
	if got[0].GetIqn() != "iqn.2026-05.example:v1" || got[1].GetIqn() != "iqn.2026-05.example:v1" {
		t.Fatalf("frontends must preserve shared multipath IQN: %+v", got)
	}
}

func TestStatusFrontendsForAssignedVolume_DedupesRepeatedFacts(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	hb := frontendHeartbeat("node-a", "v1", "r1", "127.0.0.1:3260")
	hb.Slots = append(hb.Slots, hb.Slots[0])
	if err := obs.IngestHeartbeat(hb); err != nil {
		t.Fatalf("ingest heartbeat: %v", err)
	}

	got := statusFrontendsForAssignedVolume(obs, "v1", []string{"r1", "r1"}, true)
	if len(got) != 1 {
		t.Fatalf("frontends=%d want 1: %+v", len(got), got)
	}
}

func TestStatusFrontendsForAssignedVolume_PreservesMultipleVolumesOnSameServer(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	for _, hb := range []authority.HeartbeatMessage{
		frontendHeartbeat("node-a", "v1", "r1", "127.0.0.1:3260"),
		frontendHeartbeat("node-a", "v2", "r1", "127.0.0.1:3261"),
	} {
		if err := obs.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest heartbeat: %v", err)
		}
	}

	gotV1 := statusFrontendsForAssignedVolume(obs, "v1", []string{"r1"}, true)
	if len(gotV1) != 1 || gotV1[0].GetAddr() != "127.0.0.1:3260" {
		t.Fatalf("v1 frontends=%+v, want first heartbeat preserved", gotV1)
	}
	gotV2 := statusFrontendsForAssignedVolume(obs, "v2", []string{"r1"}, true)
	if len(gotV2) != 1 || gotV2[0].GetAddr() != "127.0.0.1:3261" {
		t.Fatalf("v2 frontends=%+v, want second heartbeat", gotV2)
	}
}

func TestNodeLoss_PrimaryFirstReplicaIDsMovesPromotedReplicaFirst(t *testing.T) {
	got := primaryFirstReplicaIDs([]string{"r1", "r2", "r3"}, "r2")
	want := []string{"r2", "r1", "r3"}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v want %v", got, want)
		}
	}
}

func TestNodeLoss_StatusFrontendsForAssignedVolume_CurrentPrimaryFirstForCSIAttach(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	for _, hb := range []authority.HeartbeatMessage{
		frontendHeartbeat("node-a", "v1", "r1", "10.0.0.1:3260"),
		frontendHeartbeat("node-b", "v1", "r2", "10.0.0.2:3260"),
		frontendHeartbeat("node-c", "v1", "r3", "10.0.0.3:3260"),
	} {
		if err := obs.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest heartbeat: %v", err)
		}
	}

	replicas := primaryFirstReplicaIDs([]string{"r1", "r2", "r3"}, "r2")
	got := statusFrontendsForAssignedVolume(obs, "v1", replicas, true)
	if len(got) != 3 {
		t.Fatalf("frontends=%d want 3", len(got))
	}
	if got[0].GetAddr() != "10.0.0.2:3260" {
		t.Fatalf("first frontend=%q want promoted primary r2 address", got[0].GetAddr())
	}
}

func TestDynamicLifecycle_ReplicaSlotsForMergesFreshObservedSlots(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	for _, hb := range []authority.HeartbeatMessage{
		frontendHeartbeat("node-a", "v1", "r2", "127.0.0.1:4421"),
		frontendHeartbeat("node-a", "v1", "r1", "127.0.0.1:4420"),
	} {
		if err := obs.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest heartbeat: %v", err)
		}
	}
	h := &Host{
		obs:       obs,
		lifecycle: &LifecycleStores{},
	}

	got := h.replicaSlotsFor("v1")
	want := []string{"r1", "r2"}
	if len(got) != len(want) {
		t.Fatalf("replica slots=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replica slots=%v want %v", got, want)
		}
	}
}

func TestStatusFrontendsForAssignedLine_FailClosedWhenUnassignedOrMissing(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	if got := statusFrontendsForAssignedLine(obs, "v1", "r1", false); len(got) != 0 {
		t.Fatalf("unassigned frontends=%+v want nil", got)
	}
	if got := statusFrontendsForAssignedLine(obs, "v1", "r1", true); len(got) != 0 {
		t.Fatalf("missing observation frontends=%+v want nil", got)
	}
}

func frontendHeartbeat(serverID, volumeID, replicaID, frontendAddr string) authority.HeartbeatMessage {
	return authority.HeartbeatMessage{
		ServerID:  serverID,
		SentAt:    time.Now().UTC(),
		Reachable: true,
		Eligible:  true,
		Slots: []authority.HeartbeatSlot{{
			VolumeID:  volumeID,
			ReplicaID: replicaID,
			DataAddr:  "127.0.0.1:9201",
			CtrlAddr:  "127.0.0.1:9101",
			Frontends: []authority.FrontendTargetFact{{
				Protocol: "iscsi",
				Addr:     frontendAddr,
				IQN:      "iqn.2026-05.example:" + volumeID,
				LUN:      0,
			}},
		}},
	}
}

func TestValidateHeartbeat_RejectsMalformedFrontendFacts(t *testing.T) {
	base := func(ft *controlFrontend) error {
		return validateHeartbeat(ft.report())
	}
	cases := []struct {
		name string
		ft   controlFrontend
	}{
		{name: "empty-protocol", ft: controlFrontend{addr: "127.0.0.1:3260", iqn: "iqn.x"}},
		{name: "empty-addr", ft: controlFrontend{protocol: "iscsi", iqn: "iqn.x"}},
		{name: "iscsi-empty-iqn", ft: controlFrontend{protocol: "iscsi", addr: "127.0.0.1:3260"}},
		{name: "nvme-empty-nqn", ft: controlFrontend{protocol: "nvme", addr: "127.0.0.1:4420"}},
		{name: "bad-protocol", ft: controlFrontend{protocol: "nfs", addr: "127.0.0.1:2049"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := base(&tc.ft); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

type controlFrontend struct {
	protocol string
	addr     string
	iqn      string
	nqn      string
}

func (f controlFrontend) report() *control.HeartbeatReport {
	return &control.HeartbeatReport{
		ServerId: "node-a",
		SentAt:   timestamppb.Now(),
		Slots: []*control.HeartbeatSlot{{
			VolumeId:  "v1",
			ReplicaId: "r1",
			DataAddr:  "127.0.0.1:9201",
			CtrlAddr:  "127.0.0.1:9101",
			Frontends: []*control.FrontendTarget{{
				Protocol: f.protocol,
				Addr:     f.addr,
				Iqn:      f.iqn,
				Nqn:      f.nqn,
			}},
		}},
	}
}
