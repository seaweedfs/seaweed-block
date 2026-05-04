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

func TestStatusFrontendsForAssignedLine_FailClosedWhenUnassignedOrMissing(t *testing.T) {
	obs := authority.NewObservationHost(authority.ObservationHostConfig{})
	if got := statusFrontendsForAssignedLine(obs, "v1", "r1", false); len(got) != 0 {
		t.Fatalf("unassigned frontends=%+v want nil", got)
	}
	if got := statusFrontendsForAssignedLine(obs, "v1", "r1", true); len(got) != 0 {
		t.Fatalf("missing observation frontends=%+v want nil", got)
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
