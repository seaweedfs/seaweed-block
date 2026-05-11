package ops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestVolumeStatusReportCollector_CollectsInjectedReadOnlySources(t *testing.T) {
	ctx := context.Background()
	calls := map[string]int{}
	capturedAt := time.Date(2026, 5, 11, 21, 0, 0, 0, time.UTC)

	report, err := VolumeStatusReportCollector{
		Now:             func() time.Time { return capturedAt },
		Source:          ReportSource{Component: "collector-test", Host: "m02", Scenario: "component"},
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
		MasterStatus: func(context.Context) (*control.StatusResponse, error) {
			calls["master"]++
			return &control.StatusResponse{
				VolumeId:        "v1",
				ReplicaId:       "r1",
				Epoch:           10,
				EndpointVersion: 5,
				Assigned:        true,
				Frontends: []*control.FrontendTarget{
					{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
				},
			}, nil
		},
		LocalStatus: func(context.Context) (*hostvolume.StatusProjection, error) {
			calls["local"]++
			return &hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:        "v1",
					ReplicaID:       "r1",
					Epoch:           10,
					EndpointVersion: 5,
					Healthy:         true,
				},
				FrontendPrimaryReady: true,
				AuthorityRole:        hostvolume.AuthorityRolePrimary,
				ReplicationRole:      hostvolume.ReplicationRoleNone,
			}, nil
		},
		Peers: func(context.Context) ([]replication.ReplicaPeerStatus, error) {
			calls["peers"]++
			return []replication.ReplicaPeerStatus{{ReplicaID: "r2", State: "healthy", Epoch: 10, EndpointVersion: 5}}, nil
		},
		Durable: func(context.Context) ([]durable.VolumeStatus, error) {
			calls["durable"]++
			return []durable.VolumeStatus{{VolumeID: "v1", ReplicaID: "r1", Epoch: 10, EndpointVersion: 5, Latched: true, Operational: true}}, nil
		},
		Residue: func(context.Context) (ResidueReport, error) {
			calls["residue"]++
			return ResidueReport{HostInitiator: HostInitiatorResidue{ISCSISessions: []string{}, NVMESubsystems: []string{}}}, nil
		},
	}.Collect(ctx)

	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, name := range []string{"master", "local", "peers", "durable", "residue"} {
		if calls[name] != 1 {
			t.Fatalf("%s called %d times, want 1 (calls=%v)", name, calls[name], calls)
		}
	}
	if !report.CapturedAt.Equal(capturedAt) {
		t.Fatalf("captured_at=%s want %s", report.CapturedAt, capturedAt)
	}
	if report.Source.Component != "collector-test" || report.ProductRevision != "product-rev" || report.RunnerRevision != "runner-rev" {
		t.Fatalf("metadata mismatch: %+v", report)
	}
	if report.Volume.VolumeID != "v1" || report.Volume.ReplicaID != "r1" || len(report.Volume.Frontends) != 1 {
		t.Fatalf("volume facts mismatch: %+v", report.Volume)
	}
	if report.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary || !report.Authority.FrontendPrimaryReady {
		t.Fatalf("authority facts mismatch: %+v", report.Authority)
	}
	if len(report.Replication.Peers) != 1 || len(report.Durable) != 1 {
		t.Fatalf("peer/durable facts mismatch: peers=%+v durable=%+v", report.Replication.Peers, report.Durable)
	}
}

func TestVolumeStatusReportCollector_ReturnsPartialReportWithSourceErrors(t *testing.T) {
	report, err := VolumeStatusReportCollector{
		Now:             func() time.Time { return time.Date(2026, 5, 11, 21, 30, 0, 0, time.UTC) },
		Source:          ReportSource{Component: "collector-test"},
		ProductRevision: "product-rev",
		MasterStatus: func(context.Context) (*control.StatusResponse, error) {
			return nil, errors.New("master unavailable")
		},
		LocalStatus: func(context.Context) (*hostvolume.StatusProjection, error) {
			return &hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:        "v1",
					ReplicaID:       "r2",
					Epoch:           11,
					EndpointVersion: 8,
				},
				AuthorityRole:   hostvolume.AuthorityRoleSuperseded,
				ReplicationRole: hostvolume.ReplicationRoleRecovering,
			}, nil
		},
		Peers: func(context.Context) ([]replication.ReplicaPeerStatus, error) {
			return nil, errors.New("peer source down")
		},
	}.Collect(context.Background())

	if err == nil {
		t.Fatal("expected joined collection error")
	}
	errText := err.Error()
	for _, want := range []string{"collect master status: master unavailable", "collect peer status: peer source down"} {
		if !strings.Contains(errText, want) {
			t.Fatalf("error %q missing %q", errText, want)
		}
	}
	if report.Volume.VolumeID != "v1" || report.Volume.ReplicaID != "r2" {
		t.Fatalf("partial report should keep local identity: %+v", report.Volume)
	}
	if report.Authority.AuthorityRole != hostvolume.AuthorityRoleSuperseded {
		t.Fatalf("partial report should keep local authority role: %+v", report.Authority)
	}
	if len(report.Replication.Peers) != 0 || report.Replication.Peers == nil {
		t.Fatalf("failed peer source should produce empty peers array: %+v", report.Replication.Peers)
	}
	if report.ProductRevision != "product-rev" {
		t.Fatalf("metadata lost on partial report: %+v", report)
	}
}

func TestVolumeStatusReportCollector_NilSourcesProduceUnavailableReport(t *testing.T) {
	report, err := VolumeStatusReportCollector{
		Now: func() time.Time { return time.Date(2026, 5, 11, 22, 0, 0, 0, time.UTC) },
	}.Collect(context.Background())

	if err != nil {
		t.Fatalf("nil sources should not fail: %v", err)
	}
	if report.Volume.VolumeID != Unavailable || report.Volume.ReplicaID != Unavailable {
		t.Fatalf("volume identity should be unavailable: %+v", report.Volume)
	}
	if report.ProductRevision != Unavailable || report.Source.Component != Unavailable {
		t.Fatalf("metadata should be explicit unavailable: product=%q source=%+v", report.ProductRevision, report.Source)
	}
	if report.Volume.Protocols == nil || report.Replication.Peers == nil || report.Durable == nil {
		t.Fatalf("nil sources should still emit empty arrays: %+v", report)
	}
}
