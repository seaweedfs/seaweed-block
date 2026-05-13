package ops

import (
	"strings"
	"testing"
	"time"

	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
)

func TestVolumeStatusSummary_OKReport(t *testing.T) {
	report := healthySummaryReport()

	if got := ClassifyVolumeStatusReport(report); got != VolumeStatusExitOK {
		t.Fatalf("exit=%d want %d issues=%v", got, VolumeStatusExitOK, VolumeStatusReportIssues(report))
	}
	summary := RenderVolumeStatusSummary(report)
	for _, want := range []string{
		"status: ok",
		"schema_version: 1.0",
		"source: component=component-test host=m02 scenario=summary",
		"volume: id=v1 replica=r1 protocols=iscsi,nvme frontends=2",
		"frontend: protocol=iscsi addr=127.0.0.1:3260 iqn=iqn.2026-05.io.seaweedfs:v1 nqn=- lun=0 nsid=0",
		"frontend: protocol=nvme addr=127.0.0.1:4420 iqn=- nqn=nqn.2026-05.io.seaweedfs:v1 lun=0 nsid=1",
		"authority: role=primary healthy=true primary_ready=true assigned=true epoch=7 endpoint_version=2",
		"residue: iscsi_sessions=0 nvme_subsystems=0 processes=0 kubernetes=0 storage_paths=0",
		"issues: none",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestVolumeStatusSummary_UnhealthyReport(t *testing.T) {
	report := healthySummaryReport()
	report.Authority.FrontendPrimaryReady = false
	report.Replication.Peers[0].Healthy = false
	report.Replication.Peers[0].State = "degraded"
	report.Durable[0].Operational = false
	report.Residue.HostInitiator.ISCSISessions = []string{"tcp: [1] 127.0.0.1:3260 iqn.2026-05.io.seaweedfs:v1"}

	if got := ClassifyVolumeStatusReport(report); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d want %d issues=%v", got, VolumeStatusExitUnhealthy, VolumeStatusReportIssues(report))
	}
	summary := RenderVolumeStatusSummary(report)
	for _, want := range []string{
		"status: unhealthy",
		"- primary frontend_primary_ready=false",
		"- peer r2 healthy=false state=degraded",
		"- durable v1/r1 operational=false",
		"- residue iscsi_sessions=1",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestVolumeStatusSummary_SupportingReplicaReadyIsClean(t *testing.T) {
	report := healthySummaryReport()
	report.Authority.AuthorityRole = hostvolume.AuthorityRoleUnknown
	report.Authority.Healthy = false
	report.Authority.FrontendPrimaryReady = false
	report.Replication.ReplicationRole = hostvolume.ReplicationRoleReady

	if got := ClassifyVolumeStatusReport(report); got != VolumeStatusExitOK {
		t.Fatalf("exit=%d want %d issues=%v", got, VolumeStatusExitOK, VolumeStatusReportIssues(report))
	}
	summary := RenderVolumeStatusSummary(report)
	for _, want := range []string{
		"status: ok",
		"authority: role=unknown healthy=false primary_ready=false assigned=true epoch=7 endpoint_version=2",
		"replication: role=replica_ready peers=1",
		"issues: none",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestVolumeStatusSummary_MissingReplicationRoleIsUnhealthy(t *testing.T) {
	report := healthySummaryReport()
	report.Replication.ReplicationRole = Unavailable

	if got := ClassifyVolumeStatusReport(report); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d want %d issues=%v", got, VolumeStatusExitUnhealthy, VolumeStatusReportIssues(report))
	}
	summary := RenderVolumeStatusSummary(report)
	for _, want := range []string{
		"status: unhealthy",
		"- replication_role unavailable",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestVolumeStatusSummary_InconsistentAuthorityReplicationPairIsUnhealthy(t *testing.T) {
	report := healthySummaryReport()
	report.Replication.ReplicationRole = hostvolume.ReplicationRoleReady

	if got := ClassifyVolumeStatusReport(report); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d want %d issues=%v", got, VolumeStatusExitUnhealthy, VolumeStatusReportIssues(report))
	}
	summary := RenderVolumeStatusSummary(report)
	if want := "- primary replication_role=replica_ready want none"; !strings.Contains(summary, want) {
		t.Fatalf("summary missing %q:\n%s", want, summary)
	}
}

func TestVolumeStatusSummary_InvalidReport(t *testing.T) {
	report := healthySummaryReport()
	report.SchemaVersion = "0.9"
	report.Volume.VolumeID = Unavailable

	if got := ClassifyVolumeStatusReport(report); got != VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d issues=%v", got, VolumeStatusExitInvalid, VolumeStatusReportIssues(report))
	}
	summary := RenderVolumeStatusSummary(report)
	for _, want := range []string{
		"status: invalid",
		"- invalid: schema_version=0.9 want 1.0",
		"- invalid: volume_id unavailable",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func healthySummaryReport() VolumeStatusReport {
	return VolumeStatusReport{
		SchemaVersion:   VolumeStatusReportSchemaVersion,
		CapturedAt:      time.Date(2026, 5, 11, 17, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Host: "m02", Scenario: "summary"},
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
		Volume: VolumeReport{
			VolumeID:  "v1",
			ReplicaID: "r1",
			Protocols: []string{"iscsi", "nvme"},
			Frontends: []FrontendReport{
				{Protocol: "iscsi", Addr: "127.0.0.1:3260", IQN: "iqn.2026-05.io.seaweedfs:v1", LUN: 0},
				{Protocol: "nvme", Addr: "127.0.0.1:4420", NQN: "nqn.2026-05.io.seaweedfs:v1", NSID: 1},
			},
		},
		Authority: AuthorityReport{
			Epoch:                7,
			EndpointVersion:      2,
			Assigned:             true,
			AuthorityRole:        "primary",
			FrontendPrimaryReady: true,
			Healthy:              true,
		},
		Replication: ReplicationReport{
			ReplicationRole: hostvolume.ReplicationRoleNone,
			Peers: []PeerReport{
				{ReplicaID: "r2", State: "healthy", Healthy: true, Epoch: 7, EndpointVersion: 2},
			},
		},
		Durable: []DurableReport{
			{VolumeID: "v1", Impl: "walstore", Path: "/var/lib/sw-block/v1/r1", ReplicaID: "r1", Epoch: 7, EndpointVersion: 2, Latched: true, Operational: true},
		},
		Residue: ResidueReport{
			HostInitiator: HostInitiatorResidue{
				ISCSISessions:  []string{},
				NVMESubsystems: []string{},
			},
			Processes:    []string{},
			Kubernetes:   []string{},
			StoragePaths: []string{},
		},
	}
}
