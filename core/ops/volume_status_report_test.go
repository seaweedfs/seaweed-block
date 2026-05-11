package ops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestBuildVolumeStatusReport_ComposesExistingStatusSurfaces(t *testing.T) {
	capturedAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.FixedZone("test", -7*3600))
	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		CapturedAt: capturedAt,
		Source: ReportSource{
			Component: "component-test",
			Host:      "m02",
			Scenario:  "ops-volume-status",
		},
		ProductRevision: "abc123",
		RunnerRevision:  "runner456",
		MasterStatus: &control.StatusResponse{
			VolumeId:        "v1",
			ReplicaId:       "r1",
			Epoch:           1,
			EndpointVersion: 1,
			Assigned:        true,
			Frontends: []*control.FrontendTarget{
				{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
				{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
			},
		},
		LocalStatus: &hostvolume.StatusProjection{
			Projection: frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r1",
				Epoch:           2,
				EndpointVersion: 3,
				Healthy:         true,
			},
			FrontendPrimaryReady: true,
			AuthorityRole:        hostvolume.AuthorityRolePrimary,
			ReplicationRole:      hostvolume.ReplicationRoleNone,
		},
		Peers: []replication.ReplicaPeerStatus{
			{
				ReplicaID:       "r2",
				State:           "healthy",
				Epoch:           2,
				EndpointVersion: 3,
				DataAddr:        "127.0.0.1:10001",
				CtrlAddr:        "127.0.0.1:10002",
				SessionID:       99,
			},
		},
		Durable: []durable.VolumeStatus{
			{
				VolumeID:        "v1",
				Path:            "/var/lib/sw-block/v1.bin",
				Impl:            "smartwal",
				ReplicaID:       "r1",
				Epoch:           2,
				EndpointVersion: 3,
				Latched:         true,
				Operational:     true,
				Evidence:        "recover ok",
			},
		},
		Residue: ResidueReport{
			HostInitiator: HostInitiatorResidue{
				ISCSISessions:  []string{"tcp: [1] 127.0.0.1:3260 iqn.2026-05.io.seaweedfs:v1"},
				NVMESubsystems: []string{"nqn.2026-05.io.seaweedfs:v1"},
			},
			Processes:    []string{"blockvolume --volume v1"},
			Kubernetes:   []string{"deploy/sw-blockvolume-v1"},
			StoragePaths: []string{"/var/lib/sw-block/v1.bin"},
		},
	})

	if report.SchemaVersion != VolumeStatusReportSchemaVersion {
		t.Fatalf("schema_version=%q", report.SchemaVersion)
	}
	if !report.CapturedAt.Equal(capturedAt.UTC()) {
		t.Fatalf("captured_at=%s want %s", report.CapturedAt, capturedAt.UTC())
	}
	if report.ProductRevision != "abc123" || report.RunnerRevision != "runner456" {
		t.Fatalf("revision mismatch: product=%q runner=%q", report.ProductRevision, report.RunnerRevision)
	}
	if report.Volume.VolumeID != "v1" || report.Volume.ReplicaID != "r1" {
		t.Fatalf("volume identity mismatch: %+v", report.Volume)
	}
	if got, want := report.Volume.Protocols, []string{"iscsi", "nvme"}; !stringSlicesEqual(got, want) {
		t.Fatalf("protocols=%v want %v", got, want)
	}
	if len(report.Volume.Frontends) != 2 {
		t.Fatalf("frontends len=%d want 2", len(report.Volume.Frontends))
	}
	if report.Volume.Frontends[0].NQN == "" || report.Volume.Frontends[0].NSID != 1 {
		t.Fatalf("nvme frontend not preserved: %+v", report.Volume.Frontends[0])
	}
	if report.Authority.Epoch != 2 || report.Authority.EndpointVersion != 3 {
		t.Fatalf("authority should prefer local status lineage: %+v", report.Authority)
	}
	if !report.Authority.Assigned || report.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary {
		t.Fatalf("authority assignment/role mismatch: %+v", report.Authority)
	}
	if !report.Authority.Healthy || !report.Authority.FrontendPrimaryReady {
		t.Fatalf("authority readiness mismatch: %+v", report.Authority)
	}
	if report.Replication.ReplicationRole != hostvolume.ReplicationRoleNone {
		t.Fatalf("replication_role=%q", report.Replication.ReplicationRole)
	}
	if len(report.Replication.Peers) != 1 || !report.Replication.Peers[0].Healthy {
		t.Fatalf("peer healthy derivation failed: %+v", report.Replication.Peers)
	}
	if report.Replication.Peers[0].LastError != Unavailable {
		t.Fatalf("peer last_error=%q want unavailable", report.Replication.Peers[0].LastError)
	}
	if len(report.Durable) != 1 || !report.Durable[0].Latched || !report.Durable[0].Operational {
		t.Fatalf("durable status mismatch: %+v", report.Durable)
	}
	if len(report.Residue.HostInitiator.ISCSISessions) != 1 || len(report.Residue.HostInitiator.NVMESubsystems) != 1 {
		t.Fatalf("residue mismatch: %+v", report.Residue)
	}

	if _, err := json.Marshal(report); err != nil {
		t.Fatalf("status report must marshal as JSON: %v", err)
	}
}

func TestBuildVolumeStatusReport_JSONKeepsZeroValuedFrontendIdentity(t *testing.T) {
	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		MasterStatus: &control.StatusResponse{
			Frontends: []*control.FrontendTarget{
				{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
				{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
			},
		},
	})
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Volume struct {
			Frontends []map[string]any `json:"frontends"`
		} `json:"volume"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.Volume.Frontends[0]["lun"]; !ok {
		t.Fatalf("iSCSI frontend JSON omitted valid lun=0: %s", raw)
	}
	if _, ok := decoded.Volume.Frontends[0]["nsid"]; !ok {
		t.Fatalf("iSCSI frontend JSON omitted explicit nsid=0: %s", raw)
	}
	if _, ok := decoded.Volume.Frontends[1]["lun"]; !ok {
		t.Fatalf("NVMe frontend JSON omitted explicit lun=0: %s", raw)
	}
	if _, ok := decoded.Volume.Frontends[1]["nsid"]; !ok {
		t.Fatalf("NVMe frontend JSON omitted nsid=1: %s", raw)
	}
}

func TestBuildVolumeStatusReport_UsesMasterFrontendFacts(t *testing.T) {
	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		MasterStatus: &control.StatusResponse{
			VolumeId:  "v1",
			ReplicaId: "r1",
			Frontends: []*control.FrontendTarget{
				{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
			},
		},
		LocalStatus: &hostvolume.StatusProjection{
			Projection: frontend.Projection{VolumeID: "v1", ReplicaID: "r1"},
		},
	})

	if got := len(report.Volume.Frontends); got != 1 {
		t.Fatalf("frontends len=%d want 1", got)
	}
	if report.Volume.Frontends[0].Protocol != "nvme" {
		t.Fatalf("frontend should come from master StatusResponse, got %+v", report.Volume.Frontends[0])
	}
}

func TestBuildVolumeStatusReport_MarksUnavailableInputsExplicitly(t *testing.T) {
	report := BuildVolumeStatusReport(VolumeStatusReportInput{})

	if report.ProductRevision != Unavailable {
		t.Fatalf("product_revision=%q want unavailable", report.ProductRevision)
	}
	if report.Source.Component != Unavailable {
		t.Fatalf("source.component=%q want unavailable", report.Source.Component)
	}
	if report.Volume.VolumeID != Unavailable || report.Volume.ReplicaID != Unavailable {
		t.Fatalf("volume identity should be unavailable when not collected: %+v", report.Volume)
	}
	if report.Authority.AuthorityRole != Unavailable {
		t.Fatalf("authority_role=%q want unavailable", report.Authority.AuthorityRole)
	}
	if report.Replication.ReplicationRole != Unavailable {
		t.Fatalf("replication_role=%q want unavailable", report.Replication.ReplicationRole)
	}
	if report.Volume.Protocols == nil {
		t.Fatal("protocols should be an empty JSON array, not nil")
	}
	if report.Volume.Frontends == nil {
		t.Fatal("frontends should be an empty JSON array, not nil")
	}
	if report.Replication.Peers == nil {
		t.Fatal("peers should be an empty JSON array, not nil")
	}
	if report.Durable == nil {
		t.Fatal("durable should be an empty JSON array, not nil")
	}
	if report.Residue.HostInitiator.ISCSISessions == nil || report.Residue.HostInitiator.NVMESubsystems == nil ||
		report.Residue.Processes == nil || report.Residue.Kubernetes == nil || report.Residue.StoragePaths == nil {
		t.Fatalf("residue slices should be empty JSON arrays, not nil: %+v", report.Residue)
	}
}

func TestBuildVolumeStatusReport_CopiesInputSlices(t *testing.T) {
	residue := ResidueReport{
		HostInitiator: HostInitiatorResidue{ISCSISessions: []string{"session-1"}},
		Processes:     []string{"proc-1"},
		Kubernetes:    []string{"deploy-1"},
		StoragePaths:  []string{"/data/v1.bin"},
	}
	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		Residue: residue,
		Peers:   []replication.ReplicaPeerStatus{{ReplicaID: "r2", State: "degraded"}},
		Durable: []durable.VolumeStatus{{VolumeID: "v1", ReplicaID: "r1"}},
	})

	residue.HostInitiator.ISCSISessions[0] = "mutated"
	residue.Processes[0] = "mutated"
	residue.Kubernetes[0] = "mutated"
	residue.StoragePaths[0] = "mutated"

	if report.Residue.HostInitiator.ISCSISessions[0] != "session-1" ||
		report.Residue.Processes[0] != "proc-1" ||
		report.Residue.Kubernetes[0] != "deploy-1" ||
		report.Residue.StoragePaths[0] != "/data/v1.bin" {
		t.Fatalf("status report kept aliases to residue input: %+v", report.Residue)
	}
	if report.Replication.Peers[0].ReplicaID != "r2" || report.Durable[0].VolumeID != "v1" {
		t.Fatalf("peer/durable copy mismatch: peers=%+v durable=%+v", report.Replication.Peers, report.Durable)
	}
}

func TestBuildVolumeStatusReport_WritesSampleReportArtifact(t *testing.T) {
	out := os.Getenv("SW_BLOCK_OPS_STATUS_REPORT_OUT")
	if out == "" {
		out = filepath.Join(t.TempDir(), "volume-status-report.json")
	}

	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		CapturedAt: time.Date(2026, 5, 11, 19, 0, 0, 0, time.UTC),
		Source: ReportSource{
			Component: "core/ops",
			Host:      "m02",
			Scenario:  "operations-volume-status-report-component-gate",
		},
		ProductRevision: "product-for-report-artifact",
		RunnerRevision:  "runner-for-report-artifact",
		MasterStatus: &control.StatusResponse{
			VolumeId:        "v1",
			ReplicaId:       "r1",
			Epoch:           7,
			EndpointVersion: 2,
			Assigned:        true,
			Frontends: []*control.FrontendTarget{
				{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
				{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
			},
		},
		LocalStatus: &hostvolume.StatusProjection{
			Projection: frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r1",
				Epoch:           7,
				EndpointVersion: 2,
				Healthy:         true,
			},
			FrontendPrimaryReady: true,
			AuthorityRole:        hostvolume.AuthorityRolePrimary,
			ReplicationRole:      hostvolume.ReplicationRoleNone,
		},
		Peers: []replication.ReplicaPeerStatus{
			{ReplicaID: "r2", State: "healthy", Epoch: 7, EndpointVersion: 2},
		},
		Durable: []durable.VolumeStatus{
			{
				VolumeID:        "v1",
				Impl:            "smartwal",
				Path:            "/var/lib/sw-block/v1.blk",
				ReplicaID:       "r1",
				Epoch:           7,
				EndpointVersion: 2,
				Latched:         true,
				Operational:     true,
			},
		},
	})

	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatalf("mkdir artifact dir: %v", err)
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0o644); err != nil {
		t.Fatalf("write report artifact: %v", err)
	}

	written, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read report artifact: %v", err)
	}
	var decoded VolumeStatusReport
	if err := json.Unmarshal(written, &decoded); err != nil {
		t.Fatalf("decode report artifact: %v", err)
	}
	if decoded.SchemaVersion != VolumeStatusReportSchemaVersion {
		t.Fatalf("schema_version=%q want %q", decoded.SchemaVersion, VolumeStatusReportSchemaVersion)
	}
	if decoded.Volume.VolumeID != "v1" || decoded.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary {
		t.Fatalf("decoded report lost key fields: %+v", decoded)
	}

	text := string(written)
	for _, want := range []string{
		`"schema_version": "1.0"`,
		`"volume_id": "v1"`,
		`"lun": 0`,
		`"nsid": 1`,
		`"last_error": "unavailable"`,
		`"iscsi_sessions": []`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report artifact missing %s:\n%s", want, text)
		}
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
