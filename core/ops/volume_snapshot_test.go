package ops

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestBuildVolumeStatusSnapshot_ComposesExistingStatusSurfaces(t *testing.T) {
	capturedAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.FixedZone("test", -7*3600))
	snap := BuildVolumeStatusSnapshot(VolumeStatusSnapshotInput{
		CapturedAt: capturedAt,
		Source: SnapshotSource{
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
		Residue: ResidueSnapshot{
			HostInitiator: HostInitiatorResidue{
				ISCSISessions:  []string{"tcp: [1] 127.0.0.1:3260 iqn.2026-05.io.seaweedfs:v1"},
				NVMESubsystems: []string{"nqn.2026-05.io.seaweedfs:v1"},
			},
			Processes:    []string{"blockvolume --volume v1"},
			Kubernetes:   []string{"deploy/sw-blockvolume-v1"},
			StoragePaths: []string{"/var/lib/sw-block/v1.bin"},
		},
	})

	if snap.SchemaVersion != VolumeStatusSnapshotSchemaVersion {
		t.Fatalf("schema_version=%q", snap.SchemaVersion)
	}
	if !snap.CapturedAt.Equal(capturedAt.UTC()) {
		t.Fatalf("captured_at=%s want %s", snap.CapturedAt, capturedAt.UTC())
	}
	if snap.ProductRevision != "abc123" || snap.RunnerRevision != "runner456" {
		t.Fatalf("revision mismatch: product=%q runner=%q", snap.ProductRevision, snap.RunnerRevision)
	}
	if snap.Volume.VolumeID != "v1" || snap.Volume.ReplicaID != "r1" {
		t.Fatalf("volume identity mismatch: %+v", snap.Volume)
	}
	if got, want := snap.Volume.Protocols, []string{"iscsi", "nvme"}; !stringSlicesEqual(got, want) {
		t.Fatalf("protocols=%v want %v", got, want)
	}
	if len(snap.Volume.Frontends) != 2 {
		t.Fatalf("frontends len=%d want 2", len(snap.Volume.Frontends))
	}
	if snap.Volume.Frontends[0].NQN == "" || snap.Volume.Frontends[0].NSID != 1 {
		t.Fatalf("nvme frontend not preserved: %+v", snap.Volume.Frontends[0])
	}
	if snap.Authority.Epoch != 2 || snap.Authority.EndpointVersion != 3 {
		t.Fatalf("authority should prefer local status lineage: %+v", snap.Authority)
	}
	if !snap.Authority.Assigned || snap.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary {
		t.Fatalf("authority assignment/role mismatch: %+v", snap.Authority)
	}
	if !snap.Authority.Healthy || !snap.Authority.FrontendPrimaryReady {
		t.Fatalf("authority readiness mismatch: %+v", snap.Authority)
	}
	if snap.Replication.ReplicationRole != hostvolume.ReplicationRoleNone {
		t.Fatalf("replication_role=%q", snap.Replication.ReplicationRole)
	}
	if len(snap.Replication.Peers) != 1 || !snap.Replication.Peers[0].Healthy {
		t.Fatalf("peer healthy derivation failed: %+v", snap.Replication.Peers)
	}
	if snap.Replication.Peers[0].LastError != Unavailable {
		t.Fatalf("peer last_error=%q want unavailable", snap.Replication.Peers[0].LastError)
	}
	if len(snap.Durable) != 1 || !snap.Durable[0].Latched || !snap.Durable[0].Operational {
		t.Fatalf("durable status mismatch: %+v", snap.Durable)
	}
	if len(snap.Residue.HostInitiator.ISCSISessions) != 1 || len(snap.Residue.HostInitiator.NVMESubsystems) != 1 {
		t.Fatalf("residue mismatch: %+v", snap.Residue)
	}

	if _, err := json.Marshal(snap); err != nil {
		t.Fatalf("snapshot must marshal as JSON: %v", err)
	}
}

func TestBuildVolumeStatusSnapshot_JSONKeepsZeroValuedFrontendIdentity(t *testing.T) {
	snap := BuildVolumeStatusSnapshot(VolumeStatusSnapshotInput{
		MasterStatus: &control.StatusResponse{
			Frontends: []*control.FrontendTarget{
				{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
				{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
			},
		},
	})
	raw, err := json.Marshal(snap)
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

func TestBuildVolumeStatusSnapshot_UsesMasterFrontendFacts(t *testing.T) {
	snap := BuildVolumeStatusSnapshot(VolumeStatusSnapshotInput{
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

	if got := len(snap.Volume.Frontends); got != 1 {
		t.Fatalf("frontends len=%d want 1", got)
	}
	if snap.Volume.Frontends[0].Protocol != "nvme" {
		t.Fatalf("frontend should come from master StatusResponse, got %+v", snap.Volume.Frontends[0])
	}
}

func TestBuildVolumeStatusSnapshot_MarksUnavailableInputsExplicitly(t *testing.T) {
	snap := BuildVolumeStatusSnapshot(VolumeStatusSnapshotInput{})

	if snap.ProductRevision != Unavailable {
		t.Fatalf("product_revision=%q want unavailable", snap.ProductRevision)
	}
	if snap.Source.Component != Unavailable {
		t.Fatalf("source.component=%q want unavailable", snap.Source.Component)
	}
	if snap.Volume.VolumeID != Unavailable || snap.Volume.ReplicaID != Unavailable {
		t.Fatalf("volume identity should be unavailable when not collected: %+v", snap.Volume)
	}
	if snap.Authority.AuthorityRole != Unavailable {
		t.Fatalf("authority_role=%q want unavailable", snap.Authority.AuthorityRole)
	}
	if snap.Replication.ReplicationRole != Unavailable {
		t.Fatalf("replication_role=%q want unavailable", snap.Replication.ReplicationRole)
	}
	if snap.Volume.Protocols == nil {
		t.Fatal("protocols should be an empty JSON array, not nil")
	}
	if snap.Volume.Frontends == nil {
		t.Fatal("frontends should be an empty JSON array, not nil")
	}
	if snap.Replication.Peers == nil {
		t.Fatal("peers should be an empty JSON array, not nil")
	}
	if snap.Durable == nil {
		t.Fatal("durable should be an empty JSON array, not nil")
	}
	if snap.Residue.HostInitiator.ISCSISessions == nil || snap.Residue.HostInitiator.NVMESubsystems == nil ||
		snap.Residue.Processes == nil || snap.Residue.Kubernetes == nil || snap.Residue.StoragePaths == nil {
		t.Fatalf("residue slices should be empty JSON arrays, not nil: %+v", snap.Residue)
	}
}

func TestBuildVolumeStatusSnapshot_CopiesInputSlices(t *testing.T) {
	residue := ResidueSnapshot{
		HostInitiator: HostInitiatorResidue{ISCSISessions: []string{"session-1"}},
		Processes:     []string{"proc-1"},
		Kubernetes:    []string{"deploy-1"},
		StoragePaths:  []string{"/data/v1.bin"},
	}
	snap := BuildVolumeStatusSnapshot(VolumeStatusSnapshotInput{
		Residue: residue,
		Peers:   []replication.ReplicaPeerStatus{{ReplicaID: "r2", State: "degraded"}},
		Durable: []durable.VolumeStatus{{VolumeID: "v1", ReplicaID: "r1"}},
	})

	residue.HostInitiator.ISCSISessions[0] = "mutated"
	residue.Processes[0] = "mutated"
	residue.Kubernetes[0] = "mutated"
	residue.StoragePaths[0] = "mutated"

	if snap.Residue.HostInitiator.ISCSISessions[0] != "session-1" ||
		snap.Residue.Processes[0] != "proc-1" ||
		snap.Residue.Kubernetes[0] != "deploy-1" ||
		snap.Residue.StoragePaths[0] != "/data/v1.bin" {
		t.Fatalf("snapshot kept aliases to residue input: %+v", snap.Residue)
	}
	if snap.Replication.Peers[0].ReplicaID != "r2" || snap.Durable[0].VolumeID != "v1" {
		t.Fatalf("peer/durable copy mismatch: peers=%+v durable=%+v", snap.Replication.Peers, snap.Durable)
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
