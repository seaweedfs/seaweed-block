package volume

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
)

func TestHost_SetFrontendTargets_IncludedInHeartbeat(t *testing.T) {
	h := newTestVolumeHost(t)
	defer func() { _ = h.Close() }()

	targets := []*control.FrontendTarget{{
		Protocol: "iscsi",
		Addr:     "127.0.0.1:3260",
		Iqn:      "iqn.2026-05.example:v1",
		Lun:      7,
	}}
	h.SetFrontendTargets(targets)
	targets[0].Addr = "mutated:3260"

	report := h.buildReport()
	if len(report.GetSlots()) != 1 {
		t.Fatalf("slots=%d want 1", len(report.GetSlots()))
	}
	got := report.GetSlots()[0].GetFrontends()
	if len(got) != 1 {
		t.Fatalf("frontends=%d want 1", len(got))
	}
	if got[0].GetProtocol() != "iscsi" || got[0].GetAddr() != "127.0.0.1:3260" || got[0].GetIqn() != "iqn.2026-05.example:v1" {
		t.Fatalf("frontend target copied incorrectly: %+v", got[0])
	}
	if got[0].GetLun() != 7 {
		t.Fatalf("lun=%d want 7", got[0].GetLun())
	}
}

func TestHost_SetFrontendTargets_ReplacesPreviousSet(t *testing.T) {
	h := newTestVolumeHost(t)
	defer func() { _ = h.Close() }()

	h.SetFrontendTargets([]*control.FrontendTarget{{
		Protocol: "iscsi",
		Addr:     "127.0.0.1:3260",
		Iqn:      "iqn.old:v1",
	}})
	h.SetFrontendTargets([]*control.FrontendTarget{{
		Protocol: "nvme",
		Addr:     "127.0.0.1:4420",
		Nqn:      "nqn.2026-05.io.seaweedfs:v1",
		Nsid:     1,
	}})

	got := h.buildReport().GetSlots()[0].GetFrontends()
	if len(got) != 1 {
		t.Fatalf("frontends=%d want replacement singleton", len(got))
	}
	if got[0].GetProtocol() != "nvme" || got[0].GetNqn() == "" {
		t.Fatalf("replacement frontend not reflected: %+v", got[0])
	}
}

func TestHost_SetSnapshotRuntimeEndpoint_IncludedInHeartbeat(t *testing.T) {
	h := newTestVolumeHost(t)
	defer func() { _ = h.Close() }()

	h.SetSnapshotRuntimeEndpoint("https://10.0.0.2:24443")
	got := h.buildReport().GetSlots()[0].GetSnapshotRuntimeEndpoint()
	if got != "https://10.0.0.2:24443" {
		t.Fatalf("snapshot runtime endpoint=%q", got)
	}
}

func TestHost_SetSnapshotRestoreEvidenceSource_IncludedInHeartbeat(t *testing.T) {
	h := newTestVolumeHost(t)
	defer func() { _ = h.Close() }()

	source := &testRestoreEvidenceSource{marker: snapshot.RestoreMarker{
		SnapshotID:      "snap-a",
		State:           snapshot.RestoreStateApplied,
		TargetStorageID: "store-a",
		TargetNumBlocks: 256,
		TargetBlockSize: 4096,
	}}
	h.SetSnapshotRestoreEvidenceSource(source)
	got := h.buildReport().GetSlots()[0].GetSnapshotRestore()
	if got.GetSnapshotId() != "snap-a" || got.GetState() != snapshot.RestoreStateApplied || got.GetStorageId() != "store-a" || got.GetNumBlocks() != 256 || got.GetBlockSize() != 4096 {
		t.Fatalf("snapshot restore evidence=%+v", got)
	}
	source.marker.State = snapshot.RestoreStateActivated
	if state := h.buildReport().GetSlots()[0].GetSnapshotRestore().GetState(); state != snapshot.RestoreStateActivated {
		t.Fatalf("snapshot restore state=%q", state)
	}
}

type testRestoreEvidenceSource struct {
	marker snapshot.RestoreMarker
}

func (s *testRestoreEvidenceSource) Marker() snapshot.RestoreMarker { return s.marker }

func TestHost_BuildReport_NotReadyForPrimaryFromHeartbeatAlone(t *testing.T) {
	h := newTestVolumeHost(t)
	defer func() { _ = h.Close() }()

	slot := h.buildReport().GetSlots()[0]
	if slot.GetReadyForPrimary() {
		t.Fatalf("fresh observed process must not be promotion-ready from heartbeat alone: %+v", slot)
	}
}

func TestHost_BuildReport_SupportingReplicaReadyCanBePromotionCandidate(t *testing.T) {
	h := &Host{
		cfg: Config{
			ServerID:  "node-a",
			VolumeID:  "v1",
			ReplicaID: "r2",
			DataAddr:  "127.0.0.1:9202",
			CtrlAddr:  "127.0.0.1:9102",
		},
	}
	h.view = NewAdapterProjectionView(
		stubProjector{
			p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1},
			ready: &engine.PromotionReadyFact{
				Ready:           true,
				Reason:          engine.PromotionReadyReasonReady,
				ReplicaID:       "r2",
				Epoch:           1,
				EndpointVersion: 1,
			},
		},
		"v1",
		"r2",
		stubProbe{other: &control.AssignmentFact{VolumeId: "v1", ReplicaId: "r1", Epoch: 1, EndpointVersion: 1}},
	)

	if h.view.Projection().Healthy {
		t.Fatal("supporting replica must remain frontend-unhealthy before authority moves")
	}
	slot := h.buildReport().GetSlots()[0]
	if !slot.GetReadyForPrimary() {
		t.Fatalf("caught-up supporting replica should be a promotion candidate: %+v", slot)
	}
}

func TestHost_BuildReport_SupersededReplicaNotReadyForPrimary(t *testing.T) {
	h := &Host{
		cfg: Config{
			ServerID:  "node-a",
			VolumeID:  "v1",
			ReplicaID: "r1",
			DataAddr:  "127.0.0.1:9201",
			CtrlAddr:  "127.0.0.1:9101",
		},
	}
	h.view = NewAdapterProjectionView(
		stubProjector{
			p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1},
			ready: &engine.PromotionReadyFact{
				Ready:           true,
				Reason:          engine.PromotionReadyReasonReady,
				ReplicaID:       "r1",
				Epoch:           1,
				EndpointVersion: 1,
			},
		},
		"v1",
		"r1",
		stubProbe{yes: true},
	)

	slot := h.buildReport().GetSlots()[0]
	if slot.GetReadyForPrimary() {
		t.Fatalf("superseded replica must not be re-promoted from stale healthy projection: %+v", slot)
	}
}

func newTestVolumeHost(t *testing.T) *Host {
	t.Helper()
	h, err := New(Config{
		MasterAddr: "127.0.0.1:1",
		ServerID:   "node-a",
		VolumeID:   "v1",
		ReplicaID:  "r1",
		DataAddr:   "127.0.0.1:9201",
		CtrlAddr:   "127.0.0.1:9101",
	})
	if err != nil {
		t.Fatalf("volume.New: %v", err)
	}
	return h
}
