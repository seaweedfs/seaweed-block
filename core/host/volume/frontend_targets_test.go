package volume

import (
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
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
