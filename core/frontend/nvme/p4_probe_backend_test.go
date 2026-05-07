package nvme_test

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type p4NotReadyProvider struct{}

func (p4NotReadyProvider) Open(context.Context, string) (frontend.Backend, error) {
	return nil, frontend.ErrNotReady
}

type p4ProbeProvider struct {
	backend frontend.Backend
	opens   atomic.Int32
}

func (p *p4ProbeProvider) ProbeBackend(context.Context, string) (frontend.Backend, error) {
	p.opens.Add(1)
	return p.backend, nil
}

func newP4StandbyProbeHarness(t *testing.T) (*testback.RecordingBackend, *p4ProbeProvider, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 1, EndpointVersion: 1,
	})
	probe := &p4ProbeProvider{backend: rec}
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:        "127.0.0.1:0",
		SubsysNQN:     "nqn.2026-04.example.v3:subsys",
		VolumeID:      "v1",
		Provider:      p4NotReadyProvider{},
		ProbeProvider: probe,
		Handler: nvme.HandlerConfig{
			ANA: testANAProvider{
				state:       nvme.ANANonOptimized,
				groupID:     1,
				changeCount: 7,
			},
		},
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	t.Cleanup(func() { _ = tg.Close() })
	cli := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	t.Cleanup(cli.close)
	return rec, probe, cli
}

func TestP4NVMe_StandbyPathUsesProbeBackendForAdminMetadata(t *testing.T) {
	_, probe, cli := newP4StandbyProbeHarness(t)
	if got := probe.opens.Load(); got != 1 {
		t.Fatalf("ProbeBackend opens=%d want 1 after admin connect", got)
	}

	status, ctrl := cli.adminIdentify(t, 0x01, 0)
	expectStatusSuccess(t, status, "standby Identify Controller")
	if got := ctrl[76]; got != 0x0a {
		t.Fatalf("CMIC=0x%02x want multi-controller + ANA reporting bits", got)
	}

	status, ns := cli.adminIdentify(t, 0x00, 1)
	expectStatusSuccess(t, status, "standby Identify Namespace")
	if got := binary.LittleEndian.Uint32(ns[92:96]); got != 1 {
		t.Fatalf("Identify NS ANAGRPID=%d want 1", got)
	}

	status, logData := cli.adminGetLogPage(t, 0x0C, 9)
	expectStatusSuccess(t, status, "standby ANA log")
	if got := logData[32]; got != byte(nvme.ANANonOptimized) {
		t.Fatalf("ANA state=0x%02x want non-optimized", got)
	}
}

func TestP4NVMe_StandbyProbePathRejectsDataCommands(t *testing.T) {
	rec, probe, adminOnly := newP4StandbyProbeHarness(t)
	adminOnly.close()

	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:        "127.0.0.1:0",
		SubsysNQN:     "nqn.2026-04.example.v3:subsys",
		VolumeID:      "v1",
		Provider:      p4NotReadyProvider{},
		ProbeProvider: probe,
		Handler: nvme.HandlerConfig{
			ANA: testANAProvider{
				state:       nvme.ANANonOptimized,
				groupID:     1,
				changeCount: 7,
			},
		},
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndConnect(t, addr)
	defer cli.close()
	status := cli.writeCmd(t, 0, 1, make([]byte, nvme.DefaultBlockSize))
	gotSCT := uint8((status >> 9) & 0x7)
	gotSC := uint8((status >> 1) & 0xff)
	if gotSCT != nvme.SCTPathRelated || gotSC != nvme.SCPathAsymAccessInaccessible {
		t.Fatalf("standby write status=0x%04x SCT=%d SC=0x%02x want Path/Inaccessible",
			status, gotSCT, gotSC)
	}
	if rec.WriteCount() != 0 {
		t.Fatalf("standby write reached probe backend; writes=%d", rec.WriteCount())
	}
}

func TestP4NVMe_ConfiguredControllerIDIsAdvertisedOnConnect(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 1, EndpointVersion: 1,
	})
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:       "127.0.0.1:0",
		SubsysNQN:    "nqn.2026-04.example.v3:subsys",
		VolumeID:     "v1",
		Provider:     testback.NewStaticProvider(rec),
		ControllerID: 2,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	defer cli.close()
	if cli.cntlID != 2 {
		t.Fatalf("admin Connect CNTLID=%d want configured controller id 2", cli.cntlID)
	}
}
