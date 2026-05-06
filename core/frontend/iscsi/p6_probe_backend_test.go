package iscsi_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type p6ProbeProvider struct {
	backend frontend.Backend
	opens   atomic.Int32
}

func (p *p6ProbeProvider) ProbeBackend(context.Context, string) (frontend.Backend, error) {
	p.opens.Add(1)
	return p.backend, nil
}

type immediateNotReadyProvider struct{}

func (p immediateNotReadyProvider) Open(context.Context, string) (frontend.Backend, error) {
	return nil, frontend.ErrNotReady
}

func TestP6ALUA_NormalSessionUsesProbeBackendForStandbyPath(t *testing.T) {
	primaryOpen := immediateNotReadyProvider{}
	probeBackend := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 1, EndpointVersion: 1,
	})
	probe := &p6ProbeProvider{backend: probeBackend}
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:        "127.0.0.1:0",
		IQN:           "iqn.2026-05.io.seaweedfs:alua-standby",
		VolumeID:      "v1",
		Provider:      primaryOpen,
		ProbeProvider: probe,
		Handler: iscsi.HandlerConfig{
			ALUA: aluaTestProvider(iscsi.ALUAStandby),
		},
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLoginOpts(t, addr, loginOptions{
		TargetName: "iqn.2026-05.io.seaweedfs:alua-standby",
	})
	defer func() { _ = cli.conn.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	for probe.opens.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := probe.opens.Load(); got != 1 {
		t.Fatalf("ProbeBackend opens=%d want 1", got)
	}

	var inquiry [16]byte
	inquiry[0] = iscsi.ScsiInquiry
	inquiry[4] = 96
	status, data := cli.scsiCmd(t, inquiry, nil, 96)
	expectGood(t, status, "standby inquiry")
	if len(data) == 0 || data[5]&0x10 == 0 {
		t.Fatalf("standby inquiry missing implicit ALUA TPGS bit: len=%d byte5=0x%02x", len(data), data[5])
	}

	var rtpg [16]byte
	rtpg[0] = iscsi.ScsiMaintenanceIn
	rtpg[1] = iscsi.SaiReportTargetPortGroups
	rtpg[9] = 16
	status, data = cli.scsiCmd(t, rtpg, nil, 16)
	expectGood(t, status, "standby RTPG")
	if got := iscsi.ALUAState(data[4] & 0x0f); got != iscsi.ALUAStandby {
		t.Fatalf("RTPG ALUA state=%s want %s", got, iscsi.ALUAStandby)
	}

	status, _ = cli.scsiCmd(t, writeCDB10(0, 1), make([]byte, 512), 0)
	expectCheckCondition(t, status, "standby write")
	if probeBackend.WriteCount() != 0 {
		t.Fatalf("standby write reached backend; writes=%d", probeBackend.WriteCount())
	}
}
