package main

import (
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

type aluaProjector struct{ p engine.ReplicaProjection }

func (p aluaProjector) Projection() engine.ReplicaProjection { return p.p }

type aluaSupersedeProbe struct{ yes bool }

func (p aluaSupersedeProbe) IsSuperseded(string, uint64, uint64) bool { return p.yes }

func TestProjectionALUAProvider_StateMapping(t *testing.T) {
	tests := []struct {
		name       string
		mode       engine.Mode
		superseded bool
		want       iscsi.ALUAState
	}{
		{name: "healthy", mode: engine.ModeHealthy, want: iscsi.ALUAActiveOptimized},
		{name: "superseded healthy", mode: engine.ModeHealthy, superseded: true, want: iscsi.ALUAStandby},
		{name: "recovering", mode: engine.ModeRecovering, want: iscsi.ALUATransitioning},
		{name: "degraded", mode: engine.ModeDegraded, want: iscsi.ALUAUnavailable},
		{name: "idle", mode: engine.ModeIdle, want: iscsi.ALUAUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := volume.NewAdapterProjectionView(
				aluaProjector{p: engine.ReplicaProjection{Mode: tt.mode, Epoch: 1, EndpointVersion: 1}},
				"v1",
				"r1",
				aluaSupersedeProbe{yes: tt.superseded},
			)
			prov := newProjectionALUAProvider(view, "v1", "r1")
			if got := prov.ALUAState(); got != tt.want {
				t.Fatalf("ALUAState()=%s want %s", got, tt.want)
			}
		})
	}
}

func TestProjectionALUAProvider_IdentityIsStableAndPathSpecific(t *testing.T) {
	view1 := volume.NewAdapterProjectionView(
		aluaProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1",
		"r1",
		nil,
	)
	view2 := volume.NewAdapterProjectionView(
		aluaProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1",
		"r2",
		nil,
	)
	r1 := newProjectionALUAProvider(view1, "v1", "r1")
	r2 := newProjectionALUAProvider(view2, "v1", "r2")

	if got := r1.DeviceNAA()[0] & 0xf0; got != 0x60 {
		t.Fatalf("DeviceNAA high nibble=%#x want 0x60", got)
	}
	if r1.DeviceNAA() != r2.DeviceNAA() {
		t.Fatalf("same volume must expose same DeviceNAA: r1=%x r2=%x", r1.DeviceNAA(), r2.DeviceNAA())
	}
	if r1.TargetPortGroupID() == r2.TargetPortGroupID() {
		t.Fatalf("replica paths should have distinct target port groups: %d", r1.TargetPortGroupID())
	}
	if r1.RelativeTargetPortID() == r2.RelativeTargetPortID() {
		t.Fatalf("replica paths should have distinct relative target port ids: %d", r1.RelativeTargetPortID())
	}
}

func TestProjectionALUAProvider_FailsClosedOnIdentityMismatch(t *testing.T) {
	view := volume.NewAdapterProjectionView(
		aluaProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1",
		"r1",
		nil,
	)
	prov := newProjectionALUAProvider(view, "v1", "r2")
	if got := prov.ALUAState(); got != iscsi.ALUAUnavailable {
		t.Fatalf("identity mismatch ALUAState()=%s want %s", got, iscsi.ALUAUnavailable)
	}
}

func TestProjectionALUAProvider_DoesNotImportControlTruthDomains(t *testing.T) {
	body, err := os.ReadFile("iscsi_alua_provider.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"core/authority", "core/lifecycle", "core/rpc/control"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("projection ALUA provider must not import %s", forbidden)
		}
	}
}
