package main

import (
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

func TestProjectionANAProvider_StateMapping(t *testing.T) {
	tests := []struct {
		name       string
		mode       engine.Mode
		superseded bool
		want       nvme.ANAState
	}{
		{name: "healthy", mode: engine.ModeHealthy, want: nvme.ANAOptimized},
		{name: "superseded healthy", mode: engine.ModeHealthy, superseded: true, want: nvme.ANANonOptimized},
		{name: "recovering", mode: engine.ModeRecovering, want: nvme.ANAChange},
		{name: "degraded", mode: engine.ModeDegraded, want: nvme.ANAInaccessible},
		{name: "idle", mode: engine.ModeIdle, want: nvme.ANANonOptimized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := volume.NewAdapterProjectionView(
				aluaProjector{p: engine.ReplicaProjection{Mode: tt.mode, Epoch: 1, EndpointVersion: 2}},
				"v1",
				"r1",
				aluaSupersedeProbe{yes: tt.superseded},
			)
			prov := newProjectionANAProvider(view, "v1", "r1")
			if got := prov.ANAState(); got != tt.want {
				t.Fatalf("ANAState()=%#x want %#x", got, tt.want)
			}
		})
	}
}

func TestProjectionANAProvider_GroupIDIsDenseAndWithinAdvertisedRange(t *testing.T) {
	view1 := volume.NewAdapterProjectionView(
		aluaProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1",
		"r1",
		nil,
	)
	r1a := newProjectionANAProvider(view1, "v1", "r1", "nqn.2026-05.io.seaweedfs:v1")
	r1b := newProjectionANAProvider(view1, "v1", "r1", "nqn.2026-05.io.seaweedfs:v1")

	if got := r1a.ANAGroupID(); got != 1 {
		t.Fatalf("ANAGroupID()=%d want 1 for single-group ANA", got)
	}
	if r1a.ANAGroupID() != r1b.ANAGroupID() {
		t.Fatalf("same replica produced unstable ANA group: %d vs %d", r1a.ANAGroupID(), r1b.ANAGroupID())
	}
}

func TestProjectionANAProvider_ChangeCountTracksLineage(t *testing.T) {
	view := volume.NewAdapterProjectionView(
		aluaProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 3, EndpointVersion: 7}},
		"v1",
		"r1",
		nil,
	)
	prov := newProjectionANAProvider(view, "v1", "r1")

	want := uint64(3)<<32 | 7
	if got := prov.ANAChangeCount(); got != want {
		t.Fatalf("ANAChangeCount()=%d want %d", got, want)
	}
}

func TestProjectionANAProvider_FailsClosedOnIdentityMismatch(t *testing.T) {
	view := volume.NewAdapterProjectionView(
		aluaProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1",
		"r1",
		nil,
	)
	prov := newProjectionANAProvider(view, "v1", "r2")
	if got := prov.ANAState(); got != nvme.ANAInaccessible {
		t.Fatalf("identity mismatch ANAState()=%#x want %#x", got, nvme.ANAInaccessible)
	}
}

func TestProjectionANAProvider_DoesNotImportControlTruthDomains(t *testing.T) {
	body, err := os.ReadFile("nvme_ana_provider.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"core/authority", "core/lifecycle", "core/rpc/control"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("projection ANA provider must not import %s", forbidden)
		}
	}
}
