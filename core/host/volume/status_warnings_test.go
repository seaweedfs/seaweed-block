package volume

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// Issue #51 (walstore interim): the durable store's typed recovery warning
// is visible on /status and does not change Healthy or readiness.
func TestStatusServer_StatusCarriesRecoveryWarningsWithoutChangingHealth(t *testing.T) {
	t.Run("not-ready replica", func(t *testing.T) {
		statusWarningsCase(t, NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil), false)
	})
	t.Run("healthy primary", func(t *testing.T) {
		// codex01's advisory: a healthy primary stays healthy with a warning.
		p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1}}
		statusWarningsCase(t, NewAdapterProjectionView(p, "v1", "r1", stubProbe{yes: false}), true)
	})
}

func statusWarningsCase(t *testing.T, view *AdapterProjectionView, wantHealthy bool) {
	s := NewStatusServer(view)
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	get := func() StatusProjection {
		t.Helper()
		resp, err := http.Get("http://" + addr + "/status?volume=v1")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var p StatusProjection
		if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	before := get()
	if before.Healthy != wantHealthy || before.FrontendPrimaryReady != wantHealthy {
		t.Fatalf("before the warning: healthy=%t ready=%t; want %t", before.Healthy, before.FrontendPrimaryReady, wantHealthy)
	}
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{
		VolumeID: "v1", Impl: "walstore", Warnings: []string{"wal_hole_suspected: WAL entry at offset=4134 fails its check"}, HolesSuspected: 1,
	}}})
	after := get()
	if len(after.Warnings) != 1 || after.Warnings[0][:18] != "wal_hole_suspected" {
		t.Fatalf("/status warnings %v; want the wal_hole_suspected warning", after.Warnings)
	}
	if after.Healthy != before.Healthy || after.FrontendPrimaryReady != before.FrontendPrimaryReady {
		t.Fatalf("a warning changed health: before healthy=%t ready=%t, after healthy=%t ready=%t",
			before.Healthy, before.FrontendPrimaryReady, after.Healthy, after.FrontendPrimaryReady)
	}
}
