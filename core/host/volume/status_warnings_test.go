package volume

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// Issue #51 (walstore interim): the durable store's typed recovery warning
// is visible on /status and does not change Healthy or readiness.
func TestStatusServer_StatusCarriesRecoveryWarningsWithoutChangingHealth(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
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
