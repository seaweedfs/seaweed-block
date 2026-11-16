package volume

import (
	"context"
	"encoding/json"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/iolatency"
	"net/http"
	"testing"
)

func TestDurabilityLatencyExistingHTTPEndpoint(t *testing.T) {
	var h iolatency.Histogram
	h.Observe(100)
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{VolumeID: "v1", WriteProfile: durable.WriteProfileStatus{DurabilityLatency: durable.DurabilityLatencyStatus{BackendSync: h.Snapshot()}}}}})
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	r, err := http.Get("http://" + addr + "/status/durable?volume=v1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var body struct{ Volumes []durable.VolumeStatus }
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != 200 || len(body.Volumes) != 1 || body.Volumes[0].WriteProfile.DurabilityLatency.BackendSync.Count != 1 || body.Volumes[0].WriteProfile.DurabilityLatency.BackendSync.P99UpperNanos != 128 {
		t.Fatal(body)
	}
}
