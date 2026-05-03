// Ownership: sw regression test (architect finding 2026-04-21:
// status HTTP must reject non-loopback bind and non-loopback
// client to avoid shipping an unauthenticated diagnostic API).
package volume

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestStatusServer_RejectsNonLoopbackBind(t *testing.T) {
	cases := []string{
		"0.0.0.0:0",
		"8.8.8.8:0",
		":0", // empty host = all interfaces
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
			if _, err := s.Start(addr); err == nil {
				_ = s.Close(context.Background())
				t.Fatalf("bind %q must be rejected", addr)
			} else if !strings.Contains(err.Error(), "loopback") {
				t.Fatalf("bind %q rejected but error does not mention loopback: %v", addr, err)
			}
		})
	}
}

func TestStatusServer_AcceptsLoopbackBindAndServesJSON(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("loopback bind: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.Close(ctx)
		cancel()
	}()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("loopback client status: %d want 200", resp.StatusCode)
	}
}

func TestG9A_StatusProjection_ReturnedOldPrimarySupersededNotReady(t *testing.T) {
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           2,
		EndpointVersion: 1,
	})

	s := NewStatusServer(h.view)
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var body StatusProjection
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Healthy {
		t.Fatalf("returned old primary must fail closed: %+v", body)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("returned old primary must not be frontend-primary-ready: %+v", body)
	}
	if body.AuthorityRole != AuthorityRoleSuperseded {
		t.Fatalf("authority role = %q want %q", body.AuthorityRole, AuthorityRoleSuperseded)
	}
	if body.ReplicationRole != ReplicationRoleNotReady {
		t.Fatalf("replication role = %q want %q", body.ReplicationRole, ReplicationRoleNotReady)
	}
}

func TestStatusServer_MissingVolume_Returns400(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("missing volume: got %d want 400", resp.StatusCode)
	}
}

func TestStatusServer_WrongVolume_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=otherVol")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("wrong volume: got %d want 404", resp.StatusCode)
	}
}

func TestIsLoopbackRemote(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:34567": true,
		"[::1]:80":        true,
		"8.8.8.8:80":      false,
		"":                false,
		"garbage":         false,
	}
	for in, want := range cases {
		if got := isLoopbackRemote(in); got != want {
			t.Errorf("isLoopbackRemote(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestStatusServer_RecoveryEndpoint_Disabled_Returns404 — G5-5
// regression: /status/recovery is OPT-IN. Default config (no
// EnableRecoveryEndpoint call) must NOT route the path.
func TestStatusServer_RecoveryEndpoint_Disabled_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(
		stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/recovery?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("disabled recovery endpoint: got %d want 404 (mux must not route)", resp.StatusCode)
	}
}

// TestStatusServer_RecoveryEndpoint_Enabled_ReturnsEngineProjection
// — pins that EnableRecoveryEndpoint exposes the engine projection
// over HTTP with R/S/H + Mode + RecoveryDecision visible (the fields
// G5-5 hardware-test catch-up verification reads).
func TestStatusServer_RecoveryEndpoint_Enabled_ReturnsEngineProjection(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{
		Mode:             engine.ModeRecovering,
		RecoveryDecision: "catch_up",
		R:                42,
		S:                10,
		H:                100,
		Epoch:            7,
		EndpointVersion:  3,
	}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	s.EnableRecoveryEndpoint()
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/recovery?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("enabled recovery: got %d want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, want := range []string{"R", "S", "H", "Mode", "RecoveryDecision", "Epoch", "EndpointVersion"} {
		if _, ok := body[want]; !ok {
			t.Errorf("recovery body missing field %q (need for G5-5 catch-up verifier); body=%v", want, body)
		}
	}
	if got, _ := body["R"].(float64); got != 42 {
		t.Errorf("R: got %v want 42", body["R"])
	}
	if got, _ := body["H"].(float64); got != 100 {
		t.Errorf("H: got %v want 100", body["H"])
	}
}

// TestStatusServer_RecoveryEndpoint_WrongVolume_Returns404 — pins
// that the wrong-volume guard fires the same way as /status.
func TestStatusServer_RecoveryEndpoint_WrongVolume_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	s.EnableRecoveryEndpoint()
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/recovery?volume=other")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("wrong volume: got %d want 404", resp.StatusCode)
	}
}
