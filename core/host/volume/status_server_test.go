// Ownership: sw regression test (architect finding 2026-04-21:
// status HTTP must reject non-loopback bind and non-loopback
// client to avoid shipping an unauthenticated diagnostic API).
package volume

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
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
