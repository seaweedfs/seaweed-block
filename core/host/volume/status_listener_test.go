package volume

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

func listenerStatus(t *testing.T) (*StatusServer, *AdapterProjectionView, string) {
	t.Helper()
	v := NewAdapterProjectionView(stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3}}, "v1", "r1", nil)
	s := NewStatusServer(v)
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s, v, addr
}

func TestNVMeStatusPendingDoesNotBlockRuntimeRecovery(t *testing.T) {
	for _, phase := range []string{"pending", "stopping"} {
		t.Run(phase, func(t *testing.T) {
			v := NewAdapterProjectionView(stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3}}, "v1", "r1", nil)
			s := NewStatusServer(v)
			s.EnableRuntimeEndpoint()
			src := &fakeRuntimeRecoverySource{}
			s.SetRuntimeRecoverySource(src)
			if phase == "stopping" {
				s.SetNVMeListenerReady(true)
			}
			s.SetNVMeListenerReady(false)
			addr, err := s.Start("127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close(context.Background())
			if p := readListenerStatus(t, addr); p.Healthy || p.FrontendPrimaryReady {
				t.Fatal("presentation is ready while listener is pending", p)
			}
			c := http.Client{Timeout: time.Second}
			resp, err := c.Post("http://"+addr+"/runtime/rebuild", "application/json", strings.NewReader(`{"volumeID":"v1","replicaID":"r2","targetDataAddr":"127.0.0.1:19103","sessionID":1001,"epoch":5,"endpointVersion":3,"frontierHintLsn":90}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("NVMe %s masked runtime recovery: HTTP %d", phase, resp.StatusCode)
			}
			if len(src.requests) != 1 || src.requests[0].ReplicaID != "r2" || src.requests[0].Epoch != 5 || src.requests[0].SessionID != 1001 {
				t.Fatal("recovery did not receive exact request", src.requests)
			}
		})
	}
}

func readListenerStatus(t *testing.T, addr string) StatusProjection {
	t.Helper()
	c := http.Client{Timeout: time.Second}
	r, err := c.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var p StatusProjection
	if err = json.NewDecoder(r.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNVMeStatusWaitsForHeldListenerBind(t *testing.T) {
	s, v, addr := listenerStatus(t)
	s.SetNVMeListenerReady(false)
	arrived, release := make(chan struct{}), make(chan struct{})
	target := nvme.NewTarget(nvme.TargetConfig{Listen: "127.0.0.1:0", VolumeID: "v1", SubsysNQN: "nqn.2026-11.test:readiness", Provider: memback.NewProvider(v), ListenerFactory: func(_ nvme.Transport, address string) (net.Listener, error) {
		close(arrived)
		<-release
		return net.Listen("tcp", address)
	}})
	t.Cleanup(func() { _ = target.Close() })
	type result struct {
		addr string
		err  error
	}
	done := make(chan result, 1)
	go func() { a, e := target.Start(); done <- result{a, e} }()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("listener did not reach held bind")
	}
	p := readListenerStatus(t, addr)
	if p.Healthy || p.FrontendPrimaryReady {
		t.Error("status ready before configured NVMe listener bound")
	}
	if p.AuthorityRole != AuthorityRolePrimary || p.Epoch != 5 || !v.Projection().Healthy {
		t.Error("listener wait changed storage/authority", p)
	}
	close(release)
	var started result
	select {
	case started = <-done:
	case <-time.After(time.Second):
		t.Fatal("listener did not start")
	}
	if started.err != nil {
		t.Fatal(started.err)
	}
	c, err := net.DialTimeout("tcp", started.addr, time.Second)
	if err != nil {
		t.Fatal("started listener does not accept", err)
	}
	_ = c.Close()
	s.SetNVMeListenerReady(true)
	p = readListenerStatus(t, addr)
	if !p.Healthy || !p.FrontendPrimaryReady {
		t.Fatal("bound listener did not become ready", p)
	}
	s.SetNVMeListenerReady(false)
	if p = readListenerStatus(t, addr); p.Healthy || p.FrontendPrimaryReady {
		t.Fatal("listener shutdown remained ready", p)
	}
}

func TestNVMeStatusBindFailureStaysUnreadyAndAbsentListenerUnchanged(t *testing.T) {
	s, v, addr := listenerStatus(t)
	if p := readListenerStatus(t, addr); !p.Healthy || !p.FrontendPrimaryReady {
		t.Fatal("no configured listener changed legacy status", p)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	s.SetNVMeListenerReady(false)
	target := nvme.NewTarget(nvme.TargetConfig{Listen: occupied.Addr().String(), VolumeID: "v1", Provider: memback.NewProvider(v)})
	defer target.Close()
	if _, err = target.Start(); err == nil {
		t.Fatal("fixture unexpectedly bound occupied address")
	}
	if p := readListenerStatus(t, addr); p.Healthy || p.FrontendPrimaryReady {
		t.Fatal("failed bind reported ready", p)
	}
}
