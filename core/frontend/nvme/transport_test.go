package nvme_test

import (
	"errors"
	"net"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func TestTargetTransport_DefaultsToTCP(t *testing.T) {
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-07.example.phase118:subsys",
		VolumeID:  "v1",
		Provider: testback.NewStaticProvider(testback.NewRecordingBackend(frontend.Identity{
			VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		})),
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	if addr == "" {
		t.Fatal("Target.Start returned empty address")
	}
	t.Cleanup(func() { _ = tg.Close() })
}

func TestTargetTransport_RDMAIsExplicitlyUnsupported(t *testing.T) {
	tg := nvme.NewTarget(nvme.TargetConfig{
		Transport: nvme.TransportRDMA,
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-07.example.phase118:subsys",
		VolumeID:  "v1",
		Provider: testback.NewStaticProvider(testback.NewRecordingBackend(frontend.Identity{
			VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		})),
	})
	_, err := tg.Start()
	if !errors.Is(err, nvme.ErrTransportUnsupported) {
		t.Fatalf("Target.Start error=%v, want ErrTransportUnsupported", err)
	}
}

func TestTargetTransport_ListenerFactoryReceivesSelectedTransport(t *testing.T) {
	var gotTransport nvme.Transport
	var gotListen string
	tg := nvme.NewTarget(nvme.TargetConfig{
		Transport: nvme.TransportRDMA,
		Listen:    "rdma://mlx5_0:4420",
		SubsysNQN: "nqn.2026-07.example.phase118:subsys",
		VolumeID:  "v1",
		Provider: testback.NewStaticProvider(testback.NewRecordingBackend(frontend.Identity{
			VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		})),
		ListenerFactory: func(transport nvme.Transport, listen string) (net.Listener, error) {
			gotTransport = transport
			gotListen = listen
			return nil, nvme.ErrTransportUnsupported
		},
	})
	_, err := tg.Start()
	if !errors.Is(err, nvme.ErrTransportUnsupported) {
		t.Fatalf("Target.Start error=%v, want ErrTransportUnsupported", err)
	}
	if gotTransport != nvme.TransportRDMA || gotListen != "rdma://mlx5_0:4420" {
		t.Fatalf("factory got transport=%q listen=%q", gotTransport, gotListen)
	}
}
