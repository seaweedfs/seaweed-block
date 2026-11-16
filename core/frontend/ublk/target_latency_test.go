package ublk

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"testing"
)

type latencyView struct{}

func (latencyView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, Healthy: true}
}

type latencyDevice struct{ h handler }

func (d *latencyDevice) Path() string { return "/test-kernel-boundary" }
func (d *latencyDevice) Stats() Stats { return Stats{} }
func (d *latencyDevice) Close() error { return nil }
func TestTargetDurabilityLatencyThroughProviderBackend(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), NumBlocks: 32, BlockSize: 4096}, latencyView{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	tg := NewTarget(TargetConfig{Provider: p, VolumeID: "v1", VolumeSize: 32 * 4096, BlockSize: 4096, DevID: -1})
	var dev *latencyDevice
	tg.startDevice = func(cfg Config) (targetDevice, error) {
		// Only the kernel constructor is replaced; Target.Start must select the
		// actual production providerBackend and pass it through this Config.
		if _, ok := cfg.Backend.(*providerBackend); !ok {
			t.Fatalf("wrong production backend %T", cfg.Backend)
		}
		dev = &latencyDevice{h: handler{backend: cfg.Backend, size: cfg.Size}}
		return dev, nil
	}
	if _, err := tg.Start(); err != nil {
		t.Fatal(err)
	}
	defer tg.Close()
	if n := dev.h.handle(context.Background(), request{op: ioOpWrite, flags: ioFlagFUA, nrSectors: 8}, make([]byte, 4096)); n != 4096 {
		t.Fatal(n)
	}
	if n := dev.h.handle(context.Background(), request{op: ioOpFlush}, nil); n != 0 {
		t.Fatal(n)
	}
	statuses := p.DurableStatuses()
	if len(statuses) != 1 {
		t.Fatal(statuses)
	}
	s := statuses[0].WriteProfile.DurabilityLatency
	if s.Frontend.Writes != 1 || s.Frontend.FUAWrites != 1 || s.Frontend.Flushes != 1 || s.Frontend.RequestTotal.Count != 2 || s.Frontend.ReceiveToBackend.Count != 2 || s.Frontend.AckDispatch.Count != 2 || s.Frontend.FUATotal.Count != 1 || s.Frontend.FlushTotal.Count != 1 {
		t.Fatalf("production Target frontend stages missing: %+v", s.Frontend)
	}
	if s.WAL == nil || s.WAL.FileSync.Count == 0 || s.BackendSync.Count != 2 {
		t.Fatal("storage stages missing", s)
	}
}
