package durable_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestDurabilityLatencyStatusPresent(t *testing.T) {
	b, _, _ := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	if _, err := b.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(b.WriteProfile())
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["DurabilityLatency"]; !ok {
		t.Fatalf("durability stage histograms missing from existing WriteProfile status: %s", data)
	}
}

func TestDurabilityLatencyNVMeRequests(t *testing.T) {
	b, _, _ := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: b, BlockSize: 4096, VolumeSize: 16 * 4096, NSID: 1})
	for _, cmd := range []nvme.IOCommand{
		{Opcode: 1, NSID: 1, NLB: 1, Data: make([]byte, 4096), FUA: true},
		{Opcode: 0, NSID: 1},
		{Opcode: 1, NSID: 1, SLBA: 99, NLB: 1, Data: make([]byte, 4096)},
	} {
		h.Handle(context.Background(), cmd)
	}
	s := b.WriteProfile().DurabilityLatency
	if s.Frontend.Writes != 2 || s.Frontend.FUAWrites != 1 || s.Frontend.Flushes != 1 || s.Frontend.Failed != 1 || s.Frontend.RequestTotal.Count != 3 || s.Frontend.ReceiveToBackend.Count != 2 || s.Frontend.AckDispatch.Count != 2 || s.BackendSync.Count != 2 {
		t.Fatal(s)
	}
	if s.WAL == nil || s.WAL.WALAppend.Count == 0 || s.WAL.FileSync.Count == 0 {
		t.Fatal("missing WAL stages", s)
	}
}

func TestDurabilityLatencyChunkProviderStatus(t *testing.T) {
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), 0)
	openRecovered(t, p)
	if _, err := p.Open(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	b := p.Backend("v1")
	if _, err := b.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	statuses := p.DurableStatuses()
	if len(statuses) != 1 {
		t.Fatal(statuses)
	}
	l := statuses[0].WriteProfile.DurabilityLatency
	if l.WAL == nil || l.WAL.WALAppend.Count == 0 || l.WAL.GroupCommitWait.Count == 0 || l.WAL.FileSync.Count == 0 || l.BackendSync.Count == 0 {
		t.Fatal(l)
	}
}
