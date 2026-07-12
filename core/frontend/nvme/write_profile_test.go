package nvme_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type writeProfileBackend struct {
	*testback.RecordingBackend
	ops           atomic.Uint64
	bytes         atomic.Uint64
	durationNanos atomic.Uint64
}

func (b *writeProfileBackend) RecordTargetWrite(bytes int, d time.Duration) {
	b.ops.Add(1)
	b.bytes.Add(uint64(bytes))
	nanos := d.Nanoseconds()
	if nanos <= 0 {
		nanos = 1
	}
	b.durationNanos.Add(uint64(nanos))
}

func TestNVMeWriteProfile_RecordsSuccessfulTargetWrite(t *testing.T) {
	backend := &writeProfileBackend{RecordingBackend: testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})}
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: backend})

	payload := make([]byte, nvme.DefaultBlockSize)
	res := h.Handle(context.Background(), nvme.IOCommand{
		Opcode: opWrite,
		NSID:   1,
		SLBA:   0,
		NLB:    1,
		Data:   payload,
	})
	if err := res.AsError(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if backend.ops.Load() != 1 {
		t.Fatalf("target write ops=%d want 1", backend.ops.Load())
	}
	if backend.bytes.Load() != uint64(len(payload)) {
		t.Fatalf("target write bytes=%d want %d", backend.bytes.Load(), len(payload))
	}
	if backend.durationNanos.Load() == 0 {
		t.Fatal("target write duration was not recorded")
	}
}
