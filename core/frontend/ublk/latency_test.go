package ublk

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"github.com/seaweedfs/seaweed-block/core/iolatency"
)

type timingBackend struct {
	*testback.RecordingBackend
	requests iolatency.Requests
}

func (b *timingBackend) BeginDurabilityRequest(ctx context.Context, k string) (context.Context, *iolatency.Request) {
	return b.requests.Begin(ctx, k)
}
func (b *timingBackend) Write(ctx context.Context, o int64, p []byte) (int, error) {
	iolatency.BackendEnter(ctx)
	defer iolatency.BackendExit(ctx)
	return b.RecordingBackend.Write(ctx, o, p)
}
func (b *timingBackend) Sync(ctx context.Context) error {
	iolatency.BackendEnter(ctx)
	defer iolatency.BackendExit(ctx)
	return b.RecordingBackend.Sync(ctx)
}
func TestUblkDurabilityLatency(t *testing.T) {
	b := &timingBackend{RecordingBackend: testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})}
	h := newTestHandler(b)
	h.handle(context.Background(), request{op: ioOpWrite, flags: ioFlagFUA, nrSectors: 8}, make([]byte, 4096))
	h.handle(context.Background(), request{op: ioOpFlush}, nil)
	h.handle(context.Background(), request{op: ioOpWrite, startSector: 1 << 30, nrSectors: 8}, make([]byte, 4096))
	s := b.requests.Snapshot()
	if s.Writes != 2 || s.FUAWrites != 1 || s.Flushes != 1 || s.Failed != 1 || s.RequestTotal.Count != 3 || s.ReceiveToBackend.Count != 2 || s.AckDispatch.Count != 2 {
		t.Fatal(s)
	}
	if b.SyncCount() != 2 || b.WriteCount() != 1 {
		t.Fatal("timing changed dispatch")
	}
}
