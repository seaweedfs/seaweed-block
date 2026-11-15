package ublk

// Frontend contract for the ublk bridge, the same contract the NBD and
// NVMe frontends keep: reads and writes reach the backend at the right
// offset, FLUSH and write-FUA reach Backend.Sync, every backend failure
// is reported (never acknowledged), and an out-of-range request never
// touches the backend.

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

const testSize = 1 << 20

func newTestHandler(b Backend) handler { return handler{backend: b, size: testSize} }

func sectors(n int) uint32 { return uint32(n >> sectorShift) }

func TestDispatch_WriteThenReadRoundTrip(t *testing.T) {
	rb := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := newTestHandler(rb)
	data := bytes.Repeat([]byte{0x5a}, 8192)
	if got := h.handle(context.Background(), request{op: ioOpWrite, startSector: 16, nrSectors: sectors(len(data))}, data); got != int32(len(data)) {
		t.Fatalf("write result %d, want %d", got, len(data))
	}
	if rb.WriteCount() != 1 || rb.WriteAt(0).Offset != 16<<sectorShift || !bytes.Equal(rb.WriteAt(0).Data, data) {
		t.Fatalf("backend write %+v, want offset %d and the request bytes", rb.WriteAt(0), 16<<sectorShift)
	}
	if rb.SyncCount() != 0 {
		t.Fatalf("a plain write synced %d times", rb.SyncCount())
	}
	out := make([]byte, len(data))
	if got := h.handle(context.Background(), request{op: ioOpRead, startSector: 16, nrSectors: sectors(len(out))}, out); got != int32(len(out)) {
		t.Fatalf("read result %d, want %d", got, len(out))
	}
	if !bytes.Equal(out, data) || rb.ReadAt(0).Offset != 16<<sectorShift {
		t.Fatalf("read back %x... at offset %d", out[:4], rb.ReadAt(0).Offset)
	}
}

func TestDispatch_FlushSyncs(t *testing.T) {
	rb := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := newTestHandler(rb)
	if got := h.handle(context.Background(), request{op: ioOpFlush}, nil); got != 0 {
		t.Fatalf("flush result %d, want 0", got)
	}
	if rb.SyncCount() != 1 || rb.WriteCount() != 0 {
		t.Fatalf("flush: %d syncs %d writes, want exactly one sync", rb.SyncCount(), rb.WriteCount())
	}
}

func TestDispatch_WriteFUASyncsBeforeCompleting(t *testing.T) {
	rb := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := newTestHandler(rb)
	data := bytes.Repeat([]byte{1}, 4096)
	if got := h.handle(context.Background(), request{op: ioOpWrite, flags: ioFlagFUA, nrSectors: sectors(len(data))}, data); got != int32(len(data)) {
		t.Fatalf("FUA write result %d", got)
	}
	if rb.WriteCount() != 1 || rb.SyncCount() != 1 {
		t.Fatalf("FUA write: %d writes %d syncs, want 1 and 1", rb.WriteCount(), rb.SyncCount())
	}
}

// failingBackend fails the selected operation.
type failingBackend struct {
	failRead, failWrite, failSync, shortRead bool
	err                                      error
	syncs, writes, reads                     int
}

func (b *failingBackend) Read(_ context.Context, _ int64, p []byte) (int, error) {
	b.reads++
	if b.failRead {
		return 0, b.err
	}
	if b.shortRead {
		return len(p) - 1, nil
	}
	return len(p), nil
}

func (b *failingBackend) Write(_ context.Context, _ int64, p []byte) (int, error) {
	b.writes++
	if b.failWrite {
		return 0, b.err
	}
	return len(p), nil
}

func (b *failingBackend) Sync(context.Context) error {
	b.syncs++
	if b.failSync {
		return b.err
	}
	return nil
}

func TestDispatch_BackendFailuresAreReportedNotAcknowledged(t *testing.T) {
	eio := -int32(syscall.EIO)
	for _, tc := range []struct {
		name string
		b    *failingBackend
		r    request
	}{
		{"read error", &failingBackend{failRead: true, err: errors.New("boom")}, request{op: ioOpRead, nrSectors: 8}},
		{"short read", &failingBackend{shortRead: true}, request{op: ioOpRead, nrSectors: 8}},
		{"write error", &failingBackend{failWrite: true, err: errors.New("boom")}, request{op: ioOpWrite, nrSectors: 8}},
		{"flush error", &failingBackend{failSync: true, err: errors.New("boom")}, request{op: ioOpFlush}},
		{"FUA sync error", &failingBackend{failSync: true, err: errors.New("boom")}, request{op: ioOpWrite, flags: ioFlagFUA, nrSectors: 8}},
		{"stale primary", &failingBackend{failWrite: true, err: frontend.ErrStalePrimary}, request{op: ioOpWrite, nrSectors: 8}},
		{"backend closed", &failingBackend{failSync: true, err: frontend.ErrBackendClosed}, request{op: ioOpFlush}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(tc.b)
			if got := h.handle(context.Background(), tc.r, make([]byte, 4096)); got != eio {
				t.Fatalf("result %d, want -EIO (%d)", got, eio)
			}
		})
	}
}

func TestDispatch_OutOfRangeNeverTouchesTheBackend(t *testing.T) {
	einval := -int32(syscall.EINVAL)
	for _, r := range []request{
		{op: ioOpRead, startSector: testSize >> sectorShift, nrSectors: 1},
		{op: ioOpWrite, startSector: (testSize >> sectorShift) - 1, nrSectors: 2},
		{op: ioOpWrite, startSector: 1<<63 - 1, nrSectors: 8},
	} {
		b := &failingBackend{}
		if got := newTestHandler(b).handle(context.Background(), r, make([]byte, 4096)); got != einval {
			t.Fatalf("%+v: result %d, want -EINVAL", r, got)
		}
		if b.reads+b.writes+b.syncs != 0 {
			t.Fatalf("%+v reached the backend", r)
		}
	}
}

func TestDispatch_BufferShorterThanRequestIsRefused(t *testing.T) {
	b := &failingBackend{}
	if got := newTestHandler(b).handle(context.Background(), request{op: ioOpWrite, nrSectors: 16}, make([]byte, 4096)); got != -int32(syscall.EINVAL) {
		t.Fatalf("result %d, want -EINVAL for a request larger than its buffer", got)
	}
	if b.writes != 0 {
		t.Fatal("an oversized request reached the backend")
	}
}

func TestDispatch_UnsupportedOperations(t *testing.T) {
	for _, op := range []uint8{ioOpDiscard, ioOpWriteSame, ioOpWriteZeroes, 10, 18, 0xff} {
		b := &failingBackend{}
		if got := newTestHandler(b).handle(context.Background(), request{op: op, nrSectors: 8}, make([]byte, 4096)); got != -int32(syscall.EOPNOTSUPP) {
			t.Fatalf("op %d: result %d, want -EOPNOTSUPP", op, got)
		}
		if b.reads+b.writes+b.syncs != 0 {
			t.Fatalf("op %d reached the backend", op)
		}
	}
}

// The counters the fsync gate reads: every received request is counted
// by kind before the backend call, FUA writes separately, and failures.
func TestDispatch_CountersCountReceivedRequests(t *testing.T) {
	c := &counters{}
	h := handler{backend: &failingBackend{failSync: true, err: errors.New("boom")}, size: testSize, stats: c}
	ctx := context.Background()
	buf := make([]byte, 4096)
	h.handle(ctx, request{op: ioOpRead, nrSectors: 8}, buf)
	h.handle(ctx, request{op: ioOpWrite, nrSectors: 8}, buf)
	h.handle(ctx, request{op: ioOpWrite, flags: ioFlagFUA, nrSectors: 8}, buf) // sync fails -> error
	h.handle(ctx, request{op: ioOpFlush}, nil)                                 // sync fails -> error
	h.handle(ctx, request{op: ioOpFlush}, nil)
	h.handle(ctx, request{op: ioOpDiscard, nrSectors: 8}, buf) // -EOPNOTSUPP -> error
	got := c.snapshot()
	want := Stats{ReadRequests: 1, WriteRequests: 2, FUAWrites: 1, FlushRequests: 2, OtherRequests: 1, Errors: 4}
	if got != want {
		t.Fatalf("counters %+v, want %+v", got, want)
	}
	if (handler{backend: &failingBackend{}, size: testSize}).handle(ctx, request{op: ioOpFlush}, nil) != 0 {
		t.Fatal("an uncounted handler failed a flush")
	}
}
