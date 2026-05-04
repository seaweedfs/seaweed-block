package iscsi_test

import (
	"context"
	"io"
	"log"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

type slowP2Backend struct {
	inner      *testback.RecordingBackend
	writeDelay time.Duration
	writeCalls atomic.Int64
}

func (b *slowP2Backend) Identity() frontend.Identity { return b.inner.Identity() }
func (b *slowP2Backend) Close() error                { return b.inner.Close() }
func (b *slowP2Backend) SetOperational(ok bool, evidence string) {
	b.inner.SetOperational(ok, evidence)
}
func (b *slowP2Backend) Sync(ctx context.Context) error { return b.inner.Sync(ctx) }
func (b *slowP2Backend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	return b.inner.Read(ctx, offset, p)
}
func (b *slowP2Backend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	b.writeCalls.Add(1)
	if b.writeDelay > 0 {
		time.Sleep(b.writeDelay)
	}
	return b.inner.Write(ctx, offset, p)
}

func startP2MemoryTarget(t *testing.T, backend frontend.Backend, volumeSize uint64, maxBurst int) (*iscsi.Target, string) {
	t.Helper()
	neg := iscsi.DefaultNegotiableConfig()
	neg.MaxBurstLength = maxBurst
	neg.FirstBurstLength = maxBurst
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:      "127.0.0.1:0",
		IQN:         "iqn.2026-04.example.v3:v1",
		VolumeID:    "v1",
		Provider:    testback.NewStaticProvider(backend),
		Handler:     iscsi.HandlerConfig{VolumeSize: volumeSize},
		Negotiation: neg,
		Logger:      log.New(io.Discard, "", 0),
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	return tg, addr
}

func currentHeapAlloc() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestP2_ISCSI_LargeWrite4MiB_DoesNotGrowHeapUnbounded ports the useful
// part of V2's large-write memory guard to the V3 target. It exercises real
// R2T chunking with several filesystem-sized writes, then checks that the
// target is not retaining one assembled payload per completed command.
func TestP2_ISCSI_LargeWrite4MiB_DoesNotGrowHeapUnbounded(t *testing.T) {
	const (
		writeBytes = 4 * 1024 * 1024
		blocks     = writeBytes / int(iscsi.DefaultBlockSize)
		writes     = 5
		maxBurst   = 256 * 1024
	)

	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	tg, addr := startP2MemoryTarget(t, rec, writeBytes*writes, maxBurst)
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	payload := make([]byte, writeBytes)
	for i := range payload {
		payload[i] = byte((i * 31) % 251)
	}

	before := currentHeapAlloc()
	for i := 0; i < writes; i++ {
		lba := uint32(i * blocks)
		status, _, traces := cli.scsiCmdFullWithR2TTrace(t, writeCDB10(lba, uint16(blocks)), nil, payload, 0)
		expectGood(t, status, "4MiB WRITE(10)")
		if len(traces) < 2 {
			t.Fatalf("write %d: R2T count=%d want multiple chunks", i, len(traces))
		}
		for j, tr := range traces {
			if tr.Desired > maxBurst {
				t.Fatalf("write %d R2T[%d] DesiredDataLength=%d exceeds MaxBurst=%d", i, j, tr.Desired, maxBurst)
			}
		}
	}
	after := currentHeapAlloc()
	deltaMB := int64(after-before) / (1024 * 1024)
	t.Logf("%d x 4MiB WRITE(10): heap delta=%d MB writes=%d", writes, deltaMB, rec.WriteCount())

	if rec.WriteCount() != writes {
		t.Fatalf("backend WriteCount=%d want %d", rec.WriteCount(), writes)
	}
	if deltaMB > 160 {
		t.Fatalf("heap grew %d MB after %d x 4MiB iSCSI writes; likely retained Data-Out buffers", deltaMB, writes)
	}
}

// TestP2_ISCSI_LargeWrite_SlowBackend_DoesNotAccumulateBuffers simulates a
// backend that blocks briefly on each write, like a WAL admission or disk
// flush stall. Completed commands should release their R2T/Data-Out buffers
// even when backend writes are slower than the initiator's command loop.
func TestP2_ISCSI_LargeWrite_SlowBackend_DoesNotAccumulateBuffers(t *testing.T) {
	const (
		writeBytes = 1 * 1024 * 1024
		blocks     = writeBytes / int(iscsi.DefaultBlockSize)
		writes     = 8
		maxBurst   = 128 * 1024
	)

	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	slow := &slowP2Backend{inner: rec, writeDelay: 25 * time.Millisecond}
	tg, addr := startP2MemoryTarget(t, slow, writeBytes*writes, maxBurst)
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	payload := make([]byte, writeBytes)
	for i := range payload {
		payload[i] = byte((i * 17) % 251)
	}

	before := currentHeapAlloc()
	for i := 0; i < writes; i++ {
		lba := uint32(i * blocks)
		status, _, traces := cli.scsiCmdFullWithR2TTrace(t, writeCDB10(lba, uint16(blocks)), nil, payload, 0)
		expectGood(t, status, "slow-backend WRITE(10)")
		if len(traces) < 2 {
			t.Fatalf("write %d: R2T count=%d want multiple chunks", i, len(traces))
		}
	}
	after := currentHeapAlloc()
	deltaMB := int64(after-before) / (1024 * 1024)
	t.Logf("%d x 1MiB slow WRITE(10): heap delta=%d MB writes=%d", writes, deltaMB, slow.writeCalls.Load())

	if slow.writeCalls.Load() != writes {
		t.Fatalf("backend writeCalls=%d want %d", slow.writeCalls.Load(), writes)
	}
	if deltaMB > 96 {
		t.Fatalf("heap grew %d MB with slow backend; likely accumulating completed Data-Out buffers", deltaMB)
	}
}
