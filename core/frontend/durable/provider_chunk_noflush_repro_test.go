//go:build diagnostic_repro

package durable_test

import (
	"context"
	"encoding/binary"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
	"time"
)

// Public provider + actual NVMe handler, file-backed v3 with the default
// 64 MiB WAL. No injected I/O failures, private selector, or flusher control.
func TestWALStoreChunkV3_NoFlushPressureDiagnostic(t *testing.T) {
	const total = 400 * 1024 * 1024
	const transfer = 128 * 1024
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: total / 4096, ChunkBlocks: 1024, ChunkCaptureInterval: 0,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	})
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	be, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	before, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ws := s.(*storage.WALStore)
	t.Logf("before HEAD gen=%d remoteLSN=%d cp=%d next=%d retention=%d", before.Gen, before.RemoteLSN, ws.CheckpointLSN(), ws.NextLSN(), ws.RecoveryRetentionLSNs())
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: be, BlockSize: 4096, VolumeSize: total})
	data := make([]byte, transfer)
	start := time.Now()
	for off := 0; off < total; off += transfer {
		for i := range data {
			if i%8 == 0 {
				binary.LittleEndian.PutUint64(data[i:i+8], uint64(off+i))
			}
		}
		r := h.Handle(ctx, nvme.IOCommand{Opcode: 0x01, NSID: 1, SLBA: uint64(off / 4096), NLB: transfer / 4096, Data: data})
		if r.AsError() != nil {
			t.Fatalf("NO FLUSH write bytes=%d elapsed=%v result=%+v cp=%d next=%d", off, time.Since(start), r, ws.CheckpointLSN(), ws.NextLSN())
		}
		if (off+transfer)%(16*1024*1024) == 0 {
			t.Logf("NO FLUSH progress bytes=%d elapsed=%v cp=%d next=%d", off+transfer, time.Since(start), ws.CheckpointLSN(), ws.NextLSN())
		}
	}
	// The expected bytes come from the submitted pattern, not another reader.
	for off := 0; off < total; off += transfer {
		got := make([]byte, transfer)
		n, err := be.Read(ctx, int64(off), got)
		if err != nil {
			t.Fatalf("readback at%d: %v", off, err)
		}
		if n != transfer {
			t.Fatalf("short readback at%d: %d", off, n)
		}
		for i := 0; i < len(got); i += 8 {
			if value := binary.LittleEndian.Uint64(got[i : i+8]); value != uint64(off+i) {
				t.Fatalf("readback offset%d got%d", off+i, value)
			}
		}
	}

	after, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st := p.DurableStatuses()
	if after.Gen != before.Gen || after.RemoteLSN != before.RemoteLSN {
		t.Fatalf("remote moved without explicit cut: before=%+v after=%+v", before, after)
	}
	if len(st) != 1 || st[0].Chunk == nil || st[0].Chunk.Stats.CutsOK != 0 {
		t.Fatalf("unexpected cuts: %+v", st)
	}
	if ws.CheckpointLSN() == 0 || st[0].WriteProfile.WALAppendWrapCount == 0 {
		t.Fatal("no checkpoint or physical wrap observed")
	}
	t.Logf("after HEAD gen=%d remoteLSN=%d cp=%d next=%d wraps=%d cuts=%d", after.Gen, after.RemoteLSN, ws.CheckpointLSN(), ws.NextLSN(), st[0].WriteProfile.WALAppendWrapCount, st[0].Chunk.Stats.CutsOK)

}
