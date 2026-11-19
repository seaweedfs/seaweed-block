package durable_test

import (
	"bytes"
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
func TestWALStoreChunkV3_SequentialBurstFlush(t *testing.T) {
	const total = 320 * 1024 * 1024
	const transfer = 128 * 1024
	const flushEvery = 48 * 1024 * 1024
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: total / 4096, ChunkBlocks: 1024,
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
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: be, BlockSize: 4096, VolumeSize: total})
	data := make([]byte, transfer)
	start := time.Now()
	for off := 0; off < total; off += transfer {
		fillBurstTransfer(data, off)
		r := h.Handle(ctx, nvme.IOCommand{Opcode: 0x01, NSID: 1, SLBA: uint64(off / 4096), NLB: transfer / 4096, Data: data})
		if r.AsError() != nil {
			_, syncErr := s.Sync()
			t.Fatalf("write bytes=%d elapsed=%v result=%+v exact Sync=%T %v", off, time.Since(start), r, syncErr, syncErr)
		}
		if (off+transfer)%flushEvery == 0 || off+transfer == total {
			r = h.Handle(ctx, nvme.IOCommand{Opcode: 0x00, NSID: 1})
			if r.AsError() != nil {
				_, syncErr := s.Sync()
				t.Fatalf("flush bytes=%d elapsed=%v result=%+v exact Sync=%T %v", off+transfer, time.Since(start), r, syncErr, syncErr)
			}
			t.Logf("flush bytes=%d elapsed=%v", off+transfer, time.Since(start))
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
			if word := binary.LittleEndian.Uint64(got[i : i+8]); word != uint64(off+i) {
				t.Fatalf("readback word at byte%d=%d want%d", off+i, word, off+i)
			}
		}
	}

}

func fillBurstTransfer(data []byte, off int) {
	for i := 0; i < len(data); i += 8 {
		binary.LittleEndian.PutUint64(data[i:i+8], uint64(off+i))
	}
}

func TestWALStoreChunkV3_BurstPatternBindsFullLBA(t *testing.T) {
	const transfer = 128 * 1024
	zero, later := make([]byte, transfer), make([]byte, transfer)
	fillBurstTransfer(zero, 0)
	for _, off := range []int{32 * 1024 * 1024, 64 * 1024 * 1024, 256 * 1024 * 1024} {
		fillBurstTransfer(later, off)
		if bytes.Equal(zero, later) {
			t.Fatalf("pattern aliases LBA0 and LBA%d", off/4096)
		}
		for _, i := range []int{0, 4096, len(later) - 8} {
			if got := binary.LittleEndian.Uint64(later[i : i+8]); got != uint64(off+i) {
				t.Fatalf("word at byte%d=%d want%d", off+i, got, off+i)
			}
		}
	}
}
