package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestManifestFormatUpgradeSustainedWritesPastWALWithPeriodicFlush(t *testing.T) {
	const blocks, blockSize, chunkBlocks = 1024, 4096, 64
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f, b := newFakeFiler(), newFakeBlobs()
	r, err := NewChunkRemote(f, b, path.Join(catBase, "sustained"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := storage.CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "source"), blocks, blockSize, storage.WALStoreOptions{ChunkBlocks: chunkBlocks, WMode: storage.WModeStrict})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: chunkBlocks, Epoch: 1, Holder: "source", FreshFromHead: true, BarrierBound: 2, UploadConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	walBytes := s.WALCapacityBytes()
	if walBytes == 0 || walBytes > 128<<20 {
		t.Fatalf("smoke WAL capacity outside bounded fixture: %d", walBytes)
	}
	batchBytes := uint64(blocks * blockSize)
	rounds := int(walBytes/batchBytes) + 1
	var payload, last uint64
	var finalByte byte
	for round := 0; round < rounds; round++ {
		finalByte = byte(round%251 + 1)
		data := bytes.Repeat([]byte{finalByte}, blocks*blockSize)
		batch := make([][]byte, blocks)
		for i := range batch {
			batch[i] = data[i*blockSize : (i+1)*blockSize]
		}
		if _, err := s.WriteBatch(0, batch); err != nil {
			t.Fatal(err)
		}
		last, err = s.Sync()
		if err != nil {
			t.Fatal(err)
		}
		payload += batchBytes
		// Use the production periodic flusher, not a second checkpoint path.
		for s.CheckpointLSN() < last {
			select {
			case <-ctx.Done():
				t.Fatalf("periodic Flush did not cover batch %d: %v", round, ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		if (round+1)%4 == 0 || round+1 == rounds {
			if _, err := l.Cut(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if payload <= walBytes || s.FlushCount() == 0 {
		t.Fatal("sustained fixture did not exceed WAL with periodic Flush")
	}
	h, err := r.GetHead(ctx)
	if err != nil || h.RemoteLSN != last {
		t.Fatalf("final frontier: %+v %v", h, err)
	}
	e, err := f.LookupEntry(ctx, path.Join(catBase, "sustained"), headName)
	if err != nil {
		t.Fatal(err)
	}
	var shape struct {
		Format int `json:"format"`
		Levels int `json:"levels"`
	}
	if err := json.Unmarshal(e.Content, &shape); err != nil || shape.Format != 3 || shape.Levels != 1 {
		t.Fatal("sustained cut did not publish format3")
	}
	fresh, err := storage.CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "fresh"), blocks, blockSize, storage.WALStoreOptions{ChunkBlocks: chunkBlocks, WMode: storage.WModeStrict})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fresh.Close() })
	if _, err := fresh.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.SeedChunkFrontier(last); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.AttachChunkLayer(fresh, r, storage.ChunkLayerConfig{ChunkBlocks: chunkBlocks, Epoch: 2, Holder: "fresh", FreshFromHead: true}); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{finalByte}, blockSize)
	for lba := uint32(0); lba < blocks; lba++ {
		got, err := fresh.Read(lba)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("fresh read block%d: %v", lba, err)
		}
	}
	t.Logf("payload=%d > WAL=%d, checkpoint drains=%d, FlushCount=%d; full fresh lazy read exact", payload, walBytes, rounds, s.FlushCount())
}
