//go:build manifest_perf

package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// File-backed proxy: every immutable object is written and fsynced. Metadata
// RPCs use the deterministic filer model; results exclude network/guest fsync.
type manifestFiles struct{ dir string }

func (b manifestFiles) Put(ctx context.Context, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(b.dir, "blob-")
	if err != nil {
		return "", err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	cerr := f.Close()
	if err == nil {
		err = cerr
	}
	return filepath.Base(f.Name()), err
}
func (b manifestFiles) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(b.dir, key))
}

func TestManifestPerf_64DirtyChunks(t *testing.T) {
	for rep := 0; rep < 3; rep++ {
		order := []int{1, 8}
		if rep%2 == 1 {
			order = []int{8, 1}
		}
		for _, parallel := range order {
			dir := t.TempDir()
			f := newFakeFiler()
			b := manifestFiles{dir}
			r, err := NewChunkRemote(f, b, "/chunks/v1")
			if err != nil {
				t.Fatal(err)
			}
			s, err := storage.CreateWALStore(filepath.Join(dir, "store"), 64*1024, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Recover(); err != nil {
				t.Fatal(err)
			}
			l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: 1024, Epoch: 1, Holder: "h1", FreshFromHead: true, UploadConcurrency: parallel})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 64; i++ {
				if _, err = s.Write(uint32(i*1024), bytes.Repeat([]byte{byte(i + 1)}, 4096)); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			if _, err = s.Sync(); err != nil {
				t.Fatal(err)
			}
			syncTime := time.Since(start)
			start = time.Now()
			if _, err = l.Capture(context.Background()); err != nil {
				t.Fatal(err)
			}
			cutTime := time.Since(start)
			t.Logf("PERF dirty64 rep=%d parallel=%d chunk_bytes=4194304 upload_bytes=268435456 sync_ms=%.3f cut_ms=%.3f", rep, parallel, float64(syncTime.Microseconds())/1000, float64(cutTime.Microseconds())/1000)
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestManifestPerf_Written10GiBSnapshotFork(t *testing.T) {
	ctx := context.Background()
	f := newFakeFiler()
	c := newCat(t, f)
	b := manifestFiles{t.TempDir()}
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v1", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v1", Geometry{NumBlocks: 10 * 1024 * 1024 * 1024 / 4096, BlockSize: 4096, ChunkBlocks: 1024}); err != nil {
		t.Fatal(err)
	}
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	h, _ := r.GetHead(ctx)
	next := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 42, Chunks: map[uint64]storage.ChunkRef{}}
	keys := make([]string, 2560)
	for i := range keys {
		keys[i] = fmt.Sprintf("data/%d", i)
	}
	intent, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: h.Gen, Epoch: 1, Holder: "h1", Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0xa5}, 4<<20)
	start := time.Now()
	for i, key := range keys {
		binary.LittleEndian.PutUint64(data, uint64(i))
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		next.Chunks[uint64(i)] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next)
	if err != nil {
		t.Fatal(err)
	}
	next.Gen = gen
	if err = r.DeleteIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	t.Logf("PERF materialized_bytes=10737418240 chunks=2560 upload_ms=%.3f", float64(time.Since(start).Microseconds())/1000)
	for _, format := range []string{"flat", "segmented"} {
		if format == "segmented" {
			prepared, ids, err := r.PrepareManifest(ctx, next, next, 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: next.Gen, Epoch: next.Epoch}, prepared)
			if err != nil {
				t.Fatal(err)
			}
			prepared.Gen = gen
			next = prepared
			for _, id := range ids {
				if err = r.DeleteIntent(ctx, id); err != nil {
					t.Fatal(err)
				}
			}
		}
		entry, err := headEntry(next)
		if err != nil {
			t.Fatal(err)
		}
		segmentBytes := 0
		for _, ref := range next.Segments {
			body, err := r.Get(ctx, ref.Key)
			if err != nil {
				t.Fatal(err)
			}
			segmentBytes += len(body)
		}
		for rep := 0; rep < 3; rep++ {
			name := fmt.Sprintf("%s-%d", format, rep)
			start := time.Now()
			snap, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: name}, next)
			if err != nil {
				t.Fatal(err)
			}
			snapTime := time.Since(start)
			target := "fork-" + name
			if err = f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: target, IsDirectory: true}, true); err != nil {
				t.Fatal(err)
			}
			start = time.Now()
			if _, err = c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: name, Target: target, RequestID: target}); err != nil {
				t.Fatal(err)
			}
			forkTime := time.Since(start)
			t.Logf("PERF written10GiB format=%s rep=%d head_bytes=%d total_segment_bytes=%d snapshot_entry_bytes=%d snapshot_ms=%.3f fork_ms=%.3f", format, rep, len(entry.Content), segmentBytes, snap.ManifestBytes, float64(snapTime.Microseconds())/1000, float64(forkTime.Microseconds())/1000)
		}
	}
}
