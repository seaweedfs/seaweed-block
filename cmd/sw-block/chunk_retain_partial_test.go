package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// failingFiler fails the UpdateEntry calls fail picks, after or before
// applying them.
type failingFiler struct {
	*chunkstore.MemFiler
	fail       func(dir, name string) bool
	afterApply bool
}

func (f *failingFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	if f.fail != nil && f.fail(dir, e.GetName()) {
		if f.afterApply {
			if err := f.MemFiler.UpdateEntry(ctx, dir, e, expected); err != nil {
				return err
			}
			return errors.New("response lost")
		}
		return errors.New("filer unavailable")
	}
	return f.MemFiler.UpdateEntry(ctx, dir, e, expected)
}

// retain that fails part way prints what it confirmed, the failing
// snapshot (uncertain) and what it never reached, with the error, and
// exits nonzero; a lost response is finished by the next run.
func TestOpsChunk_RetainReportsPartialProgress(t *testing.T) {
	ctx := context.Background()
	ff := &failingFiler{MemFiler: chunkstore.NewMemFiler()}
	fm := newManifestFixtureFiler(ff)
	prev := dialChunkFiler
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) { return fm, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(fm, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-48 * time.Hour)
	head := storage.ChunkHead{Gen: 2, Epoch: 1, Holder: "s1/r1", RemoteLSN: 42, State: storage.ChunkHeadActive,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e1/c0/k"}}}
	head = prepareFixtureHead(t, fm, head)
	for i := 0; i < 4; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		cat.SetClock(func() time.Time { return at })
		if _, err := cat.CreateSnapshot(ctx, "v1", chunkstore.SnapshotRequest{Name: fmt.Sprintf("s%d", i), RequestID: fmt.Sprintf("r%d", i)}, head); err != nil {
			t.Fatal(err)
		}
	}
	filer := []string{"--volume", "v1", "--chunk-filer", "filer:18888"}
	if code, out, errs := runChunk(append([]string{"retain", "--set-policy", "--keep-newest", "0", "--keep-younger", "0s"}, filer...)...); code != 0 {
		t.Fatalf("set policy: %d %q %q", code, out, errs)
	}
	// Expired newest first: s3, s2, s1, s0. The mark of s2 fails.
	ff.fail = func(dir, _ string) bool { return strings.HasSuffix(dir, "/snaps/s2") }
	type partial struct {
		chunkstore.RetentionResult
		Error *chunkErrBody `json:"error"`
	}
	code, out, errs := runChunk(append([]string{"retain"}, filer...)...)
	var p partial
	if code == 0 || json.Unmarshal([]byte(out), &p) != nil || p.Error == nil || p.Failed == nil {
		t.Fatalf("failing retain: exit %d %q %q; want nonzero with the partial result", code, out, errs)
	}
	if strings.Join(p.Deleted, ",") != "s3" || p.Failed.Name != "s2" || !p.Failed.Uncertain || strings.Join(p.NotAttempted, ",") != "s1,s0" {
		t.Fatalf("partial result: %+v", p)
	}
	if !strings.Contains(errs, "deleted 1, not attempted 2") {
		t.Fatalf("stderr %q; want the counts", errs)
	}

	ff.afterApply = true // the mark lands, its response is lost
	code, out, _ = runChunk(append([]string{"retain"}, filer...)...)
	if code == 0 || json.Unmarshal([]byte(out), &p) != nil || p.Failed == nil || p.Failed.Name != "s2" || !p.Failed.Uncertain {
		t.Fatalf("lost response: exit %d %q", code, out)
	}
	ff.fail = nil
	code, out, errs = runChunk(append([]string{"retain"}, filer...)...)
	var done chunkstore.RetentionResult
	if code != 0 || json.Unmarshal([]byte(out), &done) != nil || strings.Join(done.Deleted, ",") != "s1,s0,s2" {
		t.Fatalf("rerun: %d %q %q; want s1, s0 and the half-deleted s2", code, out, errs)
	}
}
