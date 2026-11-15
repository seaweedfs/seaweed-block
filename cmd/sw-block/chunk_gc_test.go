package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/host/master"
)

type gcCLIStore struct {
	writes int
	*chunkstore.MemFiler
	deletes int
}

func (s *gcCLIStore) DeleteBlob(context.Context, string) error { s.deletes++; return nil }

func TestOpsChunkGC_DefaultIsReadOnly(t *testing.T) {
	h, e := master.New(master.Config{AuthorityStoreDir: t.TempDir(), Listen: "127.0.0.1:0"})
	if e != nil {
		t.Fatal(e)
	}
	h.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := h.Close(ctx); e != nil {
			t.Error(e)
		}
	}()
	store := &gcCLIStore{MemFiler: chunkstore.NewMemFiler()}
	old := dialChunkFiler
	defer func() { dialChunkFiler = old }()
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return store, func() error { return nil }, nil }
	code, out, errs := runChunk("gc", "--master", h.Addr(), "--chunk-filer", "local-test", "--chunk-path", "/gc-test")
	if code != 0 {
		t.Fatalf("missing safe GC entrypoint: exit=%d out=%s stderr=%s", code, out, errs)
	}
	var report struct {
		DryRun      bool  `json:"dry_run"`
		Roots       []any `json:"roots"`
		WouldDelete []any `json:"would_delete"`
	}
	if e := json.Unmarshal([]byte(out), &report); e != nil || !report.DryRun || report.Roots == nil || report.WouldDelete == nil {
		t.Fatalf("incomplete dry-run report: %s %v", out, e)
	}
	entries, e := store.ListEntries(context.Background(), "/gc-test", "", "", 100)
	if e != nil || len(entries) != 0 || store.deletes != 0 || store.writes != 0 {
		t.Fatalf("default GC mutated store: entries=%d deletes=%d err=%v", len(entries), store.deletes, e)
	}
}

func (s *gcCLIStore) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, excl bool) error {
	s.writes++
	return s.MemFiler.CreateEntry(ctx, dir, e, excl)
}
func (s *gcCLIStore) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	s.writes++
	return s.MemFiler.UpdateEntry(ctx, dir, e, expected)
}
func (s *gcCLIStore) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	s.writes++
	return s.MemFiler.DeleteEntry(ctx, dir, name, expected)
}
