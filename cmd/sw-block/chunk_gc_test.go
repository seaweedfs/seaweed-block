package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/host/master"
)

type gcCLIStore struct {
	writes       int
	blobs        map[string][]byte
	failDeleteAt int
	afterUpdate  func(*filerpb.Entry) error
	*chunkstore.MemFiler
	deletes int
}

func (s *gcCLIStore) Put(_ context.Context, data []byte) (string, error) {
	if s.blobs == nil {
		s.blobs = map[string][]byte{}
	}
	fid := fmt.Sprintf("1,%d", len(s.blobs)+1)
	s.blobs[fid] = append([]byte{}, data...)
	return fid, nil
}
func (s *gcCLIStore) Get(_ context.Context, fid string) ([]byte, error) {
	b, ok := s.blobs[fid]
	if !ok {
		return nil, errors.New("missing blob")
	}
	return b, nil
}
func (s *gcCLIStore) DeleteBlob(_ context.Context, fid string) error {
	s.deletes++
	delete(s.blobs, fid)
	if s.deletes == s.failDeleteAt {
		return errors.New("delete response lost")
	}
	return nil
}

func TestOpsChunkGC_DefaultIsReadOnly(t *testing.T) {
	h, e := master.New(gcTestMasterConfig(t))
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
	dialChunkFiler = func(context.Context, string) (chunkstore.Filer, func() error, error) {
		return store, func() error { return nil }, nil
	}
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
	if err := s.MemFiler.UpdateEntry(ctx, dir, e, expected); err != nil {
		return err
	}
	if s.afterUpdate != nil {
		return s.afterUpdate(e)
	}
	return nil
}
func (s *gcCLIStore) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	s.writes++
	return s.MemFiler.DeleteEntry(ctx, dir, name, expected)
}

func TestOpsChunkGC_InvalidAdmissionHasNoStoreEffects(t *testing.T) {
	old := dialChunkFiler
	defer func() { dialChunkFiler = old }()
	calls := 0
	dialChunkFiler = func(context.Context, string) (chunkstore.Filer, func() error, error) {
		calls++
		t.Fatal("invalid request reached store")
		return nil, nil, nil
	}
	for _, args := range [][]string{
		{"gc"},
		{"gc", "--chunk-filer", "store", "--master", "master", "--grace", "5m"},
		{"gc", "--chunk-filer", "store", "--master", "master", "--intent-ttl", "0"},
		{"gc", "--chunk-filer", "store", "--master", "master", "--timeout", "0"},
		{"gc", "--chunk-filer", "store", "--master", "master", "unexpected"},
	} {
		rc, _, _ := runChunk(args...)
		if rc != 2 {
			t.Fatalf("invalid %v: exit=%d", args, rc)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid admission dialed %d times", calls)
	}
}

func gcPartialFixture(t *testing.T) (*gcCLIStore, *time.Time) {
	t.Helper()
	store := &gcCLIStore{MemFiler: chunkstore.NewMemFiler()}
	oldDial, oldClock := dialChunkFiler, chunkGCNow
	t.Cleanup(func() { dialChunkFiler = oldDial; chunkGCNow = oldClock })
	dialChunkFiler = func(context.Context, string) (chunkstore.Filer, func() error, error) {
		return store, func() error { return nil }, nil
	}
	now := time.Now().Add(2 * time.Hour)
	chunkGCNow = func() time.Time { return now }
	ctx := context.Background()
	if err := store.CreateEntry(ctx, "/gc-partial", &filerpb.Entry{Name: "v1", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	cat, err := chunkstore.NewCatalog(store, "/gc-partial")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	remote, err := chunkstore.NewChunkRemote(store, store, "/gc-partial/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"orphan-a", "orphan-b"} {
		if err = remote.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	return store, &now
}
func runPartialGC() (int, string, string) {
	return runChunk("gc", "--master", "127.0.0.1:1", "--chunk-filer", "local", "--chunk-path", "/gc-partial", "--apply")
}
func TestOpsChunkGC_FinalWriteResponseLostReportsUnknown(t *testing.T) {
	store, now := gcPartialFixture(t)
	store.afterUpdate = func(e *filerpb.Entry) error {
		if e.Name == "_GC_MARK" && strings.Contains(string(e.Content), `"ready":true`) {
			return errors.New("final write response lost")
		}
		return nil
	}
	rc, out, _ := runPartialGC()
	if rc != 1 {
		t.Fatalf("lost response must fail: rc=%d %s", rc, out)
	}
	entry, err := store.LookupEntry(context.Background(), "/gc-partial", "_GC_MARK")
	if err != nil || !strings.Contains(string(entry.Content), `"ready":true`) {
		t.Fatalf("fixture did not commit completed mark: %v %v", entry, err)
	}
	var body struct {
		Code string `json:"code"`
		GC   struct {
			MarkOutcome string   `json:"mark_outcome"`
			Unknown     []string `json:"unknown_outcomes"`
		} `json:"gc"`
	}
	if err = json.Unmarshal([]byte(out), &body); err != nil || body.Code == "" || body.GC.MarkOutcome != "unknown" || len(body.GC.Unknown) != 1 || body.GC.Unknown[0] != "completed_mark_write" {
		t.Fatalf("lost committed outcome discarded: %s %v", out, err)
	}
	store.afterUpdate = nil
	*now = now.Add(2 * time.Hour)
	rc, out, _ = runPartialGC()
	var success chunkstore.GCExecution
	if err = json.Unmarshal([]byte(out), &success); rc != 0 || err != nil || success.Reclaimed != 2 {
		t.Fatalf("committed mark was not reusable despite error: %d %s %v", rc, out, err)
	}
}
func TestOpsChunkGC_PartialReclaimReportsKnownAndUnknown(t *testing.T) {
	store, now := gcPartialFixture(t)
	if rc, out, _ := runPartialGC(); rc != 0 {
		t.Fatalf("first mark: %d %s", rc, out)
	}
	*now = now.Add(2 * time.Hour)
	store.failDeleteAt = 2
	rc, out, _ := runPartialGC()
	if rc != 1 {
		t.Fatalf("partial failure exit=%d %s", rc, out)
	}
	// Both requests took effect, but only the first was acknowledged. The
	// second is UNKNOWN to the caller; it must not be counted as known success.
	if store.deletes != 2 || len(store.blobs) != 0 {
		t.Fatal("partial-effect fixture was not exercised")
	}
	var body struct {
		Code string `json:"code"`
		GC   struct {
			Reclaimed   int
			MarkOutcome string   `json:"mark_outcome"`
			Unknown     []string `json:"unknown_outcomes"`
		} `json:"gc"`
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil || body.Code == "" || body.GC.Reclaimed != 1 || body.GC.MarkOutcome != "in_progress" || len(body.GC.Unknown) != 1 || body.GC.Unknown[0] != "reclamation" {
		t.Fatalf("partial effects discarded or overclaimed: %s %v", out, err)
	}
}

type gcFailedMarkLookup struct {
	*gcCLIStore
	lookups int
}

func (s *gcFailedMarkLookup) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if name == "_GC_MARK" {
		s.lookups++
		return nil, errors.New("mark lookup unavailable")
	}
	return s.gcCLIStore.LookupEntry(ctx, dir, name)
}
func TestOpsChunkGC_FailedMarkLookupPreservesUnknownUnbound(t *testing.T) {
	for _, apply := range []bool{false, true} {
		name := "dry-run"
		if apply {
			name = "apply"
		}
		t.Run(name, func(t *testing.T) {
			store := &gcFailedMarkLookup{gcCLIStore: &gcCLIStore{MemFiler: chunkstore.NewMemFiler()}}
			old := dialChunkFiler
			t.Cleanup(func() { dialChunkFiler = old })
			dialChunkFiler = func(context.Context, string) (chunkstore.Filer, func() error, error) {
				return store, func() error { return nil }, nil
			}
			args := []string{"gc", "--master", "127.0.0.1:1", "--chunk-filer", "local", "--chunk-path", "/gc-lookup"}
			if apply {
				args = append(args, "--apply")
			}
			rc, out, stderr := runChunk(args...)
			var body struct {
				Code string                 `json:"code"`
				GC   chunkstore.GCExecution `json:"gc"`
			}
			if err := json.Unmarshal([]byte(out), &body); err != nil {
				t.Fatal(err)
			}
			if rc != 1 || body.Code == "" || !strings.Contains(stderr, "mark lookup unavailable") || !body.GC.UnknownUnbound || body.GC.MarkOutcome != "not_attempted" || body.GC.DryRun == apply || body.GC.MarkSaved || store.lookups != 1 || store.writes != 0 || store.deletes != 0 {
				t.Fatalf("failed lookup misreported/mutated: rc=%d out=%s stderr=%s lookups=%d writes=%d deletes=%d", rc, out, stderr, store.lookups, store.writes, store.deletes)
			}
		})
	}
}

func TestOpsChunkGC_PermanentDeleteReportsFailureWithSavedCoverage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	out := chunkstore.GCExecution{MarkOutcome: "complete", MarkSaved: true, GCResult: chunkstore.GCResult{
		Reclaimed: 1, FailedDeletes: []chunkstore.GCDeleteFailure{{GCObject: chunkstore.GCObject{Volume: "v1", Key: "bad", FID: "3,bad"}, Error: "http 400"}},
	}}
	rc := chunkGCResult(out, chunkstore.ErrBlobDeleteRejected, &stdout, &stderr)
	var body struct {
		GC chunkstore.GCExecution `json:"gc"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rc != 1 || !body.GC.MarkSaved || body.GC.MarkOutcome != "complete" || body.GC.Reclaimed != 1 || len(body.GC.FailedDeletes) != 1 || body.GC.FailedDeletes[0].FID != "3,bad" {
		t.Fatalf("partial failure lost saved coverage or object identity: rc=%d body=%s", rc, stdout.String())
	}
}
