package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// snapshot --pin / --class / --expires send the retention class and the
// expiry; conflicting flags are refused before any request.
func TestOpsChunk_SnapshotPinAndExpires(t *testing.T) {
	srv, got := chunkServer(t, 200, `{"volumeID":"v1","name":"s1"}`)
	before := time.Now()
	if code, _, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "r", "--pin", "--expires", "1h"); code != 0 {
		t.Fatalf("snapshot --pin --expires: %d %q", code, errs)
	}
	body := (*got)[0].body
	exp, _ := body["expiresAtNs"].(float64)
	if body["class"] != chunkstore.SnapshotClassPinned || exp < float64(before.Add(time.Hour).UnixNano())-1e9 {
		t.Fatalf("pinned request: %+v", body)
	}
	for _, args := range [][]string{
		{"--pin", "--class", "semantic"},
		{"--expires", "1h", "--expires-at-ns", "5"},
		{"--expires", "-1s"},
	} {
		a := append([]string{"snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s2"}, args...)
		if code, out, _ := runChunk(a...); code == 0 || !strings.Contains(out, codeBadRequestCLI) {
			t.Fatalf("snapshot %v: exit %d %q; want a bad request", args, code, out)
		}
	}
	if len(*got) != 1 {
		t.Fatalf("refused flags still sent requests: %d", len(*got))
	}
}

// usage and retain run catalog-direct against the filer: set a policy,
// dry-run, apply, and see the accounting follow.
func TestOpsChunk_UsageAndRetain(t *testing.T) {
	ctx := context.Background()
	mem := newManifestFixtureFiler(chunkstore.NewMemFiler())
	prev := dialChunkFiler
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	cat.SetClock(func() time.Time { return old })
	head := storage.ChunkHead{Gen: 2, Epoch: 1, Holder: "s1/r1", RemoteLSN: 42, State: storage.ChunkHeadActive,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e1/c0/k"}}}
	head = prepareFixtureHead(t, mem, head)
	for i := 0; i < 5; i++ {
		if _, err := cat.CreateSnapshot(ctx, "v1", chunkstore.SnapshotRequest{Name: fmt.Sprintf("s%d", i), RequestID: fmt.Sprintf("r%d", i), Class: chunkstore.SnapshotClassSemantic}, head); err != nil {
			t.Fatal(err)
		}
	}

	filer := []string{"--volume", "v1", "--chunk-filer", "filer:18888"}
	if code, out, errs := runChunk(append([]string{"retain", "--set-policy", "--keep-newest", "2", "--keep-younger", "1m"}, filer...)...); code != 0 || !strings.Contains(out, `"policySet":true`) {
		t.Fatalf("set policy: %d %q %q", code, out, errs)
	}
	code, out, errs := runChunk(append([]string{"retain", "--dry-run"}, filer...)...)
	var dry chunkstore.RetentionResult
	if code != 0 || json.Unmarshal([]byte(out), &dry) != nil || !dry.DryRun || dry.Expired != 3 || len(dry.Deleted) != 0 {
		t.Fatalf("dry run: %d %q %q", code, out, errs)
	}
	code, out, errs = runChunk(append([]string{"retain"}, filer...)...)
	var applied chunkstore.RetentionResult
	if code != 0 || json.Unmarshal([]byte(out), &applied) != nil || len(applied.Deleted) != 3 || applied.Kept != 2 {
		t.Fatalf("retain: %d %q %q", code, out, errs)
	}
	code, out, errs = runChunk(append([]string{"usage"}, filer...)...)
	var u chunkstore.VolumeUsage
	if code != 0 || json.Unmarshal([]byte(out), &u) != nil || len(u.Snapshots) != 2 || u.Policy.KeepNewest != 2 || !u.PolicySet {
		t.Fatalf("usage: %d %q %q", code, out, errs)
	}
	if code, out, _ := runChunk("retain", "--volume", "v1", "--chunk-filer", "f:1", "--set-policy", "--dry-run"); code == 0 || !strings.Contains(out, codeBadRequestCLI) {
		t.Fatalf("--set-policy --dry-run: %d %q; want a bad request", code, out)
	}
}
