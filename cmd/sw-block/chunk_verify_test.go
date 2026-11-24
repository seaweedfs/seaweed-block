package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// verifyCLIFixture is volume v1 with two published chunks on an in-memory
// filer and blob store; it returns the store and the HEAD.
func verifyCLIFixture(t *testing.T) (*manifestFixtureFiler, storage.ChunkHead) {
	t.Helper()
	ctx := context.Background()
	mem := newManifestFixtureFiler(chunkstore.NewMemFiler())
	prev := dialChunkFiler
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		return mem, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	r, err := chunkstore.NewChunkRemote(mem, mem, defaultChunkPathCLI+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	cur, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 42, Chunks: map[uint64]storage.ChunkRef{}}
	for idx := uint64(0); idx < 2; idx++ {
		data := bytes.Repeat([]byte{byte(0x30 + idx)}, 8*4096)
		key := fmt.Sprintf("chunks/e1/c%d/k", idx)
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		h.Chunks[idx] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	h, ids, err := r.PrepareManifest(ctx, cur, h, 8*4096)
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, h); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := r.DeleteIntent(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	return mem, h
}

func decodeVerify(t *testing.T, out string) chunkstore.VerifyResult {
	t.Helper()
	var res chunkstore.VerifyResult
	if err := json.Unmarshal([]byte(out), &res); err != nil || res.VolumeID == "" {
		t.Fatalf("stdout is not a verify result: %q (%v)", out, err)
	}
	return res
}

// verify's exit codes: 0 clean, 3 completed with findings, 1 could not
// complete, 2 usage; the result is one JSON object with one finding per line.
func TestOpsChunk_VerifyExitCodes(t *testing.T) {
	mem, h := verifyCLIFixture(t)
	filer := []string{"--volume", "v1", "--chunk-filer", "filer:18888"}

	code, out, errs := runChunk(append([]string{"verify"}, filer...)...)
	res := decodeVerify(t, out)
	if code != 0 || !res.Complete || res.Depth != chunkstore.VerifyBindings || len(res.Findings) != 0 || res.ChunksChecked != 2 ||
		!strings.Contains(out, `"findings":[]`) || len(res.HeadGens) != 1 || res.HeadGens[0] != h.Gen {
		t.Fatalf("clean verify: %d %q %q", code, out, errs)
	}
	code, out, errs = runChunk(append([]string{"verify", "--depth", "data", "--timeout", "10s", "--bytes-per-sec", "0"}, filer...)...)
	if res = decodeVerify(t, out); code != 0 || res.DistinctChunksRead != 2 || res.BytesRead != 2*8*4096 {
		t.Fatalf("clean data verify: %d %q %q", code, out, errs)
	}

	// A binding gone: completed with a finding.
	own := strings.TrimPrefix(h.Chunks[1].Key, "@v1/")
	if err := mem.DeleteEntry(context.Background(), path.Join(defaultChunkPathCLI, "v1", "keys"), url.PathEscape(own), nil); err != nil {
		t.Fatal(err)
	}
	code, out, errs = runChunk(append([]string{"verify"}, filer...)...)
	res = decodeVerify(t, out)
	if chunkExitFindings != 3 || code != chunkExitFindings || !res.Complete || len(res.Findings) != 1 || res.Findings[0].Class != chunkstore.FindingBindingMissing ||
		res.FindingsByClass[chunkstore.FindingBindingMissing] != 1 || !strings.Contains(errs, "1 findings") {
		t.Fatalf("verify with a missing binding: %d %q %q", code, out, errs)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[1], `{"volume":"v1"`) || !strings.Contains(lines[1], `"class":"BINDING_MISSING"`) || lines[2] != "]}" {
		t.Fatalf("findings are not one per line:\n%s", out)
	}

	// Could not complete: the error body, with what was covered.
	code, out, _ = runChunk("verify", "--volume", "nope", "--chunk-filer", "filer:18888")
	if e := chunkErrorOut(t, out); code != chunkExitFailed || e.Code != chunkstore.CodeNotChunkVolume || !strings.Contains(out, `"verify":{`) ||
		!strings.Contains(out, `"incomplete":"error"`) {
		t.Fatalf("verify of a missing volume: %d %q", code, out)
	}

	for _, args := range [][]string{
		{"verify", "--chunk-filer", "f:1"},                                      // no volume
		{"verify", "--volume", "v1"},                                            // no filer
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "--depth", "all"},  // bad depth
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "--depth", "data"}, // data without --timeout
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "--snapshot", "s1", "--all-snapshots"},
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "--concurrency", "0"},
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "--bytes-per-sec", "-1"},
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "extra"},
		{"verify", "--volume", "v1", "--chunk-filer", "f:1", "--bogus"},
	} {
		code, out, _ := runChunk(args...)
		if code != 2 || chunkErrorOut(t, out).Code != codeBadRequestCLI {
			t.Fatalf("%v: exit %d %q; want 2 and bad_request", args, code, out)
		}
	}
	if code, out, _ := runChunk("verify", "--volume", "v1", "--chunk-filer", "f:1", "--depth", "data"); !strings.Contains(out, "--timeout") {
		t.Fatalf("data without --timeout: %d %q; want the reason", code, out)
	}
}

// unreadableStore fails every read of one needle with an error that is not
// "not found" (a volume server answering 503).
type unreadableStore struct {
	*manifestFixtureFiler
	fid string
}

func (s unreadableStore) Get(ctx context.Context, fid string) ([]byte, error) {
	if fid == s.fid {
		return nil, fmt.Errorf("chunkstore: read %s: http 503 from volume:8080", fid)
	}
	return s.manifestFixtureFiler.Get(ctx, fid)
}

// A chunk that cannot be read is READ_ERROR (not verified), and the pass
// exits 3: it did not verify everything.
func TestOpsChunk_VerifyReadErrorExits3(t *testing.T) {
	mem, h := verifyCLIFixture(t)
	own := strings.TrimPrefix(h.Chunks[0].Key, "@v1/")
	e, err := mem.LookupEntry(context.Background(), path.Join(defaultChunkPathCLI, "v1", "keys"), url.PathEscape(own))
	if err != nil {
		t.Fatal(err)
	}
	store := unreadableStore{mem, e.GetChunks()[0].GetFileId()}
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		return store, func() error { return nil }, nil
	}
	code, out, errs := runChunk("verify", "--volume", "v1", "--chunk-filer", "filer:18888", "--depth", "data", "--timeout", "10s")
	res := decodeVerify(t, out)
	if code != chunkExitFindings || !res.Complete || len(res.Findings) != 1 || res.Findings[0].Class != chunkstore.FindingReadError ||
		res.ChunksNotRead != 1 || res.ReadRetries != 2 || !strings.Contains(errs, "not verified") {
		t.Fatalf("verify with an unreadable chunk: %d %q %q", code, out, errs)
	}
}

// verify writes nothing, yet it is in the set that runs the single-filer
// start check (a split filer view would give false findings), and it refuses
// the way the writing commands do (TestOpsChunk_WritingCommandsRunTheSingleFilerCheck).
func TestOpsChunk_VerifyRunsTheSingleFilerCheck(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck := dialChunkFiler, checkSingleFiler
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler = prevDial, prevCheck })
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	checks := 0
	checkSingleFiler = func(context.Context, chunkstore.Filer) error {
		checks++
		return chunkstore.ErrNotSingleFiler
	}
	filer := []string{"--chunk-filer", "filer:18888"}
	for name, args := range map[string][]string{
		"verify":              append([]string{"ops", "chunk", "verify", "--volume", "v1"}, filer...),
		"verify --depth data": append([]string{"ops", "chunk", "verify", "--volume", "v1", "--depth", "data", "--timeout", "10s"}, filer...),
	} {
		before := checks
		var out, errb bytes.Buffer
		code := run(args, &out, &errb)
		if checks != before+1 {
			t.Errorf("%s: start check ran %d times; want once", name, checks-before)
		}
		if code == 0 || !strings.Contains(out.String()+errb.String(), "exactly one filer") {
			t.Errorf("%s: exit %d, out %q err %q; want the single-filer refusal", name, code, out.String(), errb.String())
		}
	}
}

// The start check runs within verify's --timeout, as for the writing
// commands (TestOpsChunk_StartCheckIsBoundedByTheTimeout): a check stalled on
// its filer or master ends verify at the timeout, as a retryable refusal.
func TestOpsChunk_VerifyStartCheckIsBoundedByTheTimeout(t *testing.T) {
	mem := chunkstore.NewMemFiler()
	prevDial, prevCheck := dialChunkFiler, checkSingleFiler
	t.Cleanup(func() { dialChunkFiler, checkSingleFiler = prevDial, prevCheck })
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	checkSingleFiler = func(ctx context.Context, _ chunkstore.Filer) error {
		<-ctx.Done()
		return ctx.Err()
	}
	args := []string{"ops", "chunk", "verify", "--volume", "v1", "--chunk-filer", "filer:18888", "--timeout", "200ms"}
	done := make(chan struct{})
	var out, errb bytes.Buffer
	var code int
	go func() { code = run(args, &out, &errb); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("a stalled start check held verify past its 200ms --timeout")
	}
	if code == 0 || !strings.Contains(out.String(), "remote_unavailable") || !strings.Contains(out.String(), `"retryable":true`) {
		t.Errorf("verify: exit %d, out %q; want a retryable remote_unavailable refusal", code, out.String())
	}
}
