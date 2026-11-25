package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// verifyCLIFixture is volume v1 on the content-named layout (VOLUME, HEAD,
// one root naming chunks 0 and 1) on an in-memory filer and blob store; it
// returns the store and chunk 1's pool path.
func verifyCLIFixture(t *testing.T) (*manifestFixtureFiler, string) {
	t.Helper()
	ctx := context.Background()
	mem := newManifestFixtureFiler(chunkstore.NewMemFiler())
	prev := dialChunkFiler
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) {
		return mem, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialChunkFiler = prev })
	root := path.Join(defaultChunkPathCLI, "v1")
	put := func(rel string, b []byte) {
		p := path.Join(root, rel)
		if err := mem.CreateEntry(ctx, path.Dir(p), &filerpb.Entry{Name: path.Base(p), Content: b}, true); err != nil {
			t.Fatal(err)
		}
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	vb, err := layout.Volume{ID: id, Name: "v1", Format: layout.VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	put(layout.VolumeName, vb)
	manifest := map[uint64]layout.Ref{}
	var last string
	for idx := uint64(0); idx < 2; idx++ {
		data := bytes.Repeat([]byte{byte(0x30 + idx)}, 4096)
		c := layout.NameChunk(idx, data)
		put(c.Path(), data)
		manifest[idx] = layout.Ref{Owner: "v1", OwnerID: id, Chunk: c}
		last = c.Path()
	}
	r, segs, err := layout.Build(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for n, b := range segs {
		put(n.Path(), b)
	}
	rn, rb, err := layout.NameRoot(r)
	if err != nil {
		t.Fatal(err)
	}
	put(rn.Path(), rb)
	hb, err := layout.Head{VolumeID: id, Epoch: 1, Holder: "s/r", ClaimID: "c", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	put(layout.HeadName, hb)
	return mem, path.Join(root, last)
}

func decodeVerify(t *testing.T, out string) layout.VerifyResult {
	t.Helper()
	var res layout.VerifyResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("stdout is not a verify result: %q (%v)", out, err)
	}
	return res
}

// verify's exit codes: 0 clean, 3 completed with findings, 1 could not
// complete, 2 usage; the result is one JSON object with one finding per line.
func TestOpsChunk_VerifyExitCodes(t *testing.T) {
	mem, chunk1 := verifyCLIFixture(t)
	filer := []string{"--volume", "v1", "--chunk-filer", "filer:18888"}

	code, out, errs := runChunk(append([]string{"verify"}, filer...)...)
	res := decodeVerify(t, out)
	if code != 0 || !res.Complete || len(res.Findings) != 0 || res.Chunks != 2 || res.Roots != 1 || !strings.Contains(out, `"Findings":[]`) {
		t.Fatalf("clean verify: %d %q %q", code, out, errs)
	}
	code, out, errs = runChunk(append([]string{"verify", "--depth", "data", "--timeout", "10s", "--orphans"}, filer...)...)
	if res = decodeVerify(t, out); code != 0 || !res.Complete || len(res.Findings) != 0 || !res.OrphansChecked {
		t.Fatalf("clean data verify: %d %q %q", code, out, errs)
	}

	// A chunk gone: completed with a finding.
	if err := mem.DeleteEntry(context.Background(), path.Dir(chunk1), path.Base(chunk1), nil); err != nil {
		t.Fatal(err)
	}
	code, out, errs = runChunk(append([]string{"verify"}, filer...)...)
	res = decodeVerify(t, out)
	if chunkExitFindings != 3 || code != chunkExitFindings || !res.Complete || len(res.Findings) != 1 ||
		res.Findings[0].Class != layout.FindMissing || !strings.Contains(errs, "1 findings") {
		t.Fatalf("verify with a missing chunk: %d %q %q", code, out, errs)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], `"Class":"MISSING"`) || lines[2] != "]}" {
		t.Fatalf("findings are not one per line:\n%s", out)
	}

	// Could not complete: a volume without VOLUME/HEAD.
	code, out, errs = runChunk("verify", "--volume", "nope", "--chunk-filer", "filer:18888")
	if res = decodeVerify(t, out); code != chunkExitFailed || res.Complete || !strings.Contains(errs, "incomplete") {
		t.Fatalf("verify of a missing volume: %d %q %q", code, out, errs)
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

// unreadableStore fails every read of one path with an error that is not
// "not found" (a filer answering 503).
type unreadableStore struct {
	*manifestFixtureFiler
	bad string
}

func (s unreadableStore) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if path.Join(dir, name) == s.bad {
		return nil, fmt.Errorf("filer: lookup %s: unavailable (503)", s.bad)
	}
	return s.manifestFixtureFiler.LookupEntry(ctx, dir, name)
}

// A chunk that cannot be read is not judged: the pass could not complete
// (exit 1) and says why.
func TestOpsChunk_VerifyReadErrorIsIncomplete(t *testing.T) {
	mem, chunk1 := verifyCLIFixture(t)
	store := unreadableStore{mem, chunk1}
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) {
		return store, func() error { return nil }, nil
	}
	code, out, errs := runChunk("verify", "--volume", "v1", "--chunk-filer", "filer:18888", "--depth", "data", "--timeout", "10s")
	if code != chunkExitFailed || !strings.Contains(out, "503") || !strings.Contains(errs, "incomplete") {
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
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
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
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
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
