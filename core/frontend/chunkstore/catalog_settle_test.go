package chunkstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

var errTransport = errors.New("fake: transport error")

func sha256Of(b []byte) [32]byte { return sha256.Sum256(b) }

func intentsOf(t *testing.T, f *fakeFiler, vol string) []storage.ChunkIntent {
	t.Helper()
	r, _ := NewChunkRemote(f, newFakeBlobs(), catBase+"/"+vol)
	got, err := r.ListIntents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func sortedKeysOf(h storage.ChunkHead, qualify string) []string {
	var out []string
	for _, ref := range h.Chunks {
		k := ref.Key
		if qualify != "" && !strings.HasPrefix(k, "@") {
			k = "@" + qualify + "/" + k
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return strings.Join(a, "\x00") == strings.Join(b, "\x00")
}

// An uncertain snapshot create keeps the intent that protects the
// snapshot's keys; a retry with the same request id settles it and removes
// every intent of the operation (review of #111, P1).
func TestCatalog_SnapshotUncertainCreateKeepsIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	f.failCreate = func(p string) error {
		if p == catBase+"/v1/snaps/s1" {
			return errTransport
		}
		return nil
	}
	_, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h)
	if CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("uncertain create: %v, want outcome_unknown", err)
	}
	in := intentsOf(t, f, "v1")
	if len(in) != 1 || !sameStrings(in[0].Keys, sortedKeysOf(h, "")) {
		t.Fatalf("after an uncertain create the intent must still protect the snapshot's keys: %+v", in)
	}
	f.failCreate = nil
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 0 {
		t.Fatalf("a settled snapshot left %d intents", len(in))
	}
}

// A create that applied but lost its response is confirmed by the re-read.
func TestCatalog_SnapshotAppliedResponseLostIsConfirmed(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	f.failCreateAfterApply = func(p string) error {
		if p == catBase+"/v1/snaps/s1" {
			return errTransport
		}
		return nil
	}
	info, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h)
	if err != nil || info.HeadGen != h.Gen {
		t.Fatalf("applied create with a lost response: %+v %v", info, err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 0 {
		t.Fatalf("a confirmed snapshot left %d intents", len(in))
	}
}

// The same for the fork's HEAD create, with the qualified keys protected in
// the target.
func TestFork_UncertainHeadCreateKeepsIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	f.failCreate = func(p string) error {
		if p == catBase+"/v2/HEAD" {
			return errTransport
		}
		return nil
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("uncertain HEAD create: %v, want outcome_unknown", err)
	}
	in := intentsOf(t, f, "v2")
	if len(in) != 1 || !sameStrings(in[0].Keys, sortedKeysOf(h, "v1")) {
		t.Fatalf("after an uncertain HEAD create the intent must still protect the qualified keys: %+v", in)
	}
	f.failCreate = nil
	res, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"})
	if err != nil || res.Replayed {
		t.Fatalf("retry: %+v %v", res, err)
	}
	if in := intentsOf(t, f, "v2"); len(in) != 0 {
		t.Fatalf("a settled fork left %d intents", len(in))
	}

	// Applied, response lost: confirmed by the re-read.
	f.failCreateAfterApply = func(p string) error {
		if p == catBase+"/v3/HEAD" {
			return errTransport
		}
		return nil
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "g"}); err != nil {
		t.Fatalf("applied HEAD create with a lost response: %v", err)
	}
	if in := intentsOf(t, f, "v3"); len(in) != 0 {
		t.Fatalf("a confirmed fork left %d intents", len(in))
	}
}

// An incomplete fork resumes only onto the image its first attempt bound,
// and a request id reused with other parameters is refused (P1).
func TestFork_IncompleteRetryBoundToOriginalImage(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s2", RequestID: "r2"}, h); err != nil {
		t.Fatal(err)
	}
	f.failCreate = func(p string) error {
		if p == catBase+"/v2/HEAD" {
			return errTransport
		}
		return nil
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("first attempt: %v", err)
	}
	f.failCreate = nil

	// The same request id with another snapshot is refused.
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeTargetExists {
		t.Fatalf("request id reused for another snapshot: %v, want target_exists", err)
	}

	// s1 is deleted and recreated over a newer source image.
	f.mu.Lock()
	delete(f.entries, catBase+"/v1/snaps/s1")
	f.mu.Unlock()
	r1, _ := NewChunkRemote(f, b, catBase+"/v1")
	if err := r1.Put(ctx, "chunks/e1/c5/new", chunkBytes(0x55)); err != nil {
		t.Fatal(err)
	}
	next := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 99, Chunks: map[uint64]storage.ChunkRef{}}
	for k, v := range h.Chunks {
		next.Chunks[k] = v
	}
	next.Chunks[5] = storage.ChunkRef{Key: "chunks/e1/c5/new", SHA256: sha256Of(chunkBytes(0x55))}
	gen, err := r1.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: 1}, next)
	if err != nil {
		t.Fatal(err)
	}
	next.Gen = gen
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r-new"}, next); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeTargetExists {
		t.Fatalf("resume after the snapshot changed image: %v, want target_exists", err)
	}
	if _, err := f.LookupEntry(ctx, catBase+"/v2", "HEAD"); err == nil {
		t.Fatal("a fork resumed onto another image and wrote HEAD")
	}
}

// A completed fork replays its recorded result without reading the source:
// after the source snapshot is deleting, and after the target advanced (P2).
func TestFork_CompletedReplayIndependentOfSource(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	first, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	again, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"})
	if err != nil || !again.Replayed {
		t.Fatalf("replay after the source snapshot started deleting: %+v %v", again, err)
	}
	again.Replayed = false
	if again != first {
		t.Fatalf("replayed %+v, recorded %+v", again, first)
	}
	// The target's holder advances HEAD; the replay still reports the fork.
	r2, _ := NewChunkRemote(f, b, catBase+"/v2")
	cur, _ := r2.GetHead(ctx)
	adv := cur
	adv.Epoch, adv.Holder, adv.RemoteLSN = 1, "host-2", 50
	if _, err := r2.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, adv); err != nil {
		t.Fatal(err)
	}
	later, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"})
	if err != nil || !later.Replayed || later.HeadGen != 1 || later.RemoteLSN != first.RemoteLSN || later.SourceGen != first.SourceGen {
		t.Fatalf("replay after the target advanced: %+v %v", later, err)
	}
}
