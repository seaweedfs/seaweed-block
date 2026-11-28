package durable

import (
	"bytes"
	"context"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// slowPoolFiler gives every lookup of a chunk object (pool/) a latency,
// bounded by its ctx as a remote filer's is.
type slowPoolFiler struct {
	*chunkstore.MemFiler
	delay atomic.Int64 // nanoseconds; 0 = none
}

func (f *slowPoolFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if d := time.Duration(f.delay.Load()); d > 0 && strings.Contains(path.Join(dir, name), "/pool/") {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.MemFiler.LookupEntry(ctx, dir, name)
}

// Through the provider's own attach (the remote wrapped as blockvolume wraps
// it), a cut whose commit checks objects slower than one HEAD update (2 s)
// still publishes: the check runs with the cut's budget, before the update.
func TestProviderCutCommitsWhenItsObjectCheckOutlastsTheHeadUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("lookups take 2.5 s each")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	filer := &slowPoolFiler{MemFiler: chunkstore.NewMemFiler()}
	b := &kzLayoutBlobs{b: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(filer, b, "/c1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 256 * 4096, BlockSize: 4096, ChunkBytes: 16 * 4096}); err != nil {
		t.Fatal(err)
	}
	remote := &kzLayout{cat: cat, ref: ref, blobs: b}
	p := kzProvider(t, t.TempDir(), remote)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	for c := 0; c < 16; c++ {
		if _, err := p.Backend("v1").Write(ctx, int64(c)*16*4096, bytes.Repeat([]byte{byte(c + 1)}, 4096)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	before, _, err := cat.ReadHead(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	filer.delay.Store(int64(2500 * time.Millisecond))
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatalf("capture whose object check outlasts one HEAD update: %v", err)
	}
	after, _, err := cat.ReadHead(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.Current == nil || before.Current != nil && *after.Current == *before.Current {
		t.Fatalf("HEAD's root did not move: before %v, after %v", before.Current, after.Current)
	}
}

// Every optional interface the chunk layer asserts on its remote, and the
// holder's LayoutRemote implements, is still implemented by the remote as
// the provider wraps it. The list is checked against the chunk layer's
// assertions in storage, read from its source: a new assertion fails here
// until it is listed.
func TestProviderWrappedRemoteKeepsTheLayoutRemotesOptionalInterfaces(t *testing.T) {
	remote := newKZLayout(t)
	lr, err := chunkstore.NewLayoutRemote(remote.cat, remote.ref, layout.HolderID{Epoch: 1, Holder: "a", ClaimID: "k"})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := storage.ChunkRemote(&layoutBoundRemote{closeBoundRemote: &closeBoundRemote{ChunkRemote: lr, done: context.Background()}, keyer: lr})
	checks := map[string]func(storage.ChunkRemote) bool{
		"ChunkHeadRootReader": func(r storage.ChunkRemote) bool { _, ok := r.(storage.ChunkHeadRootReader); return ok },
		"ChunkCommitChecker":  func(r storage.ChunkRemote) bool { _, ok := r.(storage.ChunkCommitChecker); return ok },
		"ChunkKeyer":          func(r storage.ChunkRemote) bool { _, ok := r.(storage.ChunkKeyer); return ok },
		"ChunkPutRounds":      func(r storage.ChunkRemote) bool { _, ok := r.(storage.ChunkPutRounds); return ok },
	}
	for name, has := range checks {
		if has(lr) && !has(wrapped) {
			t.Errorf("the provider's wrapped remote drops %s, which its LayoutRemote implements", name)
		}
	}
	asserted := map[string]bool{}
	files, err := filepath.Glob("../../storage/*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("storage sources: %v (%d files)", err, len(files))
	}
	re := regexp.MustCompile(`remote\.\((\w+)\)`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(src), -1) {
			asserted[strings.ToUpper(m[1][:1])+m[1][1:]] = true
		}
	}
	for name := range asserted {
		if checks[name] == nil {
			t.Errorf("storage asserts %s on its remote; list it here", name)
		}
	}
	if len(asserted) == 0 {
		t.Fatal("found no remote assertion in storage: the scan is broken")
	}
	if _, ok := storage.ChunkRemote(lr).(storage.ChunkCommitChecker); !ok {
		t.Fatal("LayoutRemote no longer implements ChunkCommitChecker")
	}
}
