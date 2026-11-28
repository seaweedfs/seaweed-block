package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A cut whose commit checks more objects than the filer can look up within
// one HEAD update (2 s) still commits: the check that every new chunk is
// this session's runs with the cut's budget, before the update. Here every
// chunk lookup takes 2.5 s, as a slow or remote filer's.
func TestCutCommitsWhenItsObjectCheckOutlastsTheHeadUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("lookups take 2.5 s each")
	}
	lh := newLayoutHost(t)
	s, l, _ := lh.attach("a", true, true)
	t.Cleanup(func() { _ = l.Close(); _ = s.Close() })
	w := map[uint32][]byte{}
	for c := uint32(0); c < lrBlocks/lrChunkBlocks; c++ {
		w[c*lrChunkBlocks] = block(byte(c + 1))
	}
	writeSync(t, s, w)
	lh.ff.slowLookup = func(p string) time.Duration {
		if strings.Contains(p, "/pool/") {
			return 2500 * time.Millisecond
		}
		return 0
	}
	before := lh.head()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := l.Cut(ctx); err != nil {
		t.Fatalf("cut whose object check outlasts one HEAD update: %v", err)
	}
	after := lh.head()
	if after.Current == nil || before.Current != nil && *after.Current == *before.Current {
		t.Fatalf("HEAD's root did not move: before %v, after %v", before.Current, after.Current)
	}
}

// c1Remote wraps the holder's LayoutRemote under the chunk layer: check
// replaces CheckCommit (nil: the real one), and pool lookups made inside
// CASHead are counted.
type c1Remote struct {
	*LayoutRemote
	check  func(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) error
	inCAS  atomic.Bool
	casGot atomic.Int64
	ff     *fakeFiler
}

func (w *c1Remote) CheckCommit(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) error {
	if w.check != nil {
		return w.check(ctx, e, n)
	}
	return w.LayoutRemote.CheckCommit(ctx, e, n)
}

func (w *c1Remote) CASHead(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) (uint64, error) {
	w.inCAS.Store(true)
	defer w.inCAS.Store(false)
	return w.LayoutRemote.CASHead(ctx, e, n)
}

// c1Cut writes one block in every chunk of a fresh volume and cuts it once
// through the chunk layer over a c1Remote with check.
func c1Cut(t *testing.T, check func(w *c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error) (*layoutHost, *c1Remote, error) {
	t.Helper()
	lh := newLayoutHost(t)
	s, err := storage.CreateWALStoreWithOptions(filepath.Join(lh.dir, "a"), lrBlocks, 4096, storage.WALStoreOptions{ChunkBlocks: lrChunkBlocks})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	w := &c1Remote{LayoutRemote: r, ff: lh.ff}
	if check != nil {
		w.check = check(w)
	}
	l, err := storage.AttachChunkLayer(s, w, storage.ChunkLayerConfig{ChunkBlocks: lrChunkBlocks, Epoch: lh.me.Epoch, Holder: lh.me.Holder, FreshFromHead: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close(); _ = s.Close() })
	blocks := map[uint32][]byte{}
	for c := uint32(0); c < lrBlocks/lrChunkBlocks; c++ {
		blocks[c*lrChunkBlocks] = block(byte(c + 1))
	}
	writeSync(t, s, blocks)
	lh.ff.afterLookup = func(p string) {
		if strings.Contains(p, "/pool/") && w.inCAS.Load() {
			w.casGot.Add(1)
		}
	}
	_, err = l.Cut(context.Background())
	return lh, w, err
}

// With the pre-check passed for its (session, root), the CAS looks up no
// chunk; the commit lands.
func TestCASSkipsTheLookupsCheckedForItsSessionAndRoot(t *testing.T) {
	_, w, err := c1Cut(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := w.casGot.Load(); n != 0 {
		t.Fatalf("CAS looked up %d chunks after a passed pre-check of its own root", n)
	}
}

// A CAS with no passed pre-check for its (session, root) runs the lookups
// itself, as before the change: here the pre-check is not run at all.
func TestCASWithNoPassedCheckDoesTheLookups(t *testing.T) {
	_, w, err := c1Cut(t, func(*c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error {
		return func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error { return nil }
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := w.casGot.Load(); n == 0 {
		t.Fatal("CAS with no passed pre-check looked up no chunk")
	}
}

// A pre-check of another root (here the image without its last chunk) is
// not a check of the CAS's root: the CAS does the lookups.
func TestCASAfterACheckOfAnotherRootDoesTheLookups(t *testing.T) {
	_, w, err := c1Cut(t, func(w *c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error {
		return func(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) error {
			other := n
			other.Chunks = map[uint64]storage.ChunkRef{}
			last := uint64(0)
			for i := range n.Chunks {
				last = max(last, i)
			}
			for i, ref := range n.Chunks {
				if i != last {
					other.Chunks[i] = ref
				}
			}
			return w.LayoutRemote.CheckCommit(ctx, e, other)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := w.casGot.Load(); n == 0 {
		t.Fatal("CAS after a pre-check of another root looked up no chunk")
	}
}

// Between the pre-check and the update the session stays open, so a
// cleaning cannot begin (ErrBusy): nothing can remove what was checked.
func TestBeginCleanBetweenTheCheckAndTheUpdateIsBusy(t *testing.T) {
	var cleanErr error
	lh, _, err := c1Cut(t, func(w *c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error {
		return func(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) error {
			if err := w.LayoutRemote.CheckCommit(ctx, e, n); err != nil {
				return err
			}
			s, _ := lhStore(w)
			_, cleanErr = layout.RunBeginClean(ctx, s, w.LayoutRemote.me, "c-between")
			return nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(cleanErr, layout.ErrBusy) {
		t.Fatalf("BeginClean between the check and the update: %v, want ErrBusy", cleanErr)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle || h.Current == nil {
		t.Fatalf("after the commit: HEAD %+v", h.Activity)
	}
}

// lhStore is the HEAD store w's remote commits through.
func lhStore(w *c1Remote) (layout.HeadStore, error) { return w.LayoutRemote.st, nil }

// Only LayoutRemote.CASHead may tell RunCommit that the objects were
// checked, and only for the (session, root) CheckCommit passed: a new site
// that sets ObjectsChecked fails here until it is reviewed and listed.
func TestObjectsCheckedIsSetOnlyByCASHead(t *testing.T) {
	var sites []string
	for _, dir := range []string{".", "layout"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for i, line := range strings.Split(string(b), "\n") {
				if strings.Contains(line, "ObjectsChecked =") || strings.Contains(line, "ObjectsChecked:") {
					sites = append(sites, fmt.Sprintf("%s:%d", filepath.ToSlash(f), i+1))
				}
			}
		}
	}
	if len(sites) != 1 || !strings.HasPrefix(sites[0], "layout_remote.go:") {
		t.Fatalf("ObjectsChecked is set at %v; want exactly one site, in LayoutRemote.CASHead", sites)
	}
}

// A pre-check that finds a chunk missing (here one uploaded chunk is gone
// from the filer) ends the cut as a refusal: the CAS that follows answers it
// from HEAD and looks up no chunk, and HEAD keeps its root.
func TestAPreCheckThatFindsAChunkMissingEndsWithNoLookupInTheCAS(t *testing.T) {
	var lost string
	lh, w, err := c1Cut(t, func(w *c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error {
		return func(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) error {
			w.ff.mu.Lock()
			for p := range w.ff.entries {
				if strings.Contains(p, "/pool/") {
					lost = p
					delete(w.ff.entries, p)
					break
				}
			}
			w.ff.mu.Unlock()
			return w.LayoutRemote.CheckCommit(ctx, e, n)
		}
	})
	if lost == "" {
		t.Fatal("no uploaded chunk to remove")
	}
	if err == nil {
		t.Fatalf("cut committed with %s missing", lost)
	}
	if n := w.casGot.Load(); n != 0 {
		t.Fatalf("CAS after a failed pre-check looked up %d chunks", n)
	}
	if h := lh.head(); h.Current != nil {
		t.Fatalf("HEAD moved to %v after a refused commit", h.Current)
	}
}
