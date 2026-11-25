package chunkstore

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// headReaders is every function outside tests that reads a volume's HEAD
// (ReadHead, GetHead, GetHeadRoot, getHeadRoot, headExists, or a LookupEntry
// of HEAD), with what it does when HEAD is absent and the probe below that
// pins it. "GAP" marks a reader that takes an absent HEAD for an empty
// one; a change that treats an absent HEAD as unknown flips those probes.
var headReaders = map[string]string{
	// chunkstore: the two raw reads every other reader goes through
	"core/frontend/chunkstore/catalog.go ReadHead -> LookupEntry":              "ok=false, no error (probe ReadHead)",
	"core/frontend/chunkstore/chunk_remote.go getHeadRoot -> LookupEntry":      "zero HEAD, no error (probe GetHead)",
	"core/frontend/chunkstore/catalog.go headExists -> LookupEntry":            "false (probe EnsureVolume)",
	"core/frontend/chunkstore/chunk_remote.go GetHead -> getHeadRoot":          "zero HEAD, no error (probe GetHead)",
	"core/frontend/chunkstore/chunk_remote.go GetHeadRoot -> getHeadRoot":      "zero HEAD, no error (probe GetHeadRoot)",
	"core/frontend/chunkstore/chunk_remote.go CASHead -> getHeadRoot":          "GAP: expected gen 0 creates a HEAD over existing keys (probe CASHead)",
	"core/frontend/chunkstore/catalog.go ensureInitialHead -> headExists":      "GAP: creates an empty gen-1 HEAD over existing keys (probe EnsureVolume)",
	"core/frontend/chunkstore/catalog.go EnsureVolume -> ReadHead":             "GAP: attaches the recreated empty HEAD (probe EnsureVolume)",
	"core/frontend/chunkstore/catalog.go fork -> ReadHead":                     "replay falls through and publishes the fork-time HEAD (probe ForkReplay)",
	"core/frontend/chunkstore/catalog.go AbortFork -> ReadHead":                "GAP: exclusive-creates an aborted HEAD (probe AbortFork)",
	"core/frontend/chunkstore/gc.go run -> GetHead":                            "GAP: zero HEAD roots nothing; the volume's keys become candidates (probe GCMark)",
	"core/frontend/chunkstore/gc_copy.go SweepCopies -> ReadHead":              "after AbortFork: settles from the HEAD it created (probe AbortFork)",
	"core/frontend/chunkstore/restore.go MarkIndependent -> ReadHead":          "refuses (not a restore / no HEAD) (probe MarkIndependent)",
	"core/frontend/chunkstore/share_guard.go beginShare -> ReadHead":           "refuses outcome_unknown: the HEAD to share is no longer current",
	"core/frontend/chunkstore/usage.go Usage -> ReadHead":                      "GAP: reports an empty image, no error (probe Usage)",
	"core/frontend/chunkstore/verify.go readHead -> LookupEntry":               "finding ROOT_INVALID (probe Verify)",
	"core/frontend/chunkstore/layout/remove.go RemoveUnreachable -> ReadHead":  "refuses ErrHeadAbsent and writes nothing to the objects (probe LayoutRemoval)",
	"core/frontend/chunkstore/layout/head.go runTransition -> ReadHead":        "refuses ErrHeadAbsent and writes nothing; no layout transition creates a HEAD (probe LayoutTransition)",
	"core/frontend/chunkstore/verify.go run -> LookupEntry":                    "finding ROOT_INVALID (probe Verify)",
	"core/frontend/chunkstore/volume_delete.go DeleteVolume -> ReadHead":       "operator delete writes a deleting HEAD from nothing and completes (probe DeleteVolume)",
	"core/frontend/chunkstore/volume_delete.go markHeadDeleting -> ReadHead":   "zero HEAD: creates the deleting HEAD (probe DeleteVolume)",
	"core/frontend/chunkstore/volume_delete.go commitHeadPurging -> ReadHead":  "reads the HEAD markHeadDeleting wrote (probe DeleteVolume)",
	"core/frontend/chunkstore/volume_delete.go finishHeadDeleted -> ReadHead":  "reads the HEAD markHeadDeleting wrote (probe DeleteVolume)",
	"core/frontend/chunkstore/volume_delete.go revertHeadDeleting -> ReadHead": "reads the HEAD markHeadDeleting wrote (probe DeleteVolume)",
	"core/frontend/chunkstore/volume_delete.go volumeGone -> ReadHead":         "absent HEAD = not gone (probe DeleteVolume)",
	// above the chunk store: they read through storage.ChunkRemote GetHead /
	// GetHeadRoot, so an absent HEAD reaches them as the zero HEAD (probe GetHead)
	"cmd/sw-block/chunk.go chunkShow -> ReadHead":                        "ok=false: reports no HEAD (probe ReadHead)",
	"core/frontend/durable/provider_chunk.go GetHead -> GetHead":         "zero HEAD (probe GetHead)",
	"core/frontend/durable/provider_chunk.go GetHeadRoot -> GetHead":     "zero HEAD (probe GetHead)",
	"core/frontend/durable/provider_chunk.go GetHeadRoot -> GetHeadRoot": "zero HEAD (probe GetHeadRoot)",
	"core/frontend/durable/provider_chunk.go newChunkLayer -> GetHead":   "zero HEAD: a fresh lineage (probe GetHead)",
	"core/storage/chunk_head_belt.go confirmChunkHead -> GetHead":        "gen mismatch: fenced, HEAD publication displaced (reads the zero HEAD, probe GetHead)",
	"core/storage/chunk_head_belt.go confirmChunkHead -> GetHeadRoot":    "gen mismatch: fenced, HEAD publication displaced (probe GetHeadRoot)",
	"core/storage/chunk_head_belt.go readChunkHead -> GetHead":           "zero HEAD (probe GetHead)",
	"core/storage/chunk_layer.go attachChunkLayerImpl -> GetHead":        "zero HEAD: attaches as a new lineage (probe GetHead)",
	"core/storage/chunk_restore.go ClaimChunkHead -> GetHead":            "GAP: the zero HEAD passes the fence check (gen 0), the claim then CASes from gen 0 (probe CASHead)",
}

// TestHeadReaders_Enumeration lists every HEAD reader in core/ and cmd/ from
// the source and fails when one is added or removed without updating
// headReaders (and its probe).
func TestHeadReaders_Enumeration(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	for _, top := range []string{"core", "cmd"} {
		err := filepath.Walk(filepath.Join(root, top), func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains(p, "filerpb") || strings.Contains(p, "masterpb") {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, p, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, p)
			rel = filepath.ToSlash(rel)
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					name := ""
					switch x := call.Fun.(type) {
					case *ast.SelectorExpr:
						name = x.Sel.Name
					case *ast.Ident:
						name = x.Name
					}
					hit := name == "ReadHead" || name == "GetHead" || name == "GetHeadRoot" || name == "getHeadRoot" || name == "headExists"
					if name == "LookupEntry" {
						for _, a := range call.Args {
							if id, ok := a.(*ast.Ident); ok && id.Name == "headName" {
								hit = true
							}
						}
					}
					if hit {
						found[rel+" "+fn.Name.Name+" -> "+name] = true
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var added, removed []string
	for k := range found {
		if _, ok := headReaders[k]; !ok {
			added = append(added, k)
		}
	}
	for k := range headReaders {
		if !found[k] {
			removed = append(removed, k)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	if len(added) != 0 || len(removed) != 0 {
		t.Fatalf("HEAD readers changed: classify each new reader's absent-HEAD behaviour in headReaders with a probe.\nnew: %v\ngone: %v", added, removed)
	}
}

// absentHeadFixture: volume v1 is live (VOLUME), holds a bound key, and its
// HEAD entry was removed (pruned by a stock tool, or by hand).
func absentHeadFixture(t *testing.T) (*fakeFiler, gcBlobs, *Catalog, *Collector, *time.Time, *ChunkRemote) {
	t.Helper()
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, catBase+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("published data")); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteEntry(ctx, catBase+"/v1", headName, nil); err != nil {
		t.Fatal(err)
	}
	return f, b, c, g, now, r
}

// headReaderForkFixture: v1 holds a snapshot s1; v2 is a directory a fork
// can target.
func headReaderForkFixture(t *testing.T) (*fakeFiler, *fakeBlobs) {
	t.Helper()
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "s"}, h); err != nil {
		t.Fatal(err)
	}
	return f, b
}

// TestHeadReaders_AbsentHeadProbes pins, per reader, what today's code does
// when HEAD is absent (see headReaders).
func TestHeadReaders_AbsentHeadProbes(t *testing.T) {
	ctx := context.Background()
	t.Run("ReadHead", func(t *testing.T) {
		_, _, c, _, _, _ := absentHeadFixture(t)
		if h, ok, err := c.ReadHead(ctx, "v1"); err != nil || ok || h.Gen != 0 {
			t.Fatalf("ReadHead: %+v %t %v; want absent, no error", h, ok, err)
		}
	})
	t.Run("GetHead", func(t *testing.T) {
		_, _, _, _, _, r := absentHeadFixture(t)
		if h, err := r.GetHead(ctx); err != nil || h.Gen != 0 {
			t.Fatalf("GetHead: %+v %v; want the zero HEAD", h, err)
		}
	})
	t.Run("GetHeadRoot", func(t *testing.T) {
		_, _, _, _, _, r := absentHeadFixture(t)
		if h, err := r.GetHeadRoot(ctx); err != nil || h.Gen != 0 {
			t.Fatalf("GetHeadRoot: %+v %v; want the zero HEAD", h, err)
		}
	})
	t.Run("CASHead", func(t *testing.T) { // GAP
		_, _, _, _, _, r := absentHeadFixture(t)
		gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "h", Chunks: map[uint64]storage.ChunkRef{}})
		if err != nil || gen != 1 {
			t.Fatalf("CASHead from gen 0 over keys: %d %v; today it creates gen 1", gen, err)
		}
	})
	t.Run("EnsureVolume", func(t *testing.T) { // GAP
		_, _, c, _, _, _ := absentHeadFixture(t)
		if _, err := c.EnsureVolume(ctx, "v1", catGeo); err != nil {
			t.Fatalf("attach over keys without HEAD: %v; today it attaches", err)
		}
		if h, ok, _ := c.ReadHead(ctx, "v1"); !ok || h.Gen != 1 || len(h.Chunks) != 0 {
			t.Fatalf("HEAD after attach: %+v %t; today an empty gen-1 HEAD", h, ok)
		}
	})
	t.Run("Usage", func(t *testing.T) { // GAP
		_, _, c, _, _, _ := absentHeadFixture(t)
		u, err := c.Usage(ctx, "v1")
		if err != nil || u.LiveChunks != 0 {
			t.Fatalf("Usage: %+v %v; today an empty image", u, err)
		}
	})
	t.Run("GCMark", func(t *testing.T) { // GAP
		_, _, _, g, now, _ := absentHeadFixture(t)
		if _, err := g.Run(ctx); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
		res, err := g.Run(ctx)
		if err != nil || res.Candidates == 0 {
			t.Fatalf("GC over an absent HEAD: %+v %v; today the volume's keys are candidates", res, err)
		}
	})
	t.Run("Verify", func(t *testing.T) {
		_, _, c, _, _, _ := absentHeadFixture(t)
		res, err := c.Verify(ctx, "v1", VerifyOptions{})
		if err != nil || res.FindingsByClass[FindingRootInvalid] == 0 {
			t.Fatalf("Verify: %+v %v; want a ROOT_INVALID finding", res, err)
		}
	})
	t.Run("MarkIndependent", func(t *testing.T) {
		_, _, c, _, _, _ := absentHeadFixture(t)
		if _, err := c.MarkIndependent(ctx, "v1"); err == nil {
			t.Fatal("MarkIndependent of a volume without HEAD succeeded")
		}
	})
	t.Run("DeleteVolume", func(t *testing.T) {
		_, _, c, _, _, _ := absentHeadFixture(t)
		res, err := c.DeleteVolume(ctx, "v1", nil)
		if err != nil || !res.Completed {
			t.Fatalf("operator delete over an absent HEAD: %+v %v; today it completes", res, err)
		}
		if h, ok, _ := c.ReadHead(ctx, "v1"); !ok || h.State != storage.ChunkHeadDeleted {
			t.Fatalf("HEAD after the delete: %+v %t; want deleted", h, ok)
		}
	})
	t.Run("AbortFork", func(t *testing.T) { // GAP
		_, _, c, _, _, _ := absentHeadFixture(t)
		claimed, err := c.AbortFork(ctx, "v1")
		if err != nil || !claimed {
			t.Fatalf("AbortFork over an absent HEAD: %t %v; today it creates an aborted HEAD", claimed, err)
		}
	})
	t.Run("LayoutTransition", func(t *testing.T) {
		st := &absentLayoutHead{}
		if _, err := layout.RunBegin(ctx, st, layout.HolderID{Epoch: 1, Holder: "h"}, "s"); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("layout transition over an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if st.updates != 0 {
			t.Fatalf("layout transition over an absent HEAD wrote %d times", st.updates)
		}
	})
	t.Run("LayoutRemoval", func(t *testing.T) {
		r, err := layout.NewReader("/swblock/chunks", "v1", func(context.Context, string) ([]byte, bool, error) { return nil, false, nil })
		if err != nil {
			t.Fatal(err)
		}
		pool := &countingPool{}
		if _, err := layout.RemoveUnreachable(ctx, &absentLayoutHead{}, r, pool, layout.HolderID{Epoch: 1, Holder: "h"}, "c", 0, []layout.ChunkName{layout.NameChunk(1, []byte("x"))}); !errors.Is(err, layout.ErrHeadAbsent) {
			t.Fatalf("layout removal over an absent HEAD: %v; want ErrHeadAbsent", err)
		}
		if pool.writes != 0 {
			t.Fatalf("layout removal over an absent HEAD wrote %d times to the objects", pool.writes)
		}
	})
	t.Run("ForkReplay", func(t *testing.T) {
		f, b := headReaderForkFixture(t)
		c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
		if err != nil {
			t.Fatal(err)
		}
		req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f1"}
		if _, err := c.Fork(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := f.DeleteEntry(ctx, catBase+"/v2", headName, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Fork(ctx, req); err != nil {
			t.Fatalf("fork replay over an absent target HEAD: %v; today it republishes", err)
		}
		if h, ok, _ := c.ReadHead(ctx, "v2"); !ok || h.Gen != 1 {
			t.Fatalf("target HEAD after the replay: %+v %t; today the fork-time HEAD again", h, ok)
		}
	})
}

// absentLayoutHead is a layout HEAD store whose HEAD is absent.
type absentLayoutHead struct{ updates int }

func (a *absentLayoutHead) ReadHead(context.Context) (layout.Head, bool, error) {
	return layout.Head{}, false, nil
}

func (a *absentLayoutHead) UpdateHead(context.Context, uint64, layout.Head) error {
	a.updates++
	return nil
}

// countingPool is a layout object store that counts writes and holds
// nothing.
type countingPool struct{ writes int }

func (p *countingPool) Stat(context.Context, string) (layout.ObjectMeta, bool, error) {
	return layout.ObjectMeta{}, false, nil
}
func (p *countingPool) Create(context.Context, string, []byte, string) error { p.writes++; return nil }
func (p *countingPool) Reuse(context.Context, string, layout.ObjectMeta, string) error {
	p.writes++
	return nil
}
func (p *countingPool) Mark(context.Context, string, layout.ObjectMeta, string) error {
	p.writes++
	return nil
}
func (p *countingPool) DeleteMarked(context.Context, string, string) error { p.writes++; return nil }
