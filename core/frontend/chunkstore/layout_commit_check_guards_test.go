package chunkstore

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// objectsCheckedWriters returns every place in src that writes
// ObjectsChecked (an assignment to a field of that name, or a keyed
// composite-literal element), as "file (Receiver).Method" or "file func".
func objectsCheckedWriters(t *testing.T, file, src string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	var sites []string
	writes := func(owner string, n ast.Node) {
		ast.Inspect(n, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range x.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "ObjectsChecked" {
						sites = append(sites, file+" "+owner)
					}
				}
			case *ast.KeyValueExpr:
				if id, ok := x.Key.(*ast.Ident); ok && id.Name == "ObjectsChecked" {
					sites = append(sites, file+" "+owner)
				}
			}
			return true
		})
	}
	for _, d := range f.Decls {
		if gen, ok := d.(*ast.GenDecl); ok {
			writes("package", gen) // var and const initializers outside any function
			continue
		}
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		owner := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) == 1 {
			typ := fn.Recv.List[0].Type
			if star, ok := typ.(*ast.StarExpr); ok {
				typ = star.X
			}
			if id, ok := typ.(*ast.Ident); ok {
				owner = "(" + id.Name + ")." + fn.Name.Name
			}
		}
		writes(owner, fn.Body)
	}
	return sites
}

// Only LayoutRemote.CASHead may tell RunCommit that the objects were
// checked (and only for the session and root CheckCommit passed). The
// census parses every production Go file of the module and attributes each
// write to its receiver and method; the controls show it finds a write in
// another method, a keyed literal and a multiple assignment.
func TestObjectsCheckedIsWrittenOnlyByCASHead(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "testdata" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if !strings.Contains(string(b), "ObjectsChecked") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		sites = append(sites, objectsCheckedWriters(t, filepath.ToSlash(rel), string(b))...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(sites)
	want := []string{"core/frontend/chunkstore/layout_remote.go (LayoutRemote).CASHead"}
	if fmt.Sprint(sites) != fmt.Sprint(want) {
		t.Fatalf("ObjectsChecked is written at %v; want only %v", sites, want)
	}

	for name, c := range map[string]struct {
		src  string
		want string
	}{
		"another LayoutRemote method": {
			"package chunkstore\nfunc (l *LayoutRemote) Other(req *CommitRequest) { req.ObjectsChecked = true }\n",
			"x.go (LayoutRemote).Other",
		},
		"a keyed literal in another package": {
			"package other\nfunc build() { _ = layout.CommitRequest{Session: \"s\", ObjectsChecked: true} }\n",
			"x.go build",
		},
		"a package-level literal": {
			"package chunkstore\nvar unchecked = layout.CommitRequest{ObjectsChecked: true}\n",
			"x.go package",
		},
		"a multiple assignment": {
			"package chunkstore\nfunc (l *LayoutRemote) CASHead2(req *CommitRequest) { var n int; req.ObjectsChecked, n = true, 1; _ = n }\n",
			"x.go (LayoutRemote).CASHead2",
		},
	} {
		if got := objectsCheckedWriters(t, "x.go", c.src); len(got) != 1 || got[0] != c.want {
			t.Errorf("control %q: census found %v, want [%s]", name, got, c.want)
		}
	}
}

// A check passed in one session does not cover the same root in another:
// after an abort and a new session, the CAS of the same image does the
// lookups.
func TestCASAfterAnAbortAndANewSessionAtTheSameRootDoesTheLookups(t *testing.T) {
	var checkedIn string
	_, w, _ := c1Cut(t, func(w *c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error {
		return func(ctx context.Context, e storage.ChunkHeadVersion, n storage.ChunkHead) error {
			if err := w.LayoutRemote.CheckCommit(ctx, e, n); err != nil {
				return err
			}
			w.LayoutRemote.mu.Lock()
			checkedIn = w.LayoutRemote.checked.session
			w.LayoutRemote.mu.Unlock()
			w.LayoutRemote.Abort() // the next call drops the session; CASHead opens another
			return nil
		}
	})
	if checkedIn == "" {
		t.Fatal("the pre-check recorded no session")
	}
	if n := w.casGot.Load(); n == 0 {
		t.Fatal("CAS in a new session looked up no chunk: a check of another session was reused")
	}
}

// CommitPendingSnapshot never has a passed check: naming the current image
// (no new chunks) still looks up its segments and its root.
func TestCommitPendingSnapshotLooksUpTheRootAndSegments(t *testing.T) {
	lh, w, err := c1Cut(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	var segs, roots atomic.Int64
	lh.ff.afterLookup = func(p string) {
		switch {
		case strings.Contains(p, "/seg/"):
			segs.Add(1)
		case strings.Contains(p, "/root/"):
			roots.Add(1)
		}
	}
	w.LayoutRemote.SetSnapshot("s1", layout.SnapshotMeta{})
	if err := w.LayoutRemote.CommitPendingSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := requireRootAndSegments(segs.Load(), roots.Load()); err != nil {
		t.Fatalf("CommitPendingSnapshot: %v", err)
	}
	// Controls: the check fails when either kind of lookup is missing.
	if requireRootAndSegments(0, roots.Load()) == nil || requireRootAndSegments(segs.Load(), 0) == nil {
		t.Fatal("requireRootAndSegments passes with one kind of lookup missing")
	}
	if _, ok := lh.head().Snapshots["s1"]; !ok {
		t.Fatal("snapshot s1 not in HEAD")
	}
}

// The metadata-conflict retry inside CASHead (HEAD's generation moved with
// its image and session unchanged, as by an operator's snapshot-table
// write): without a passed check the retry looks up the chunks again; with
// one for the same pair it may reuse it.
func TestMetadataRetryLooksUpOnlyWithoutAPassedCheck(t *testing.T) {
	for _, c := range []struct {
		name    string
		check   bool
		lookups bool
	}{
		{"no passed check", false, true},
		{"passed check of the same pair", true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var retried atomic.Bool
			var atRetry atomic.Int64
			var ff *fakeFiler
			_, w, err := c1Cut(t, func(w *c1Remote) func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error {
				ff = w.ff
				ff.failUpdate = func(p string) error {
					if strings.HasSuffix(p, "/HEAD") && w.inCAS.Load() && !retried.Load() {
						retried.Store(true)
						atRetry.Store(w.casGot.Load())
						e := ff.entries[p] // under ff.mu: an operator write that moves only the generation
						gen := string(e.Extended[layoutGen])
						e.Extended[layoutGen] = []byte(gen + "0")
					}
					return nil
				}
				if c.check {
					return nil // the real pre-check
				}
				return func(context.Context, storage.ChunkHeadVersion, storage.ChunkHead) error { return nil }
			})
			if err != nil {
				t.Fatal(err)
			}
			if !retried.Load() {
				t.Fatal("no metadata retry happened")
			}
			afterRetry := w.casGot.Load() - atRetry.Load()
			if got := afterRetry > 0; got != c.lookups {
				t.Fatalf("retry looked up %d chunks; want lookups=%v", afterRetry, c.lookups)
			}
		})
	}
}

// requireRootAndSegments refuses a commit check that looked up no segment
// or no root.
func requireRootAndSegments(segs, roots int64) error {
	switch {
	case segs == 0 && roots == 0:
		return fmt.Errorf("looked up no segment and no root")
	case segs == 0:
		return fmt.Errorf("looked up %d roots but no segment", roots)
	case roots == 0:
		return fmt.Errorf("looked up %d segments but no root", segs)
	}
	return nil
}
