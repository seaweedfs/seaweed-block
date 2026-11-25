package transport

import (
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The epoch table's writers: the replica's note of a shipped entry's epoch
// before the apply, the primary's note at role adoption, the rebuild-done
// replacement, and the trim at load of rows above the recovered store's next
// LSN (a lost tail; a node that truncates drops rows past its new end). Any
// other writer is a new way for the table to disagree with the data it
// describes; it fails here until it is reviewed and listed.
var epochTableWriters = []string{
	"cmd/blockvolume/replication_ingress.go: func newReplicationIngress .TrimAbove",
	"cmd/blockvolume/replication_ingress.go: func notePrimaryEpoch .Note",
	"cmd/blockvolume/replication_ingress.go: func notePrimaryEpoch .Replace", // promoted over an invalid table: exactly [(e, next LSN)]
	"core/transport/replica.go: func EnterDualLane .InvalidateDualLane",       // dual-lane session: before its first byte
	"core/transport/replica.go: func handleConn .Invalidate", // rebuild: before the first base byte
	"core/transport/replica.go: func handleConn .Invalidate", // catch-up: at the recovery lane start
	"core/transport/replica.go: func handleConn .Note",       // live-lane entry, before the apply
	"core/transport/replica.go: func handleConn .CompleteRecovery", // rebuild done, only by the mark's owner
	"core/transport/replica.go: func handleConn .CompleteRecovery", // catch-up's final barrier, only by the mark's owner
}

// epochWrites lists the non-test calls of Note, Replace or TrimAbove whose
// receiver's TYPE is *EpochTable (any variable, field or alias name), as
// "file: func F .Method", type-checking every package that imports this one.
func epochWrites(t *testing.T, root string) []string {
	t.Helper()
	const self = "github.com/seaweedfs/seaweed-block/core/transport"
	dirs := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == ".git" || n == "vendor" || n == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			// Only the files of this build (GOOS, GOARCH, tags), as the
			// compiler sees them.
			if ok, err := build.Default.MatchFile(filepath.Dir(path), filepath.Base(path)); err != nil || !ok {
				return err
			}
			dirs[filepath.Dir(path)] = append(dirs[filepath.Dir(path)], path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "source", nil)
	var out []string
	for dir, paths := range dirs {
		var files []*ast.File
		uses := false
		for _, p := range paths {
			f, err := parser.ParseFile(fset, p, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, f)
			for _, im := range f.Imports {
				if strings.Trim(im.Path.Value, `"`) == self {
					uses = true
				}
			}
		}
		rel, _ := filepath.Rel(root, dir)
		if !uses && filepath.ToSlash(rel) != "core/transport" {
			continue
		}
		info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
		var typeErrs []error
		conf := types.Config{Importer: imp, Error: func(err error) { typeErrs = append(typeErrs, err) }}
		_, _ = conf.Check(dir, fset, files, info)
		// The census is authoritative only over fully type-checked code: an
		// error could hide a selection, so it fails rather than undercounts.
		if len(typeErrs) > 0 {
			t.Fatalf("census cannot type-check %s: %v", rel, typeErrs[0])
		}
		for _, f := range files {
			name := filepath.ToSlash(mustRel(root, fset.Position(f.Pos()).Filename))
			out = append(out, typedWritesIn(f, name, info)...)
		}
	}
	sort.Strings(out)
	return out
}

func mustRel(root, p string) string {
	r, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return r
}

// typedWritesIn: calls of the writer methods on a receiver of type
// *EpochTable (or EpochTable).
func typedWritesIn(f *ast.File, rel string, info *types.Info) []string {
	var out []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !epochWriter[sel.Sel.Name] {
				return true
			}
			s := info.Selections[sel]
			if s == nil {
				return true
			}
			recv := s.Recv()
			if p, ok := recv.(*types.Pointer); ok {
				recv = p.Elem()
			}
			if named, ok := recv.(*types.Named); ok && named.Obj().Name() == "EpochTable" {
				out = append(out, rel+": func "+fn.Name.Name+" ."+sel.Sel.Name)
			}
			return true
		})
	}
	return out
}

var epochWriter = map[string]bool{"Note": true, "Replace": true, "TrimAbove": true, "Invalidate": true, "InvalidateDualLane": true, "CompleteRecovery": true}

func TestEpochTableHasExactlyItsListedWriters(t *testing.T) {
	got := epochWrites(t, filepath.Join("..", ".."))
	want := append([]string(nil), epochTableWriters...)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("epoch table writers:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// Only Note and Replace reach the durable write.
	f, err := parser.ParseFile(token.NewFileSet(), "epoch_table.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var persisters []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "persistLocked" {
					persisters = append(persisters, fn.Name.Name)
				}
			}
			return true
		})
	}
	sort.Strings(persisters)
	if strings.Join(persisters, ",") != "CompleteRecovery,Invalidate,InvalidateDualLane,Note,Replace,TrimAbove" {
		t.Fatalf("methods reaching persistLocked: %v; want CompleteRecovery, Invalidate, InvalidateDualLane, Note, Replace, TrimAbove", persisters)
	}
}

// The census sees a fourth writer whatever the receiver is called: an alias
// of the table, a field, a plain variable.
func TestEpochTableWriterCensusFindsAFourth(t *testing.T) {
	src := `package p

type EpochTable struct{}

func (*EpochTable) Note(a, b uint64) error { return nil }

type holder struct{ h *EpochTable }

func sneak(epochs *EpochTable) {
	history := epochs
	_ = history.Note(9, 9)
	x := holder{h: epochs}
	_ = x.h.Note(1, 1)
}

type other struct{}

func (other) Note(a, b uint64) error { return nil }

func notATable(o other) { _ = o.Note(1, 1) }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "sneak.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
	if _, err := (&types.Config{}).Check("p", fset, []*ast.File{f}, info); err != nil {
		t.Fatal(err)
	}
	got := typedWritesIn(f, "sneak.go", info)
	if len(got) != 2 || got[0] != "sneak.go: func sneak .Note" || got[1] != "sneak.go: func sneak .Note" {
		t.Fatalf("census of planted writers (alias, field; not another type): %v", got)
	}
}
