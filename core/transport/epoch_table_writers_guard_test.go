package transport

import (
	"go/ast"
	"go/parser"
	"go/token"
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
	"core/transport/replica.go: func handleConn .Note",
	"core/transport/replica.go: func handleConn .Replace",
}

// epochWrites lists the non-test calls of Note or Replace on an expression
// naming an epoch table (an identifier or field whose name contains "epoch"),
// as "file: func F .Method".
func epochWrites(t *testing.T, root string) []string {
	t.Helper()
	var out []string
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
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		out = append(out, writesIn(f, filepath.ToSlash(rel))...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func writesIn(f *ast.File, rel string) []string {
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
			if !ok || (sel.Sel.Name != "Note" && sel.Sel.Name != "Replace" && sel.Sel.Name != "TrimAbove") {
				return true
			}
			var recv string
			switch x := sel.X.(type) {
			case *ast.Ident:
				recv = x.Name
			case *ast.SelectorExpr:
				recv = x.Sel.Name
			}
			if strings.Contains(strings.ToLower(recv), "epoch") {
				out = append(out, rel+": func "+fn.Name.Name+" ."+sel.Sel.Name)
			}
			return true
		})
	}
	return out
}

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
	if strings.Join(persisters, ",") != "Note,Replace,TrimAbove" {
		t.Fatalf("methods reaching persistLocked: %v; want Note, Replace, TrimAbove", persisters)
	}
}

// The census sees a fourth writer: a call in any function on any epoch-named
// receiver.
func TestEpochTableWriterCensusFindsAFourth(t *testing.T) {
	src := "package p\nfunc sneak(epochs *T) { _ = epochs.Note(9, 9) }\nfunc ok(x *T) { _ = x.Note(1, 1) }\n"
	f, err := parser.ParseFile(token.NewFileSet(), "sneak.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := writesIn(f, "sneak.go"); len(got) != 1 || got[0] != "sneak.go: func sneak .Note" {
		t.Fatalf("census of a planted writer: %v", got)
	}
}
