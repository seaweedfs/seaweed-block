package master

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// deleteMarkCensus lists the violations of the DeleteWithoutAck writer rules
// in the given Go sources (file name -> text):
//   - a record (an attestedStores literal) is built only in open (a fresh
//     record, no mark yet) and in record, which must carry the mark over
//     from the record it replaces;
//   - the mark is assigned only in markDeleteWithoutAck, and only true.
func deleteMarkCensus(t *testing.T, files map[string]string) []string {
	t.Helper()
	var out []string
	fset := token.NewFileSet()
	for name, src := range files {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			carries := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); ok && id.Name == "attestedStores" {
						if fn.Name.Name != "open" && fn.Name.Name != "record" {
							out = append(out, name+": "+fn.Name.Name+" builds a store record")
						}
					}
				case *ast.AssignStmt:
					for i, l := range x.Lhs {
						sel, ok := l.(*ast.SelectorExpr)
						if !ok || sel.Sel.Name != "DeleteWithoutAck" || i >= len(x.Rhs) {
							continue
						}
						rhs := x.Rhs[i]
						if r, ok := rhs.(*ast.SelectorExpr); ok && r.Sel.Name == "DeleteWithoutAck" && fn.Name.Name == "record" {
							carries = true
							continue
						}
						if v, ok := rhs.(*ast.Ident); ok && v.Name == "true" && fn.Name.Name == "markDeleteWithoutAck" {
							continue
						}
						out = append(out, name+": "+fn.Name.Name+" writes DeleteWithoutAck")
					}
				}
				return true
			})
			if fn.Name.Name == "record" && !carries {
				out = append(out, name+": record does not carry DeleteWithoutAck over")
			}
		}
	}
	sort.Strings(out)
	return out
}

func masterSources(t *testing.T) map[string]string {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatal(err)
		}
		files[n] = string(b)
	}
	return files
}

// Every writer of the delete mark, and every rewrite of a store record,
// keeps the mark: it is set once, by markDeleteWithoutAck, and never lost.
func TestDeleteMarkWriterCensus(t *testing.T) {
	if v := deleteMarkCensus(t, masterSources(t)); len(v) != 0 {
		t.Fatalf("delete mark census: %v", v)
	}
}

// The census fails on each rule broken: a record rewrite that drops the
// mark, a new place that builds a record, and a write that clears it.
func TestDeleteMarkWriterCensusControls(t *testing.T) {
	src := masterSources(t)["store_attestation.go"]
	carry := "\tnext.DeleteWithoutAck = cur.DeleteWithoutAck"
	if !strings.Contains(src, carry) {
		t.Fatal("control fixture: the carry line moved")
	}
	for name, bad := range map[string]string{
		"dropped carry": strings.Replace(src, carry, "\t_ = cur", 1),
		"new builder":   src + "\nfunc rebuild() attestedStores { return attestedStores{} }\n",
		"cleared mark":  src + "\nfunc clearMark(v *attestedStores) { v.DeleteWithoutAck = false }\n",
	} {
		if v := deleteMarkCensus(t, map[string]string{"store_attestation.go": bad}); len(v) == 0 {
			t.Errorf("%s: the census found nothing", name)
		}
	}
}
