package layout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Storage's ChunkHead generation is the adapter's synthetic engine-view token,
// not the filer's HEAD generation. Check the kernel and all consumers outside
// that engine. Only the two conditional-update implementations own comparisons.
func headGenerationComparisons(name string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		return nil, err
	}
	var bad []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if name == "core/frontend/chunkstore/layout_store.go" && fn.Name.Name == "UpdateHead" {
			continue
		}
		if name == "core/frontend/chunkstore/layout_remote.go" && fn.Name.Name == "CASHead" {
			continue
		}
		aliases := map[*ast.Object]bool{}
		usesGen := func(n ast.Node) bool {
			found := false
			ast.Inspect(n, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.SelectorExpr:
					found = found || x.Sel.Name == "Gen" || x.Sel.Name == "HeadGen"
				case *ast.Ident:
					found = found || x.Obj != nil && aliases[x.Obj]
				}
				return !found
			})
			return found
		}
		for changed := true; changed; {
			changed = false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				var lhs []ast.Expr
				var rhs []ast.Expr
				switch x := n.(type) {
				case *ast.AssignStmt:
					lhs, rhs = x.Lhs, x.Rhs
				case *ast.ValueSpec:
					for _, id := range x.Names {
						lhs = append(lhs, id)
					}
					rhs = x.Values
				}
				if len(lhs) != len(rhs) {
					return true
				}
				for i, r := range rhs {
					// Track scalar aliases, not an entire result struct or the
					// error returned by passing a generation to UpdateHead.
					switch r.(type) {
					case *ast.Ident, *ast.SelectorExpr, *ast.ParenExpr, *ast.UnaryExpr, *ast.BinaryExpr:
					default:
						continue
					}
					id, ok := lhs[i].(*ast.Ident)
					if ok && id.Obj != nil && !aliases[id.Obj] && usesGen(r) {
						aliases[id.Obj], changed = true, true
					}
				}
				return true
			})
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BinaryExpr:
				switch x.Op {
				case token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
					if usesGen(x) {
						bad = append(bad, fset.Position(x.Pos()).String()+" "+fn.Name.Name)
					}
				}
			case *ast.SwitchStmt:
				if x.Tag != nil && usesGen(x.Tag) {
					bad = append(bad, fset.Position(x.Pos()).String()+" "+fn.Name.Name)
				}
			}
			return true
		})
	}
	return bad, nil
}

func TestHeadGenerationSourceGuardDetector(t *testing.T) {
	for _, tc := range []struct {
		src string
		bad bool
	}{
		{"package p; func f(h H) bool { return h.Gen == 7 }", true},
		{"package p; func f(h H) bool { g := h.Gen; k := g; return k != 7 }", true},
		{"package p; func f(h H) { switch h.Gen { case 7: } }", true},
		{"package p; func f(h H) { store.UpdateHead(h.Gen, h) }", false},
		{"package p; func f(h H) { if err := store.UpdateHead(h.Gen, h); err != nil {} }", false},
		{"package p; func f(h H) bool { r := R{HeadGen: h.Gen}; return r.Kind == 7 }", false},
		{"package p; func f(h H) bool { return h.Epoch == 7 }", false},
	} {
		got, err := headGenerationComparisons("probe.go", []byte(tc.src))
		if err != nil || (len(got) != 0) != tc.bad {
			t.Fatalf("%s: %v %v", tc.src, got, err)
		}
	}
}

func TestHeadGenerationComparedOnlyByConditionalUpdate(t *testing.T) {
	root := filepath.Clean("../../../..")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "vendor" || rel == "core/storage" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || strings.HasSuffix(rel, ".pb.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		bad, err := headGenerationComparisons(rel, raw)
		if err != nil {
			return err
		}
		for _, site := range bad {
			t.Errorf("HEAD generation comparison outside conditional update: %s", site)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
