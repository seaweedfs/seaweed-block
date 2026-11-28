package layout

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Compare generation scalars throughout product code, including storage.
// Conditional-update implementations and one tagged image-cache short-circuit
// own comparisons; literal-zero presence probes stay legal. Scalar aliases are
// followed, but whole-struct comparisons carrying generations are NOT detected:
// this syntax guard has no type/dataflow analysis across aggregate values.
type generationSite struct{ location, key string }

func headGenerationComparisons(name string, src []byte) ([]generationSite, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		return nil, err
	}
	var bad []generationSite
	cacheExceptions := 0
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if name == "core/frontend/chunkstore/layout_store.go" && fn.Name.Name == "UpdateHead" {
			continue
		}
		if name == "core/frontend/chunkstore/layout_remote.go" && (fn.Name.Name == "CASHead" || fn.Name.Name == "prepareCommitLocked") {
			continue
		}
		aliases := map[*ast.Object]bool{}
		usesGen := func(n ast.Node) bool {
			found := false
			ast.Inspect(n, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.SelectorExpr:
					found = found || x.Sel.Name == "Gen" || x.Sel.Name == "HeadGen" || x.Sel.Name == "ownGen" || x.Sel.Name == "headGen" || x.Sel.Name == "visible"
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
					if usesGen(x.X) && usesGen(x.Y) {
						if name == "core/frontend/chunkstore/layout_remote.go" && fn.Name.Name == "viewLocked" && strings.Contains(string(src), generationCacheException) {
							left, lok := x.X.(*ast.SelectorExpr)
							right, rok := x.Y.(*ast.SelectorExpr)
							if lok && rok && left.Sel.Name == "Gen" && right.Sel.Name == "ownGen" && x.Op == token.NEQ {
								cacheExceptions++
								if cacheExceptions == 1 {
									return true
								}
							}
						}
						var expr bytes.Buffer
						_ = format.Node(&expr, fset, x)
						bad = append(bad, generationSite{fset.Position(x.Pos()).String(), name + " " + fn.Name.Name + " " + expr.String()})
					}
				}
			case *ast.SwitchStmt:
				if x.Tag != nil && usesGen(x.Tag) && switchHasGenerationCase(x, usesGen) {
					var expr bytes.Buffer
					_ = format.Node(&expr, fset, x.Tag)
					bad = append(bad, generationSite{fset.Position(x.Pos()).String(), name + " " + fn.Name.Name + " switch " + expr.String()})
				}
			}
			return true
		})
	}
	return bad, nil
}

const generationCacheException = "head-generation-cache-exception: imageOf controls view changes"

func switchHasGenerationCase(s *ast.SwitchStmt, usesGen func(ast.Node) bool) bool {
	for _, clause := range s.Body.List {
		for _, value := range clause.(*ast.CaseClause).List {
			if usesGen(value) {
				return true
			}
		}
	}
	return false
}

func TestHeadGenerationSourceGuardDetector(t *testing.T) {
	for _, tc := range []struct {
		src string
		bad bool
	}{
		{"package p; func f(h H) bool { return h.Gen == other.Gen }", true},
		{"package p; func f(h H) bool { return h.headGen == other.Gen }", true},
		{"package p; func f(h H) bool { return h.visible == other.Gen }", true},
		{"package p; func f(h H) bool { g := h.Gen; k := g; return k != other.Gen }", true},
		{"package p; func f(h H) { switch h.Gen { case other.Gen: } }", true},
		{"package p; func f(h H) bool { return h.Gen == 0 }", false},
		{"package p; func f(h H) bool { return 0 != h.Gen }", false},
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

func TestHeadGenerationCacheExceptionIsSingular(t *testing.T) {
	const name = "core/frontend/chunkstore/layout_remote.go"
	for _, tc := range []struct {
		tag, body string
		bad       bool
	}{
		{"", "if h.Gen != l.ownGen {}", true},
		{generationCacheException, "if h.Gen != l.ownGen {}", false},
		{generationCacheException, "if h.Gen != l.ownGen {}; if h.Gen != l.ownGen {}", true},
		{generationCacheException, "if h.Gen != other.Gen {}", true},
	} {
		src := "package p; // " + tc.tag + "\nfunc viewLocked() {" + tc.body + "}"
		got, err := headGenerationComparisons(name, []byte(src))
		if err != nil || (len(got) != 0) != tc.bad {
			t.Fatalf("%s: %v %v", src, got, err)
		}
	}
}

func TestHeadGenerationComparedOnlyByConditionalUpdate(t *testing.T) {
	root := filepath.Clean("../../../..")
	exceptions := 0
	// These engine comparisons use the adapter's synthetic image-view token,
	// not raw filer HEAD generation. Pin both syntax and count: a new or removed
	// site forces review. No no-op-cut authority comparison is allowed here.
	allowed := map[string]int{
		"core/storage/chunk_head_belt.go confirmChunkHead got.Gen != want.Gen":   1, // publication confirmation
		"core/storage/chunk_layer.go captureLocked sc.headGen == l.head.Gen":     1, // staged image cache
		"core/storage/chunk_layer.go captureLocked cur.Gen == expected.Gen":      1, // unresolved CAS base
		"core/storage/chunk_layer.go SettleDetach cur.Gen == base.Gen":           2, // pending and cancelled CAS base
		"core/storage/chunk_layer.go resolvePending cur.Gen == base.Gen":         2, // pending and cancelled CAS base
		"core/storage/chunk_layer.go stage sc.headGen == l.head.Gen":             1, // staged image cache
		"core/storage/chunk_memremote.go CASHead expected.Gen != m.head.Gen":     1, // in-memory conditional update
		"core/storage/chunk_publish_settled.go noteSettled next.Gen == prev.Gen": 1, // publication notification deduplication
	}
	seen := map[string]int{}
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
			if d.Name() == ".git" || d.Name() == "vendor" {
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
		exceptions += strings.Count(string(raw), generationCacheException)
		bad, err := headGenerationComparisons(rel, raw)
		if err != nil {
			return err
		}
		for _, site := range bad {
			if _, ok := allowed[site.key]; ok {
				seen[site.key]++
			} else {
				t.Errorf("HEAD generation comparison outside conditional update: %s %s", site.location, site.key)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, count := range allowed {
		if seen[key] != count {
			t.Errorf("generation site %s: count %d, want %d", key, seen[key], count)
		}
	}
	if exceptions != 1 {
		t.Fatalf("cache exception tags = %d, want exactly 1", exceptions)
	}
}
