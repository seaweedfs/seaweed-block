package lifecycle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestLayoutIdentityWriterAndReaders(t *testing.T) {
	want := map[string]bool{
		"core/lifecycle/store.go CreateVolume":                            true,
		"core/lifecycle/store.go volumeSpecsCompatible":                   true,
		"core/lifecycle/store.go validateSpec":                            true,
		"core/lifecycle/workload_plan.go PlanBlockVolumeWorkloads":        true,
		"core/host/master/services.go lifecycleVolumeResponse":            true,
		"core/host/master/workspace_layout_head.go WorkspaceLayoutVolume": true,
		"core/launcher/k8s_renderer.go blockVolumeArgs":                   true,
		"core/launcher/k8s_renderer.go durableRoot":                       true,
		"cmd/blockvolume/main.go runVolume":                               true,
		"cmd/blockvolume/main.go parseFlags":                              true,
		"core/rpc/lifecycleclient/identity.go ResolveVolume":              true,
	}
	seen := map[string]bool{}
	root := filepath.Join("..", "..")
	for _, top := range []string{"core", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, p)
			for _, decl := range f.Decls {
				name := "<package>"
				if fn, ok := decl.(*ast.FuncDecl); ok {
					name = fn.Name.Name
				}
				site := filepath.ToSlash(rel) + " " + name
				ast.Inspect(decl, func(n ast.Node) bool {
					if kv, ok := n.(*ast.KeyValueExpr); ok {
						if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "LayoutID" && !want[site] {
							t.Errorf("layout identity literal writer needs a probe: %s", site)
						}
					}
					if call, ok := n.(*ast.CallExpr); ok {
						if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == "NewVolumeID" {
							t.Errorf("unclassified local identity mint: %s", site)
						}
					}
					if s, ok := n.(*ast.SelectorExpr); ok {
						if s.Sel.Name == "LayoutID" || s.Sel.Name == "layoutID" || s.Sel.Name == "GetLayoutId" {
							seen[site] = true
							if !want[site] {
								t.Errorf("layout identity reader/writer needs a probe: %s", site)
							}
						}
						if s.Sel.Name == "NewVolumeID" && site != "core/lifecycle/store.go CreateVolume" {
							t.Errorf("layout identity minted outside lifecycle create: %s", site)
						}
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
	for site := range want {
		if !seen[site] {
			t.Errorf("missing identity probe site %s", site)
		}
	}
}
