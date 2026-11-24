package auth_test

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

// The development-mode markers admit every call: DevCall, DevUnaryInterceptor
// and DevStreamInterceptor are exported bypasses of the caller decision. Their
// only production caller is blockmaster's server wiring when it runs without
// an authenticator (serverAuth, development mode). This guard fails when a
// second non-test caller appears anywhere.
func TestDevModeBypassHasOneProductionCaller(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bypass := map[string]bool{"DevCall": true, "DevUnaryInterceptor": true, "DevStreamInterceptor": true}
	var sites []string
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := ""
				switch f := call.Fun.(type) {
				case *ast.SelectorExpr:
					name = f.Sel.Name
				case *ast.Ident:
					name = f.Name
				}
				if bypass[name] {
					sites = append(sites, rel+":"+fn.Name.Name+"."+name)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(sites)
	want := []string{
		"core/host/master/authz.go:serverAuth.DevStreamInterceptor",
		"core/host/master/authz.go:serverAuth.DevUnaryInterceptor",
	}
	if strings.Join(sites, ",") != strings.Join(want, ",") {
		t.Fatalf("development-mode bypass callers = %v, want only %v", sites, want)
	}
}
