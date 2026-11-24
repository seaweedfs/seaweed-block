package auth_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var devBypass = map[string]bool{"DevCall": true, "DevUnaryInterceptor": true, "DevStreamInterceptor": true}

const authImport = "github.com/seaweedfs/seaweed-block/core/auth"

// bypassUses lists every reference to a development-mode bypass in one file:
// calls, function values, package-level aliases and initializers, through an
// import of core/auth under any name (dot imports included), or inside
// core/auth itself. Each is named by its enclosing top-level declaration
// ("func F", "var x", "init").
func bypassUses(f *ast.File, inAuth bool) []string {
	names := map[string]bool{} // local names of the core/auth import
	dot := inAuth
	for _, imp := range f.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p == authImport {
			switch {
			case imp.Name == nil:
				names["auth"] = true
			case imp.Name.Name == ".":
				dot = true
			default:
				names[imp.Name.Name] = true
			}
		}
	}
	var uses []string
	for _, decl := range f.Decls {
		where := "decl"
		var skip *ast.Ident
		switch d := decl.(type) {
		case *ast.FuncDecl:
			where = "func " + d.Name.Name
			skip = d.Name // a bypass's own declaration
		case *ast.GenDecl:
			where = strings.ToLower(d.Tok.String())
			for _, s := range d.Specs {
				if v, ok := s.(*ast.ValueSpec); ok && len(v.Names) > 0 {
					where += " " + v.Names[0].Name
				}
			}
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && names[id.Name] && devBypass[x.Sel.Name] {
					uses = append(uses, where+"."+x.Sel.Name)
				}
				return false // the selector's own identifiers are not bare uses
			case *ast.Ident:
				if dot && devBypass[x.Name] && x != skip {
					uses = append(uses, where+"."+x.Name)
				}
			}
			return true
		})
	}
	return uses
}

// The development-mode markers admit every call: DevCall, DevUnaryInterceptor
// and DevStreamInterceptor are exported bypasses of the caller decision. Their
// only production use is blockmaster's server wiring when it runs without an
// authenticator (serverAuth, development mode). This census fails when any
// other non-test reference appears anywhere: a call, a function value, an
// alias or an initializer.
func TestDevModeBypassHasOneProductionCaller(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
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
		for _, u := range bypassUses(f, strings.HasPrefix(rel, "core/auth/") && !strings.Contains(strings.TrimPrefix(rel, "core/auth/"), "/")) {
			sites = append(sites, rel+":"+u)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(sites)
	want := []string{
		"core/host/master/authz.go:func serverAuth.DevStreamInterceptor",
		"core/host/master/authz.go:func serverAuth.DevUnaryInterceptor",
	}
	if strings.Join(sites, ",") != strings.Join(want, ",") {
		t.Fatalf("development-mode bypass uses = %v, want only %v", sites, want)
	}
}

// Negative controls: the census sees a direct call, a package-level function
// value (alias), an initializer call, a renamed import, a dot import, and an
// internal use inside core/auth.
func TestDevModeBypassCensusSeesEveryKindOfUse(t *testing.T) {
	for _, c := range []struct {
		name, src string
		inAuth    bool
		want      string
	}{
		{"direct call", `package p; import "` + authImport + `"; func f() { _ = auth.DevCall(nil) }`, false, "func f.DevCall"},
		{"package-level alias", `package p; import "` + authImport + `"; var bypass = auth.DevUnaryInterceptor`, false, "var bypass.DevUnaryInterceptor"},
		{"initializer", `package p; import "` + authImport + `"; func init() { auth.DevStreamInterceptor() }`, false, "func init.DevStreamInterceptor"},
		{"renamed import", `package p; import a "` + authImport + `"; var x = a.DevCall`, false, "var x.DevCall"},
		{"dot import", `package p; import . "` + authImport + `"; var y = DevCall`, false, "var y.DevCall"},
		{"inside core/auth", `package auth; func DevCall() {}; func g() { DevCall() }`, true, "func g.DevCall"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "x.go", c.src, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := bypassUses(f, c.inAuth)
			if len(got) != 1 || got[0] != c.want {
				t.Fatalf("census %v; want [%s]", got, c.want)
			}
		})
	}
}
