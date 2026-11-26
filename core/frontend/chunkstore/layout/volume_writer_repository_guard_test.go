package layout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// Count references, not just calls: method values, method expressions and
// package-level aliases can all hide another writer. Resolve the layout import
// so unrelated structs' VolumeName fields do not count as the VOLUME constant.
// A write using the literal entry name instead of the constant is not caught.
func volumeCreationRefs(name string, src []byte) (map[string]int, error) {
	f, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		return nil, err
	}
	layoutImports := map[string]bool{}
	for _, i := range f.Imports {
		p, _ := strconv.Unquote(i.Path.Value)
		if p == "github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout" {
			alias := "layout"
			if i.Name != nil {
				alias = i.Name.Name
			}
			layoutImports[alias] = true
		}
	}
	kernel := strings.HasPrefix(name, "core/frontend/chunkstore/layout/")
	got := map[string]int{}
	visit := func(n ast.Node, owner string) {
		ast.Inspect(n, func(n ast.Node) bool {
			symbol := ""
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if x.Sel.Name == "CreateVolumeRecord" {
					symbol = x.Sel.Name
				}
				if recv, ok := x.X.(*ast.Ident); ok && layoutImports[recv.Name] && x.Sel.Name == "VolumeName" {
					symbol = x.Sel.Name
				}
				if symbol != "" {
					got[name+" "+owner+" "+symbol]++
				}
				return false
			case *ast.Ident:
				if x.Name == "VolumeName" && (kernel || layoutImports["."]) {
					symbol = x.Name
				}
			}
			if symbol != "" {
				got[name+" "+owner+" "+symbol]++
			}
			return true
		})
	}
	for _, d := range f.Decls {
		switch x := d.(type) {
		case *ast.FuncDecl:
			if x.Body != nil {
				visit(x.Body, x.Name.Name)
			}
		case *ast.GenDecl:
			for _, spec := range x.Specs {
				if v, ok := spec.(*ast.ValueSpec); ok {
					for _, e := range v.Values {
						visit(e, "<package>")
					}
				}
			}
		}
	}
	return got, nil
}

func TestVolumeWriterRepositorySites(t *testing.T) {
	want := map[string]int{
		"core/frontend/chunkstore/layout/create.go createVolume CreateVolumeRecord": 1,
		"core/frontend/chunkstore/layout/read.go VolumeID VolumeName":               1,
		"core/frontend/chunkstore/layout/verify.go volumeID VolumeName":             1,
		"core/frontend/chunkstore/layout_create.go ReadVolumeRecord VolumeName":     1,
		"core/frontend/chunkstore/layout_create.go CreateVolumeRecord VolumeName":   1,
	}
	got := map[string]int{}
	root := filepath.Clean("../../../..")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sites, err := volumeCreationRefs(filepath.ToSlash(rel), src)
		if err != nil {
			return err
		}
		for key, n := range sites {
			got[key] += n
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("VOLUME sites = %v, want exactly %v", got, want)
	}
}

func TestVolumeWriterRepositoryDetectorControls(t *testing.T) {
	for _, src := range []string{
		`func hidden(st S){st.CreateVolumeRecord(nil,v)}`,
		`func hidden(st S){f:=st.CreateVolumeRecord;_ = f}`,
		`var hidden = S.CreateVolumeRecord`,
		`func hidden(){e:=filer.Entry{Name:k.VolumeName};_ = e}`,
		`var hidden = k.VolumeName`,
	} {
		raw := `package p; import k "github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout";` + src
		got, err := volumeCreationRefs("core/elsewhere/new.go", []byte(raw))
		if err != nil || len(got) != 1 {
			t.Fatalf("missed cross-package writer/constant control %s: %v %v", src, got, err)
		}
	}
	got, err := volumeCreationRefs("core/elsewhere/new.go", []byte(`package p; func f(x S){_ = x.VolumeName}`))
	if err != nil || len(got) != 0 {
		t.Fatal("unrelated field mistaken for the layout constant", got, err)
	}
	got, err = volumeCreationRefs("core/frontend/chunkstore/layout/create.go", []byte(`package layout; func createVolume(st S){st.CreateVolumeRecord(nil,v);st.CreateVolumeRecord(nil,v)}`))
	if err != nil || got["core/frontend/chunkstore/layout/create.go createVolume CreateVolumeRecord"] != 2 {
		t.Fatal("duplicate at existing site not counted", got, err)
	}
}
