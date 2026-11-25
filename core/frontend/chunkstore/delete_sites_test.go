package chunkstore

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

// deleteSites is every non-test function in core/ and cmd/ that deletes a
// filer entry (DeleteEntry) or builds a volume directory name (VolumeDir),
// with what it deletes or where the name comes from. Nothing removes a
// deleted HEAD, a VOLUME or a volume directory, and a volume directory is
// named only from a reference (a root's owner name and id) or from the
// master's volume record, never from a name given plus an id read from
// that same directory. A new site fails until it is classified here.
var deleteSites = map[string]string{
	"core/frontend/chunkstore/catalog.go Catalog.dropIntent -> DeleteEntry":            "an intent entry (old store)",
	"core/frontend/chunkstore/catalog.go Catalog.DeleteSnapshot -> DeleteEntry":        "a snapshot name entry, conditional on its attempt (old store)",
	"core/frontend/chunkstore/chunk_remote.go ChunkRemote.DeleteIntent -> DeleteEntry": "an intent entry (old store)",
	"core/frontend/chunkstore/retention.go Catalog.freeName -> DeleteEntry":            "a snapshot name entry, conditional on its attempt (old store)",
	"core/frontend/chunkstore/swfs.go SWFS.DeleteEntry -> DeleteEntry":                 "the filer client itself",
	"core/frontend/chunkstore/catalog_b.go VolumeRef.Dir -> VolumeDir":                 "a VolumeRef is the master's record (name, layout id) or a reference's owner name and id (foreignOwners)",
	"core/frontend/chunkstore/layout_store.go LayoutStore.deleteObject -> DeleteEntry": "only canonical pool, segment or root files under the configured volume root; HEAD, VOLUME and directory paths refused (TestLayoutStoreDeleteNeverRemovesControlRecords)",
	"core/frontend/chunkstore/layout/read.go Image.Chunk -> VolumeDir":                 "a foreign chunk's directory from the reference's owner name and id",
}

func TestDeleteAndDirectorySites_Enumeration(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	for _, top := range []string{"core", "cmd"} {
		err := filepath.Walk(filepath.Join(root, top), func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains(p, "filerpb") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, p)
			rel = filepath.ToSlash(rel)
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				name := fn.Name.Name
				if fn.Recv != nil && len(fn.Recv.List) == 1 {
					switch x := fn.Recv.List[0].Type.(type) {
					case *ast.Ident:
						name = x.Name + "." + name
					case *ast.StarExpr:
						name = x.X.(*ast.Ident).Name + "." + name
					}
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					var callee string
					switch x := c.Fun.(type) {
					case *ast.SelectorExpr:
						callee = x.Sel.Name
					case *ast.Ident:
						callee = x.Name
					}
					if callee == "DeleteEntry" || callee == "VolumeDir" {
						key := rel + " " + strings.TrimPrefix(name, "*") + " -> " + callee
						found[key] = true
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
	var added, gone []string
	for k := range found {
		if _, ok := deleteSites[k]; !ok {
			added = append(added, k)
		}
	}
	for k := range deleteSites {
		if !found[k] {
			gone = append(gone, k)
		}
	}
	sort.Strings(added)
	sort.Strings(gone)
	if len(added) != 0 || len(gone) != 0 {
		t.Fatalf("delete or directory-naming sites changed: classify each (it must not remove a HEAD, a VOLUME or a volume directory, and a directory name must come from a reference or the master's record).\nnew: %v\ngone: %v", added, gone)
	}
}
