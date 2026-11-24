package chunkstore

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// TestHeadCreate_SourceGuard holds every HEAD creator to head_create.go: a
// function of this package that builds a HEAD entry (headEntry) and creates
// an entry (CreateEntry, createResolved) must go through createHead or
// headCreateCheck; and no code but headEntry builds an entry named HEAD.
func TestHeadCreate_SourceGuard(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var creators []string
	for _, fi := range files {
		name := fi.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					switch f := x.Fun.(type) {
					case *ast.Ident:
						calls[f.Name] = true
					case *ast.SelectorExpr:
						calls[f.Sel.Name] = true
					}
				case *ast.KeyValueExpr:
					if k, ok := x.Key.(*ast.Ident); ok && k.Name == "Name" {
						if v, ok := x.Value.(*ast.Ident); ok && v.Name == "headName" && fn.Name.Name != "headEntry" {
							t.Errorf("%s: %s builds an entry named HEAD; only headEntry may", fset.Position(x.Pos()), fn.Name.Name)
						}
					}
				}
				return true
			})
			if calls["headEntry"] && (calls["CreateEntry"] || calls["createResolved"] || calls["createHead"]) {
				creators = append(creators, fn.Name.Name)
				if !calls["createHead"] && !calls["headCreateCheck"] {
					t.Errorf("%s: %s creates a HEAD without createHead or headCreateCheck", fset.Position(fn.Pos()), fn.Name.Name)
				}
			}
		}
	}
	sort.Strings(creators)
	// The scan must see the known creators, or it proves nothing.
	for _, want := range []string{"CASHead", "ensureInitialHead", "fork", "writeHead"} {
		if i := sort.SearchStrings(creators, want); i == len(creators) || creators[i] != want {
			t.Errorf("guard did not find HEAD creator %s (found %v)", want, creators)
		}
	}
}

// The runtime guard: the record filer refuses to create an entry named HEAD
// unless createHead (or the fork publish after its check) marked the call,
// so a HEAD create built anywhere else is refused, not merely unscanned.
func TestHeadCreate_RecordFilerRefusesBypass(t *testing.T) {
	f, _, c, _ := catalogFixture(t)
	ctx := context.Background()
	if err := f.DeleteEntry(ctx, catBase+"/v1", headName, nil); err != nil {
		t.Fatal(err)
	}
	he, err := headEntry(storage.ChunkHead{Gen: 1, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadActive})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.f.CreateEntry(ctx, catBase+"/v1", he, true); !errors.Is(err, errHeadCreateBypass) {
		t.Fatalf("raw HEAD create through the record filer: %v; want refused", err)
	}
	// A volume-delete creates only a deleting HEAD.
	if err := createHead(ctx, c.f, c.blobs, catBase+"/v1", he, headCreateDelete); err == nil || !strings.Contains(err.Error(), "deleting HEAD") {
		t.Fatalf("delete-reason create of an active HEAD: %v; want refused", err)
	}
	if _, err := f.LookupEntry(ctx, catBase+"/v1", headName); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a HEAD was created: %v", err)
	}
}
