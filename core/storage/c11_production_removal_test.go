package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The retained C11 file codec is historical fixture evidence, not a store
// option. Inspect uses as well as declarations (including function aliases).
func TestProductionExcludesC11FileStore(t *testing.T) {
	for _, dir := range []string{".", filepath.Join("..", "frontend", "durable")} {
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		inspected := 0
		for _, entry := range files {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			inspected++
			file := filepath.Join(dir, name)
			f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if ok && (strings.Contains(id.Name, "FileChunkBitmapStore") || id.Name == "legacyC11FileStore" || id.Name == "newLegacyC11FileStore") {
					t.Errorf("production source %s retains C11 file store %s", file, id.Name)
				}
				return true
			})
		}
		if inspected == 0 {
			t.Fatalf("no production source inspected in %s", dir)
		}
	}
}
