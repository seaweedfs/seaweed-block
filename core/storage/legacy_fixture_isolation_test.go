package storage

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The retired engine must not return through constructors or import aliases.
func TestLegacyFixtureIsolation(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"createLegacyWALStoreWithOptions": true, "openLegacyWALStoreWithOptions": true, "openLegacyReadOnlyWithOptions": true, "newWALWriter": true, "decodeWALEntry": true, "encodeWALPadding": true, "recoverWAL": true, "walEntry": true}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") || n == "format_v3_test_support.go" {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range legacyConstructorReferences(f, forbidden) {
			t.Errorf("production source %s references legacy fixture %s", e.Name(), name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-deps", "-json", "./core/storage")
	cmd.Dir = filepath.Join("..", "..")
	cmd.WaitDelay = 2 * time.Second
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("default production transitive import graph: %v", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(out)))
	foundStorage := false
	for dec.More() {
		var p struct {
			ImportPath string
			GoFiles    []string
		}
		if err := dec.Decode(&p); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(p.ImportPath, "/internal/v2legacy") {
			t.Errorf("unexpected frozen import %s", p.ImportPath)
		}
		if p.ImportPath == "github.com/seaweedfs/seaweed-block/core/storage" {
			foundStorage = true
			for _, n := range p.GoFiles {
				if n == "format_v3_test_support.go" || n == "legacy_c11_file_fixture_test.go" {
					t.Fatal("test constructors entered default production import closure")
				}
			}
		}
	}
	if !foundStorage {
		t.Fatal("production dependency graph did not contain core/storage")
	}
}

func legacyConstructorReferences(f *ast.File, forbidden map[string]bool) []string {
	var refs []string
	ast.Inspect(f, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && forbidden[id.Name] {
			refs = append(refs, id.Name)
		}
		return true
	})
	return refs
}

func TestLegacyFixtureIsolationRejectsFunctionValues(t *testing.T) {
	const name = "createLegacyWALStoreWithOptions"
	for _, body := range []string{
		"func use() { createLegacyWALStoreWithOptions() }",
		"func createLegacyWALStoreWithOptions() {}",
		"var constructor = createLegacyWALStoreWithOptions",
		"func use() { constructor := createLegacyWALStoreWithOptions; constructor() }",
		"func use() { accept(createLegacyWALStoreWithOptions) }",
	} {
		t.Run(body, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "production.go", "package storage\n"+body, 0)
			if err != nil {
				t.Fatal(err)
			}
			if refs := legacyConstructorReferences(f, map[string]bool{name: true}); len(refs) != 1 {
				t.Fatalf("legacy constructor reference escaped guard: %v", refs)
			}
		})
	}
	f, err := parser.ParseFile(token.NewFileSet(), "legacy.go", "package storage\nfunc createLegacyWALStoreWithOptions() {}", 0)
	if err != nil {
		t.Fatal(err)
	}
	if refs := legacyConstructorReferences(f, map[string]bool{name: true}); len(refs) != 0 {
		t.Fatalf("declaration is not a use: %v", refs)
	}
}
