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

// The old engine remains only for explicit fixtures until row 14a. The
// production import closure must not bring in its frozen tool/test package,
// and no production source may call the legacy constructor chain.
func TestLegacyFixtureIsolation(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"createLegacyWALStoreWithOptions": true, "openLegacyWALStoreWithOptions": true, "openLegacyReadOnlyWithOptions": true}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") || n == "format_v3_test_support.go" {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && forbidden[id.Name] {
				t.Errorf("production source %s calls legacy fixture %s", e.Name(), id.Name)
			}
			return true
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-deps", "-json", "./core/storage")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("default production transitive import graph: %v", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(out)))
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
			for _, n := range p.GoFiles {
				if n == "format_v3_test_support.go" {
					t.Fatal("test constructors entered default production import closure")
				}
			}
		}
	}
}
