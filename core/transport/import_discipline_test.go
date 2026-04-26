package transport

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// T4d-3 G-1 §7 + §4 Item E: INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY
// (Q3 architect lock). core/transport may import core/storage only
// for its recovery contract surface (LogicalStorage, RecoveryEntry,
// ErrWALRecycled, RecoveryFailureKind, RecoveryMode). Transport MUST
// NOT import substrate-internal packages: core/storage/walstore,
// core/storage/smartwal, etc.
//
// This is a source-grep fence parsing every transport .go file's
// import block. Fails loudly if a substrate-internal import is
// added in a future commit.

// TestT4d3_Catchup_TransportNeverImportsSubstrateInternals fences
// the package-boundary discipline. If a future commit adds
// `import "github.com/seaweedfs/seaweed-block/core/storage/walstore"`
// or similar to transport, this test fails.
func TestT4d3_Catchup_TransportNeverImportsSubstrateInternals(t *testing.T) {
	forbidden := []string{
		"core/storage/walstore",
		"core/storage/smartwal",
		"core/storage/blockstore", // hypothetical future substrate-internal pkg
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob transport sources: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no transport .go files found — wd or glob broken")
	}

	fset := token.NewFileSet()
	for _, file := range files {
		// Skip test files; rule applies to production code only.
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			for _, sub := range forbidden {
				if strings.Contains(path, sub) {
					t.Errorf("FAIL: %s imports %q (forbidden substrate-internal package; INV-REPL-TRANSPORT-STORAGE-CONTRACT-ONLY — Q3 architect lock)",
						file, path)
				}
			}
		}
	}
}
