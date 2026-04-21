// Ownership: QA (from sketch-approved test spec v3-phase-15-t1-test-spec.md for T1 Frontend Contract Smoke).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t1-test-spec.md
// Maps to ledger rows: PCDD-STUFFING-001
//
// Test layer: Unit (AST/grep guard)
// Bad-state family: PCDD-STUFFING-001 (authority stuffing)
// Bounded fate: core/frontend production files must not import
// core/authority nor construct AssignmentInfo / AssignmentAsk
// composite literals. Extends the existing T0 boundary guard to
// the frontend boundary (sketch §9).
package frontend_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFrontendCannotMintAuthority_BoundaryGuard(t *testing.T) {
	root, err := findFrontendPkgRoot(t)
	if err != nil {
		t.Fatalf("findFrontendPkgRoot: %v", err)
	}

	forbidden := []string{
		`"github.com/seaweedfs/seaweed-block/core/authority"`,
	}
	mintingLiterals := []string{
		"adapter.AssignmentInfo{",
		"adapter.AssignmentAsk{",
	}

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, f := range forbidden {
			if bytes.Contains(data, []byte(f)) {
				t.Errorf("%s: forbidden import %s in core/frontend production code", path, f)
			}
		}
		for _, lit := range mintingLiterals {
			if bytes.Contains(data, []byte(lit)) {
				t.Errorf("%s: forbidden authority-minting literal %s in core/frontend production code", path, lit)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}

// findFrontendPkgRoot returns the directory containing the
// core/frontend package. Uses runtime.Caller so it works from
// whatever working directory `go test` was invoked in.
func findFrontendPkgRoot(t *testing.T) (string, error) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", &pkgRootError{"runtime.Caller failed"}
	}
	return filepath.Dir(file), nil
}

type pkgRootError struct{ msg string }

func (e *pkgRootError) Error() string { return e.msg }
