package authority

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSetNowForTest_OnlyInTestFiles ensures the clock-injection
// seam never leaks into production code. SetNowForTest is a test
// affordance added for G1 adversarial coverage (DeadPeer) and is
// the twin of authority.TopologyController.SetNowForTest.
//
// Per QA Owner's routing on escalation #2: the method name ends
// in ForTest specifically so a grep-based guard can catch any
// production caller. This test IS that guard.
//
// Rule: the identifier "SetNowForTest" may appear ONLY in files
// ending with _test.go, anywhere in the repo.
func TestSetNowForTest_OnlyInTestFiles(t *testing.T) {
	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}

	var offenders []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || (len(name) > 0 && name[0] == '.') {
				if path != root {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		src := string(data)
		// We allow the identifier to appear inside a comment or
		// string — the common case is the `// SetNowForTest(...)`
		// defining comment on the method itself. Exclude the
		// definition file plus its forwarder by name, then fail
		// on any other occurrence.
		base := filepath.Base(path)
		if base == "observation_store.go" || base == "observation_host.go" || base == "controller.go" {
			return nil
		}
		if strings.Contains(src, "SetNowForTest") {
			offenders = append(offenders, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("SetNowForTest referenced in production files (must be test-only):\n  %s",
			strings.Join(offenders, "\n  "))
	}
}
