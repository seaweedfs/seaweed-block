package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSWTestOpsListShowsRegisteredScenario(t *testing.T) {
	repoRoot := findRepoRoot(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"--repo-root", repoRoot, "--list"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "g15e-k8s-dynamic-cleanup") {
		t.Fatalf("list output missing g15e scenario:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "alpha-k8s-large") {
		t.Fatalf("list output missing alpha large scenario:\n%s", stdout.String())
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root not found")
		}
		dir = parent
	}
}

func TestSWTestOpsRunsGoTestScenarioByName(t *testing.T) {
	repoRoot := findRepoRoot(t)
	artDir := filepath.Join(t.TempDir(), "art")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--repo-root", repoRoot,
		"--scenario", "g15b-manifest",
		"--artifact-dir", artDir,
		"--run-id", "cli-unit",
		"--commit", "test-commit",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "pass\tg15b-manifest") {
		t.Fatalf("stdout=%s", stdout.String())
	}
	for _, name := range []string{"run-request.json", "result.json", "test-stdout.log"} {
		if _, err := os.Stat(filepath.Join(artDir, name)); err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
	}
}
