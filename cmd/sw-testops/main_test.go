package main

import (
	"bytes"
	"encoding/json"
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
}

func TestSWTestOpsPassesScenarioParams(t *testing.T) {
	repoRoot := findRepoRoot(t)
	artDir := filepath.Join(t.TempDir(), "art")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--repo-root", repoRoot,
		"--scenario", "g15b-manifest",
		"--artifact-dir", artDir,
		"--run-id", "cli-param-unit",
		"--commit", "test-commit",
		"--param", "SW_BLOCK_ALPHA_IMAGES_ENV=/tmp/pin/alpha-images.env",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	raw, err := os.ReadFile(filepath.Join(artDir, "run-request.json"))
	if err != nil {
		t.Fatalf("read run-request: %v", err)
	}
	var req struct {
		ScenarioParams map[string]string `json:"scenario_params"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("decode run-request: %v", err)
	}
	if got := req.ScenarioParams["SW_BLOCK_ALPHA_IMAGES_ENV"]; got != "/tmp/pin/alpha-images.env" {
		t.Fatalf("scenario param=%q", got)
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
