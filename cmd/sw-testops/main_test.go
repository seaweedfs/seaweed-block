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

func TestSWTestOpsWritesControlHistoryAndListsRecords(t *testing.T) {
	repoRoot := findRepoRoot(t)
	root := t.TempDir()
	artDir := filepath.Join(root, "art")
	controlDir := filepath.Join(root, "control")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--repo-root", repoRoot,
		"--scenario", "g15b-manifest",
		"--artifact-dir", artDir,
		"--run-id", "control-unit",
		"--commit", "test-commit",
		"--control-dir", controlDir,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(controlDir, "active", "control-unit.json")); !os.IsNotExist(err) {
		t.Fatalf("active record should move to history, err=%v", err)
	}
	raw, err := os.ReadFile(filepath.Join(controlDir, "history", "control-unit.json"))
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	var rec struct {
		RunID        string `json:"run_id"`
		Scenario     string `json:"scenario"`
		State        string `json:"state"`
		SourceCommit string `json:"source_commit"`
		ArtifactDir  string `json:"artifact_dir"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if rec.RunID != "control-unit" || rec.Scenario != "g15b-manifest" || rec.State != "pass" || rec.SourceCommit != "test-commit" || rec.ArtifactDir != artDir {
		t.Fatalf("history mismatch: %+v", rec)
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--repo-root", repoRoot, "--control-dir", controlDir, "--control-list"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("list exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "pass\tcontrol-unit\tg15b-manifest\ttest-commit\t") || !strings.Contains(stdout.String(), "\t"+artDir) {
		t.Fatalf("control list missing run:\n%s", stdout.String())
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
