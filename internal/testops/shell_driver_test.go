package testops

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestShellDriverRunsScriptAndReadsResult(t *testing.T) {
	dir := t.TempDir()
	script := writeTestDriverScript(t, dir, 0, `cat > "$ART/result.json" <<JSON
{"schema_version":"1.0","run_id":"shell-run","scenario":"shell-scenario","source_commit":"abc123","status":"pass","summary":"script pass","artifact_dir":"$ART"}
JSON
`)
	req := RunRequest{
		SchemaVersion: SchemaVersion,
		Scenario:      "shell-scenario",
		Source:        SourceSpec{Repo: "seaweed_block", Commit: "abc123"},
		ArtifactDir:   filepath.Join(dir, "art"),
		RunID:         "shell-run",
	}
	res, err := ShellDriver{Path: script}.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Status != StatusPass || res.Summary != "script pass" {
		t.Fatalf("res=%+v", res)
	}
	if _, err := os.Stat(filepath.Join(req.ArtifactDir, "run-request.json")); err != nil {
		t.Fatalf("normalized run-request missing: %v", err)
	}
}

func TestShellDriverReportsErrorWhenNoResult(t *testing.T) {
	dir := t.TempDir()
	script := writeTestDriverScript(t, dir, 3, `echo nope >&2`)
	req := RunRequest{
		SchemaVersion: SchemaVersion,
		Scenario:      "shell-scenario",
		Source:        SourceSpec{Repo: "seaweed_block", Commit: "abc123"},
		ArtifactDir:   filepath.Join(dir, "art"),
		RunID:         "shell-run",
	}
	res, err := ShellDriver{Path: script}.Run(context.Background(), req)
	if err == nil {
		t.Fatal("Run succeeded, want error")
	}
	if res.Status != StatusError {
		t.Fatalf("status=%q want error", res.Status)
	}
	if _, statErr := os.Stat(filepath.Join(req.ArtifactDir, "driver-stderr.log")); statErr != nil {
		t.Fatalf("stderr log missing: %v", statErr)
	}
}

func writeTestDriverScript(t *testing.T, dir string, exitCode int, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "driver.cmd")
		script := "@echo off\r\nset ART=%~dp1\r\n" +
			"powershell -NoProfile -Command \"$req = Get-Content '%1' | ConvertFrom-Json; $art=$req.artifact_dir; New-Item -ItemType Directory -Force -Path $art | Out-Null; " +
			"Set-Content -Path (Join-Path $art 'result.json') -Value ('{\\\"schema_version\\\":\\\"1.0\\\",\\\"run_id\\\":\\\"shell-run\\\",\\\"scenario\\\":\\\"shell-scenario\\\",\\\"source_commit\\\":\\\"abc123\\\",\\\"status\\\":\\\"pass\\\",\\\"summary\\\":\\\"script pass\\\",\\\"artifact_dir\\\":\\\"' + ($art -replace '\\\\','\\\\') + '\\\"}')\"\r\n"
		if exitCode != 0 {
			script = "@echo off\r\necho nope 1>&2\r\nexit /b 3\r\n"
		}
		if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
			t.Fatalf("write script: %v", err)
		}
		return path
	}

	path := filepath.Join(dir, "driver.sh")
	script := "#!/usr/bin/env bash\nset -euo pipefail\nREQ=\"$1\"\nART=$(python3 - <<'PY' \"$REQ\"\nimport json,sys\nprint(json.load(open(sys.argv[1]))['artifact_dir'])\nPY\n)\nmkdir -p \"$ART\"\n" + body + "\nexit " + string(rune('0'+exitCode)) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}
