package testops

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDecodeRegistrationRejectsAuthorityShapedDriver(t *testing.T) {
	raw := `{
	  "schema_version":"1.0",
	  "scenario":"bad",
	  "driver":{"type":"publisher","package":"./cmd/blockmaster"}
	}`
	_, err := DecodeRegistration(strings.NewReader(raw))
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("DecodeRegistration err=%v", err)
	}
}

func TestG15bManifestRegistrationRunsGoTestAndWritesArtifacts(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "g15b-manifest.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "g15b-manifest" || registration.Driver.Type != "go-test" {
		t.Fatalf("registration=%+v", registration)
	}

	registry := NewRegistry()
	if err := registration.RegisterInto(registry, repoRoot); err != nil {
		t.Fatalf("RegisterInto: %v", err)
	}
	artDir := filepath.Join(t.TempDir(), "artifacts")
	res, err := registry.Run(context.Background(), RunRequest{
		SchemaVersion:  SchemaVersion,
		Scenario:       "g15b-manifest",
		Source:         SourceSpec{Repo: "seaweed_block", Commit: registration.KnownGreenCommit},
		ArtifactDir:    artDir,
		RunID:          "g15b-manifest-unit",
		TimeoutSeconds: 120,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Status != StatusPass {
		t.Fatalf("status=%s summary=%s", res.Status, res.Summary)
	}
	for _, name := range []string{"run-request.json", "result.json", "test-stdout.log"} {
		if _, err := os.Stat(filepath.Join(artDir, name)); err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
	}
}

func TestG15bK8sStaticRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "g15b-k8s-static.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "g15b-k8s-static" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	if registration.KnownGreenCommit != "95b7217" {
		t.Fatalf("known_green_commit=%q want 95b7217", registration.KnownGreenCommit)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-g15b-k8s-static.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
}

func TestG15dK8sDynamicRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "g15d-k8s-dynamic.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "g15d-k8s-dynamic" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	if registration.KnownGreenCommit != "a3d1e6a" {
		t.Fatalf("known_green_commit=%q want a3d1e6a", registration.KnownGreenCommit)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-g15d-k8s-dynamic.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"generated-blockvolume.yaml", "blockvolume-generated.log", "pod.log", "cleanup.log"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
}

func TestG15eK8sDynamicCleanupRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "g15e-k8s-dynamic-cleanup.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "g15e-k8s-dynamic-cleanup" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	if registration.KnownGreenCommit != "ddec28c" {
		t.Fatalf("known_green_commit=%q want ddec28c", registration.KnownGreenCommit)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-g15d-k8s-dynamic.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	for _, want := range []string{"delete-pvc.log", "delete-generated-blockvolume.log", "iscsi-sessions.after-delete.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
}

func TestISCSIP2OSSmokeRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "iscsi-p2-os-smoke.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "iscsi-p2-os-smoke" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	if registration.KnownGreenCommit != "869292b" {
		t.Fatalf("known_green_commit=%q want 869292b", registration.KnownGreenCommit)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-iscsi-os-smoke.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"mkfs.iter1.log", "sha256-check.iter1.log", "fio.iter1.log", "iscsi-sessions.final.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func registrationPath(repoRoot, name string) string {
	return filepath.Join(repoRoot, "internal", "testops", "registry", name)
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
