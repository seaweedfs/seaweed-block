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

func TestDecodeRegistrationAcceptsResourceMetadata(t *testing.T) {
	raw := `{
	  "schema_version":"1.0",
	  "scenario":"resourceful",
	  "driver":{"type":"go-test","package":"./internal/testops"},
	  "resources":{
	    "group":"m02-block-lab",
	    "exclusive":["node:m02","iscsi:m02"],
	    "ports":[3260]
	  }
	}`
	reg, err := DecodeRegistration(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if reg.Resources.Group != "m02-block-lab" || !containsString(reg.Resources.Exclusive, "iscsi:m02") || len(reg.Resources.Ports) != 1 || reg.Resources.Ports[0] != 3260 {
		t.Fatalf("resources mismatch: %+v", reg.Resources)
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
	if registration.KnownGreenCommit != "d25e7b4" {
		t.Fatalf("known_green_commit=%q want d25e7b4", registration.KnownGreenCommit)
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
	for _, want := range []string{"mkfs.iter*.log", "sha256-check.iter*.log", "fio.iter*.log", "by-path.iter*.txt", "dmesg.iter*.tail.txt", "iscsi-sessions.final.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
}

func TestISCSIP3AttachDetachLoopRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "iscsi-p3-attach-detach-loop.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "iscsi-p3-attach-detach-loop" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-k8s-attach-detach-loop.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"summary.log", "iter-*/writer.log", "iter-*/reader.log", "iter-*/iscsi-sessions.after-delete.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
	if got := registration.ScenarioDefaultParams["SW_BLOCK_ATTACH_DETACH_ITERATIONS"]; got != "3" {
		t.Fatalf("default iterations=%q want 3", got)
	}
}

func TestISCSIP3K8sFioRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "iscsi-p3-k8s-fio.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "iscsi-p3-k8s-fio" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-k8s-alpha-fio.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"pod.log", "blockvolume-generated.log", "iscsi-sessions.after-delete.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
	if !containsString(registration.RequiredCapabilities, "pod-egress-for-apk") {
		t.Fatalf("registration must call out apk/fio pod egress requirement: %v", registration.RequiredCapabilities)
	}
}

func TestISCSIP5CSINodeRestartRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "iscsi-p5-csi-node-restart.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "iscsi-p5-csi-node-restart" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-k8s-csi-node-restart.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"restart-csi-node-status.log", "writer.log", "reader.log", "iscsi-sessions.after-delete.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
	if got := registration.ScenarioDefaultParams["SW_BLOCK_LAUNCHER_PVC_OWNER_REF"]; got != "1" {
		t.Fatalf("owner-ref default=%q want 1", got)
	}
}

func TestISCSIP7BackendFioMatrixRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "iscsi-p7-backend-fio-matrix.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "iscsi-p7-backend-fio-matrix" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-iscsi-backend-fio-matrix.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"summary.md", "walstore/fio.iter*.log", "smartwal/fio.iter*.log"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
	if got := registration.ScenarioDefaultParams["SW_BLOCK_BACKEND_MATRIX"]; got != "walstore smartwal" {
		t.Fatalf("backend matrix default=%q want walstore smartwal", got)
	}
	if !containsString(registration.NonClaims, "No product performance claim.") {
		t.Fatalf("registration must keep performance non-claim: %v", registration.NonClaims)
	}
}

func TestISCSIP8CompatSoakRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "iscsi-p8-compat-soak.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "iscsi-p8-compat-soak" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "run-iscsi-compat-soak.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"summary.md", "os-fio-repeat/fio.iter*.log", "os-fio-repeat/iscsi-sessions.final.txt"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
	if got := registration.ScenarioDefaultParams["SW_BLOCK_P8_OS_FIO_RUNTIME"]; got != "120" {
		t.Fatalf("default OS fio runtime=%q want 120", got)
	}
	if !containsString(registration.NonClaims, "Not a product performance claim.") {
		t.Fatalf("registration must keep performance non-claim: %v", registration.NonClaims)
	}
}

func TestAlphaImagesPinBuildRegistrationBuildsShellDriver(t *testing.T) {
	repoRoot := findRepoRoot(t)
	raw, err := os.Open(registrationPath(repoRoot, "alpha-images-pin-build.json"))
	if err != nil {
		t.Fatalf("open registration: %v", err)
	}
	defer raw.Close()

	registration, err := DecodeRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if registration.Scenario != "alpha-images-pin-build" || registration.Driver.Type != "shell" {
		t.Fatalf("registration=%+v", registration)
	}
	driver, err := registration.NewDriver(repoRoot)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	shell, ok := driver.(ShellDriver)
	if !ok {
		t.Fatalf("driver type=%T want ShellDriver", driver)
	}
	if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != "testops-pin-alpha-images.sh" {
		t.Fatalf("shell path=%q", shell.Path)
	}
	if _, err := os.Stat(shell.Path); err != nil {
		t.Fatalf("shell driver path missing: %v", err)
	}
	for _, want := range []string{"pin-build/alpha-images.env", "pin-build/blockmaster.version.txt", "pin-build/k3s-import-sw-block.log"} {
		if !containsString(registration.Artifacts, want) {
			t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
		}
	}
	if !containsString(registration.RequiredCapabilities, "k3s") {
		t.Fatalf("registration must call out k3s import requirement: %v", registration.RequiredCapabilities)
	}
	if !containsString(registration.NonClaims, "Does not replace protocol-specific smoke tests.") {
		t.Fatalf("registration must keep smoke-test non-claim: %v", registration.NonClaims)
	}
}

func TestNVMeP5CSIRegistrationsBuildShellDrivers(t *testing.T) {
	repoRoot := findRepoRoot(t)
	for _, tc := range []struct {
		file       string
		scenario   string
		scriptBase string
		driverEnv  string
		wantCap    string
		wantArt    string
	}{
		{
			file:       "nvme-p5-csi-dynamic.json",
			scenario:   "nvme-p5-csi-dynamic",
			scriptBase: "testops-run-alpha-k8s.sh",
			driverEnv:  "SW_BLOCK_TESTOPS_WORKLOAD_SCRIPT=scripts/run-k8s-alpha-nvme.sh",
			wantCap:    "nvme_tcp-loadable",
			wantArt:    "nvme-list-subsys.after-delete.json",
		},
		{
			file:       "nvme-p5-default-iscsi-regression.json",
			scenario:   "nvme-p5-default-iscsi-regression",
			scriptBase: "testops-run-alpha-k8s.sh",
			driverEnv:  "SW_BLOCK_TESTOPS_WORKLOAD_SCRIPT=scripts/run-k8s-alpha.sh",
			wantCap:    "iscsi_tcp-loadable",
			wantArt:    "iscsi-sessions.after-delete.txt",
		},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			raw, err := os.Open(registrationPath(repoRoot, tc.file))
			if err != nil {
				t.Fatalf("open registration: %v", err)
			}
			defer raw.Close()

			registration, err := DecodeRegistration(raw)
			if err != nil {
				t.Fatalf("DecodeRegistration: %v", err)
			}
			if registration.Scenario != tc.scenario || registration.Driver.Type != "shell" {
				t.Fatalf("registration=%+v", registration)
			}
			driver, err := registration.NewDriver(repoRoot)
			if err != nil {
				t.Fatalf("NewDriver: %v", err)
			}
			shell, ok := driver.(ShellDriver)
			if !ok {
				t.Fatalf("driver type=%T want ShellDriver", driver)
			}
			if !filepath.IsAbs(shell.Path) || filepath.Base(shell.Path) != tc.scriptBase {
				t.Fatalf("shell path=%q", shell.Path)
			}
			if _, err := os.Stat(shell.Path); err != nil {
				t.Fatalf("shell path %q missing: %v", shell.Path, err)
			}
			if !containsString(registration.Driver.Env, tc.driverEnv) {
				t.Fatalf("registration driver env missing %q: %v", tc.driverEnv, registration.Driver.Env)
			}
			if !containsString(registration.RequiredCapabilities, tc.wantCap) {
				t.Fatalf("registration capabilities missing %q: %v", tc.wantCap, registration.RequiredCapabilities)
			}
			for _, want := range []string{"alpha-images.env", "blockmaster.version.txt", "generated-blockvolume.yaml", tc.wantArt} {
				if !containsString(registration.Artifacts, want) {
					t.Fatalf("registration artifacts missing %q: %v", want, registration.Artifacts)
				}
			}
			if got := registration.ScenarioDefaultParams["SW_BLOCK_LAUNCHER_PVC_OWNER_REF"]; got != "1" {
				t.Fatalf("owner-ref default=%q want 1", got)
			}
			if !containsString(registration.NonClaims, "Requires a fresh pin-build artifact passed with --param SW_BLOCK_ALPHA_IMAGES_ENV=...") {
				t.Fatalf("registration must document pin-build dependency: %v", registration.NonClaims)
			}
		})
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
