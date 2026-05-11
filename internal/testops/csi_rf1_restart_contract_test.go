package testops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCSIRF1DurableRestartScripts_PinBlockvolumeRestartContract(t *testing.T) {
	body := readRepoFile(t, "scripts", "run-alpha-app-demo.sh")
	for _, want := range []string{
		"SW_BLOCK_RESTART_BLOCKVOLUME_BEFORE_READER",
		"SW_BLOCK_LAUNCHER_STATE_HOSTPATH",
		"blockvolume restart mode requires SW_BLOCK_LAUNCHER_STATE_HOSTPATH",
		"restart_blockvolume_deployment()",
		"wait_no_swblock_iscsi_sessions",
		"blockvolume-pod-ids.before-restart.tsv",
		"blockvolume-pod-ids.after-restart.tsv",
		"blockvolume pod UID did not change across rollout restart",
		"status-durable-after-blockvolume-restart.json",
		"lifecycle-volumes.after-blockvolume-restart.json",
		"blockvolume-generated.after-restart.log",
		"--launcher-state-hostpath",
		"--launcher-status",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("app demo blockvolume restart path missing %q", want)
		}
	}

	wrapper := readRepoFile(t, "scripts", "run-k8s-blockvolume-restart.sh")
	for _, want := range []string{
		`SW_BLOCK_LAUNCHER_STATE_HOSTPATH="${SW_BLOCK_LAUNCHER_STATE_HOSTPATH:-/var/lib/sw-block}"`,
		"export SW_BLOCK_RESTART_BLOCKVOLUME_BEFORE_READER=1",
		"demo-app-pvc-writer-hold-root.yaml",
		"run-alpha-app-demo.sh",
	} {
		if !strings.Contains(wrapper, want) {
			t.Fatalf("blockvolume restart wrapper missing %q:\n%s", want, wrapper)
		}
	}
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{findRepoRoot(t)}, parts...)...)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
