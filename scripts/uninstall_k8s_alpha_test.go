package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUninstallK8sAlphaDeletesSeaweedISCSINodeRecords(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "uninstall-k8s-alpha.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"delete stale Seaweed Block iSCSI node records",
		"iscsi-nodes.before-scrub.txt",
		"iscsi-nodes.after-scrub.txt",
		"delete-iscsi-node-records.log",
		"awk '/io\\.seaweedfs/ {print $1, $2}'",
		"-m node -T \"$target\" -p \"$portal\" -o delete",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("uninstall script missing %q", want)
		}
	}
}

func TestFailureSnapshotScriptCapturesRequiredEvidenceLayers(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "collect-k8s-failure-snapshot.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"failure_snapshot_status=",
		"capture_failure_count=",
		"k8s/pods-all.yaml",
		"k8s/events-all.txt",
		"k8s/blockvolume-deployments.yaml",
		"k8s/app-pods-describe.txt",
		"logs/blockmaster.current.log",
		"logs/blockmaster.previous.log",
		"logs/csi-node.current.log",
		"logs/csi-node.previous.log",
		"host/iscsi-sessions.txt",
		"host/iscsi-nodes.txt",
		"host/multipath.txt",
		"host/dmsetup.txt",
		"host/processes.txt",
		"read_only=true",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("failure snapshot script missing %q", want)
		}
	}
}

func TestVerifyHelmCleanupReportsAllResidueDimensions(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "verify-helm-cleanup.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"cleanup_status=ok",
		"cleanup_status=failed",
		"k8s_residue_count=",
		"iscsi_residue_count=",
		"process_residue_count=",
		"multipath_residue_count=",
		"hostpath_residue_count=",
		"failure_count=",
		"helm_release_still_present",
		"kubernetes_sw_block_resources_present",
		"iscsi_sessions_present",
		"iscsi_node_records_present",
		"multipath_maps_present",
		"sw_block_processes_present",
		"hostpath_residue_present",
		"multipath-residue.after-cleanup.txt",
		"dmsetup.after-cleanup.txt",
		"cleanup-failures.txt",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("cleanup verifier missing %q", want)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
