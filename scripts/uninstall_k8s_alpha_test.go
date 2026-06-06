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
		"capture_optional()",
		"[failure-snapshot] optional command failed:",
		"k8s/pods-all.yaml",
		"k8s/events-all.txt",
		"k8s/blockvolume-deployments.yaml",
		"k8s/app-pods-describe.txt",
		"logs/blockmaster.current.log",
		"logs/blockmaster.previous.log",
		"logs/csi-node.current.log",
		"logs/csi-node.previous.log",
		`capture_optional "$ARTIFACT_DIR/logs/blockmaster.previous.log"`,
		`capture_optional "$ARTIFACT_DIR/logs/csi-controller.previous.log"`,
		`capture_optional "$ARTIFACT_DIR/logs/csi-node.previous.log"`,
		`capture_optional "$ARTIFACT_DIR/logs/blockvolume.previous.log"`,
		"host/iscsi-sessions.txt",
		"host/iscsi-nodes.txt",
		"host/multipath.txt",
		"host/dmsetup.txt",
		`capture_optional "$ARTIFACT_DIR/host/kubelet-mounts.txt"`,
		"host/processes.txt",
		"read_only=true",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("failure snapshot script missing %q", want)
		}
	}
}

func TestCollectHelmSupportBundleKeepsOptionalDiagnosticsNonFatal(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "collect-helm-support-bundle.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"capture_optional()",
		"[support-bundle] optional command failed:",
		`capture_optional "$ARTIFACT_DIR/logs/blockvolume.log"`,
		`capture_optional "$ARTIFACT_DIR/iscsi/sessions.txt"`,
		`capture_optional "$ARTIFACT_DIR/iscsi/nodes.txt"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("support bundle script missing %q", want)
		}
	}
}

func TestBuildAlphaImagesImportsLocalCSIImageWhenImportingRemoteK3SNodes(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "build-alpha-images.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	remoteBranch := `if [[ -n "$IMPORT_K3S_NODES" ]]; then`
	idx := strings.Index(script, remoteBranch)
	if idx < 0 {
		t.Fatalf("build script missing remote k3s import branch %q", remoteBranch)
	}
	branch := script[idx:]
	for _, want := range []string{
		`import_k3s_image "$IMAGE" "k3s-import-local-sw-block.log"`,
		`verify_local_k3s_image "$IMAGE" "k3s-images-local-sw-block.txt"`,
		`import_k3s_image "$CSI_IMAGE" "k3s-import-local-sw-block-csi.log"`,
		`verify_local_k3s_image "$CSI_IMAGE" "k3s-images-local-sw-block-csi.txt"`,
		`[alpha-build] k3s_import node=local skipped reason=local_k3s_unavailable`,
		`import_k3s_images_to_nodes "$IMPORT_K3S_NODES"`,
	} {
		if !strings.Contains(branch, want) {
			t.Fatalf("remote k3s import branch missing %q", want)
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
