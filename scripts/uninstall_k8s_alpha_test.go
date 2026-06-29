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
		"host/host-prereq-summary.txt",
		"write_host_prereq_summary",
		"iscsi_prereq=",
		"multipath_prereq=",
		"host_prereq=host/host-prereq-summary.txt",
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
		"host/host-prereq-summary.txt",
		"write_host_prereq_summary",
		"iscsi_prereq=",
		"multipath_prereq=",
		"host_prereq=host/host-prereq-summary.txt",
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

func TestPhase103NVMeRoCEPreflightIsReadOnlyAndClaimBounded(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "run-phase103-nvme-multihost-roce-preflight-gate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"phase103_nvme_multihost_roce_preflight_status=",
		"read_only=true",
		"nvme_cli_present=",
		"nvme_tcp_preflight_ready=",
		"rdma_device_count=",
		"roce_preflight_status=",
		"roce_preflight_candidate=",
		"roce_claim_allowed=",
		"roce_live_gate_required=true",
		"roce_live_io_claim=false",
		"performance_claim_allowed=false",
		"blocked_missing_nvme_cli",
		"blocked_missing_nvme_tcp_capability",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("phase103 preflight script missing %q", want)
		}
	}
	for _, forbidden := range []string{"modprobe ", "nvme connect", "nvme disconnect", "kubectl patch", "kubectl delete"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("phase103 preflight script should be read-only, found %q", forbidden)
		}
	}
	if strings.Contains(script, "-printf") {
		t.Fatalf("phase103 preflight script should avoid GNU-only find -printf")
	}
}

func TestPhase104RoCELiveIOFeasibilityGateIsExplicitRefusal(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "run-phase104-roce-live-io-feasibility-gate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"phase104_roce_live_io_status=",
		"target_nvme_transport_supported=tcp",
		"target_nvme_rdma_supported=false",
		"phase104_roce_live_io_result=blocked_target_transport_unsupported",
		"phase104_roce_live_io_gate_required_before_claim=true",
		"roce_claim_allowed=false",
		"roce_live_io_claim=false",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("phase104 gate missing %q", want)
		}
	}
}

func TestPhase105NVMETCPMultiHostTopologyGateIsReadOnlyAndClaimBounded(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "run-phase105-nvme-tcp-multihost-topology-gate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"phase105_nvme_tcp_multihost_topology_status=",
		"read_only=true",
		"live_io_claim=false",
		"performance_claim_allowed=false",
		"roce_claim_allowed=false",
		"reason_code=publish_target_loopback_cross_node",
		"safe_action=observe.inspect_publish_target_topology",
		"iscsi_remediation_recommended=false",
		"same_node_loopback_non_claim=true",
		"cross_node_non_loopback_live_followup=true",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("phase105 gate missing %q", want)
		}
	}
	for _, forbidden := range []string{"nvme connect", "nvme disconnect", "kubectl patch", "kubectl delete"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("phase105 gate should be read-only, found %q", forbidden)
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
