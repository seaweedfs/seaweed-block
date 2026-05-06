package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestG15d_K8sBlockStack_HasLauncherButNoPrecreatedBlockvolume(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "g15d", "block-stack.yaml")
	for _, want := range []string{
		"--launcher-loop-interval=100ms",
		"--launcher-manifest-dir=/manifests",
		"__NODE_NAME__",
		"pools:",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("block-stack missing %q", want)
		}
	}
	if strings.Contains(body, "name: sw-blockvolume-r1") || strings.Contains(body, "/usr/local/bin/blockvolume") {
		t.Fatalf("G15d block stack must not precreate blockvolume workloads:\n%s", body)
	}
}

func TestG15d_K8sCSIController_IncludesExternalProvisioner(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "g15d", "csi-controller.yaml")
	for _, want := range []string{
		"name: csi-provisioner",
		"registry.k8s.io/sig-storage/csi-provisioner:",
		"name: csi-attacher",
		"--csi-address=/csi/csi.sock",
		"--extra-create-metadata=true",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("csi-controller missing %q", want)
		}
	}
}

func TestAlphaK8sCSIController_IncludesCreateMetadata(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "alpha", "csi-controller.yaml")
	if !strings.Contains(body, "--extra-create-metadata=true") {
		t.Fatalf("alpha csi-controller must enable PVC metadata propagation:\n%s", body)
	}
}

func TestG15d_K8sDynamicPVC_UsesStorageClassNoPrecreatedPV(t *testing.T) {
	body := g15dReadFile(t, "deploy", "k8s", "g15d", "dynamic-pvc-pod.yaml")
	for _, want := range []string{
		"kind: StorageClass",
		"provisioner: block.csi.seaweedfs.com",
		"name: sw-block-dynamic-v1",
		"sha256sum -c /data/payload.sha256",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dynamic-pvc-pod missing %q", want)
		}
	}
	if strings.Contains(body, "kind: PersistentVolume\n") {
		t.Fatalf("dynamic PVC scenario must not precreate PV:\n%s", body)
	}
}

func TestG15d_K8sRunner_AppliesLauncherGeneratedManifest(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-g15d-k8s-dynamic.sh")
	for _, want := range []string{
		"generated-blockvolume.yaml",
		"SW_BLOCK_LAUNCHER_PVC_OWNER_REF",
		"SW_BLOCK_ISCSI_CHAP_USERNAME",
		"SW_BLOCK_ISCSI_CHAP_SECRET",
		"csi.storage.k8s.io/node-stage-secret-name",
		"--launcher-iscsi-chap-secret-name",
		"BLOCKVOLUME_NAMESPACE=\"kube-system\"",
		"--kubernetes-pvc-uid-lookup",
		"--launcher-pvc-owner-ref",
		"apply iSCSI CHAP Secret",
		"kubectl apply -f \"$ARTIFACT_DIR/generated-blockvolume.yaml\"",
		"kubectl -n \"$BLOCKVOLUME_NAMESPACE\" wait --for=condition=available deploy -l app=sw-blockvolume",
		"kubectl -n \"$BLOCKVOLUME_NAMESPACE\" logs -l sw-block.seaweedfs.com/volume",
		"kubectl -n \"$NAMESPACE\" delete pvc sw-block-dynamic-v1",
		"wait for launcher manifest cleanup after DeleteVolume",
		"delete generated blockvolume Deployment after manifest cleanup",
		"wait for Kubernetes GC to delete PVC-owned blockvolume Deployment",
		"iscsi-sessions.after-delete.txt",
		"PASS: dynamic PVC create/delete completed checksum write/read and cleanup",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("runner missing %q", want)
		}
	}
}

func TestAlphaScripts_OwnerRefModeInjectsBothLauncherAndCSILookupFlags(t *testing.T) {
	for _, script := range []string{"run-g15d-k8s-dynamic.sh", "run-alpha-app-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			for _, want := range []string{
				"SW_BLOCK_LAUNCHER_PVC_OWNER_REF",
				"--launcher-pvc-owner-ref",
				"--kubernetes-pvc-uid-lookup",
				"BLOCKVOLUME_NAMESPACE=\"$NAMESPACE\"",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s missing %q", script, want)
				}
			}
		})
	}
}

func TestPublicAlphaWrappers_DefaultToPVCOwnerReferenceCleanup(t *testing.T) {
	for _, script := range []string{"run-k8s-alpha.sh", "run-k8s-demo.sh"} {
		t.Run(script, func(t *testing.T) {
			body := g15dReadFile(t, "scripts", script)
			if !strings.Contains(body, `SW_BLOCK_LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"`) {
				t.Fatalf("%s must default SW_BLOCK_LAUNCHER_PVC_OWNER_REF to 1", script)
			}
		})
	}
}

func TestInstallAlpha_DefaultsToPVCOwnerReferenceCleanup(t *testing.T) {
	body := g15dReadFile(t, "scripts", "install-k8s-alpha.sh")
	for _, want := range []string{
		`LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"`,
		"--launcher-pvc-owner-ref",
		"--kubernetes-pvc-uid-lookup",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("install script missing %q", want)
		}
	}
}

func TestApplyAlphaBlockvolumes_WaitsForGeneratedManifestInsteadOfKubeSystem(t *testing.T) {
	body := g15dReadFile(t, "scripts", "apply-k8s-alpha-blockvolumes.sh")
	if !strings.Contains(body, `kubectl wait -f "$ARTIFACT_DIR/generated-blockvolume.yaml" --for=condition=available`) {
		t.Fatalf("apply script must wait using generated manifest:\n%s", body)
	}
	if strings.Contains(body, "kubectl -n kube-system wait --for=condition=available deploy -l app=sw-blockvolume") {
		t.Fatalf("apply script must not hardcode kube-system generated workloads:\n%s", body)
	}
}

func TestAlphaAppDemo_CanRestartCSINodeBeforeReader(t *testing.T) {
	body := g15dReadFile(t, "scripts", "run-alpha-app-demo.sh")
	for _, want := range []string{
		"SW_BLOCK_RESTART_CSI_NODE_BEFORE_READER",
		"SW_BLOCK_DEMO_APP_MANIFEST",
		"rollout restart ds/sw-block-csi-node",
		"restart-csi-node-status.log",
		"wait_pod_log_contains sw-block-demo-writer",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("app demo script missing %q", want)
		}
	}

	wrapper := g15dReadFile(t, "scripts", "run-k8s-csi-node-restart.sh")
	if !strings.Contains(wrapper, "demo-app-pvc-writer-hold.yaml") {
		t.Fatalf("restart wrapper must use mounted-writer manifest:\n%s", wrapper)
	}
}

func g15dReadFile(t *testing.T, parts ...string) string {
	t.Helper()
	root := g15bRepoRoot(t)
	path := filepath.Join(append([]string{root}, parts...)...)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
