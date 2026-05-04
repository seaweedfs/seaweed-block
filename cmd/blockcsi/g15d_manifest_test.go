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
		"kubectl apply -f \"$ARTIFACT_DIR/generated-blockvolume.yaml\"",
		"kubectl -n kube-system wait --for=condition=available deploy -l app=sw-blockvolume",
		"kubectl -n kube-system logs -l sw-block.seaweedfs.com/volume",
		"kubectl -n \"$NAMESPACE\" delete pvc sw-block-dynamic-v1",
		"wait for launcher manifest cleanup after DeleteVolume",
		"delete generated blockvolume Deployment after manifest cleanup",
		"iscsi-sessions.after-delete.txt",
		"PASS: dynamic PVC create/delete completed checksum write/read and cleanup",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("runner missing %q", want)
		}
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
