package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestG15b_Manifest_CSIDriverRequiresAttach(t *testing.T) {
	doc := g15bFindKind(t, "csi-driver.yaml", "CSIDriver")
	spec := g15bMap(t, doc, "spec")
	if got, ok := spec["attachRequired"].(bool); !ok || !got {
		t.Fatalf("CSIDriver.spec.attachRequired=%v (%T), want true", spec["attachRequired"], spec["attachRequired"])
	}
}

func TestG15b_Manifest_ControllerUsesAttacherNotProvisioner(t *testing.T) {
	body := g15bReadManifest(t, "csi-controller.yaml")
	if !strings.Contains(body, "csi-attacher") {
		t.Fatalf("controller manifest must include csi-attacher")
	}
	if strings.Contains(body, "csi-provisioner") {
		t.Fatalf("G15b static PV controller must not include csi-provisioner")
	}
	if !strings.Contains(body, "--master=$(BLOCKMASTER_ADDR)") {
		t.Fatalf("controller block-csi args must wire read-only blockmaster lookup")
	}
}

func TestG15b_Manifest_ProductStackSingleNodeLoopbackShape(t *testing.T) {
	body := g15bReadManifest(t, "block-stack.yaml")
	for _, want := range []string{
		"name: sw-blockmaster",
		"name: sw-blockvolume-r1",
		"name: sw-blockvolume-r2",
		"--cluster-spec=/config/cluster-spec.yaml",
		"--lifecycle-product-loop-interval=100ms",
		"--expected-slots-per-volume=2",
		"--recovery-mode=dual-lane",
		"--iscsi-listen=127.0.0.1:3260",
		"--iscsi-iqn=iqn.2026-05.io.seaweedfs:g15b-v1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("block-stack.yaml missing %q", want)
		}
	}
	if strings.Contains(body, "--iscsi-listen=0.0.0.0") {
		t.Fatalf("G15b must not weaken loopback-only iSCSI bind")
	}
}

func TestG15b_Manifest_StaticPVDoesNotEmbedTargetFacts(t *testing.T) {
	body := g15bReadManifest(t, "static-pv-pvc-pod.yaml")
	for _, forbidden := range []string{"iscsiAddr", "iqn", "nqn", "endpointVersion", "epoch"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("static PV close-path manifest must not embed %q", forbidden)
		}
	}
}

func TestG15b_Manifest_NodePluginPrivilegedShape(t *testing.T) {
	doc := g15bFindKind(t, "csi-node.yaml", "DaemonSet")
	spec := g15bMap(t, doc, "spec")
	tpl := g15bMap(t, spec, "template")
	podSpec := g15bMap(t, tpl, "spec")
	if got, ok := podSpec["hostNetwork"].(bool); !ok || !got {
		t.Fatalf("node DaemonSet hostNetwork=%v, want true", podSpec["hostNetwork"])
	}

	containers := g15bSlice(t, podSpec, "containers")
	blockCSI := g15bContainer(t, containers, "block-csi")
	sec := g15bMap(t, blockCSI, "securityContext")
	if got, ok := sec["privileged"].(bool); !ok || !got {
		t.Fatalf("block-csi privileged=%v, want true", sec["privileged"])
	}

	mounts := g15bSlice(t, blockCSI, "volumeMounts")
	wantMounts := map[string]bool{
		"/var/lib/kubelet": false,
		"/dev":             false,
		"/etc/iscsi":       false,
	}
	for _, raw := range mounts {
		m := raw.(map[string]any)
		path, _ := m["mountPath"].(string)
		if _, ok := wantMounts[path]; ok {
			wantMounts[path] = true
		}
		if path == "/var/lib/kubelet" && m["mountPropagation"] != "Bidirectional" {
			t.Fatalf("/var/lib/kubelet mountPropagation=%v, want Bidirectional", m["mountPropagation"])
		}
	}
	for path, found := range wantMounts {
		if !found {
			t.Fatalf("node plugin missing mountPath %s", path)
		}
	}
}

func TestG15b_Manifest_StaticPVUsesBlockCSIDriver(t *testing.T) {
	doc := g15bFindKind(t, "static-pv-pvc-pod.yaml", "PersistentVolume")
	spec := g15bMap(t, doc, "spec")
	csi := g15bMap(t, spec, "csi")
	if got := csi["driver"]; got != "block.csi.seaweedfs.com" {
		t.Fatalf("PV csi.driver=%v, want block.csi.seaweedfs.com", got)
	}
	if got := csi["volumeHandle"]; got != "v1" {
		t.Fatalf("PV csi.volumeHandle=%v, want v1", got)
	}
}

func TestG15b_Manifest_NoAuthorityShapedFields(t *testing.T) {
	for _, file := range []string{"block-stack.yaml", "csi-driver.yaml", "csi-controller.yaml", "csi-node.yaml", "static-pv-pvc-pod.yaml"} {
		body := strings.ToLower(g15bStripYAMLComments(g15bReadManifest(t, file)))
		for _, forbidden := range []string{"endpointversion", "authorityepoch", "assignmentfact", "assignmentask"} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("%s contains authority-shaped field %q", file, forbidden)
			}
		}
	}
}

func TestG15b_ImageBuildInputs_ContainExpectedBinariesAndNodeTools(t *testing.T) {
	swBlock := g15bReadDeployFile(t, "Dockerfile.sw-block")
	for _, want := range []string{"./cmd/blockmaster", "./cmd/blockvolume", "/usr/local/bin/blockmaster", "/usr/local/bin/blockvolume"} {
		if !strings.Contains(swBlock, want) {
			t.Fatalf("Dockerfile.sw-block missing %q", want)
		}
	}

	csi := g15bReadDeployFile(t, "Dockerfile.blockcsi")
	for _, want := range []string{"./cmd/blockcsi", "open-iscsi", "e2fsprogs", "util-linux", "/usr/local/bin/blockcsi"} {
		if !strings.Contains(csi, want) {
			t.Fatalf("Dockerfile.blockcsi missing %q", want)
		}
	}

	buildScript := g15bReadScript(t, "build-g15b-images.sh")
	for _, want := range []string{"Dockerfile.sw-block", "Dockerfile.blockcsi", "sw-block:local", "sw-block-csi:local"} {
		if !strings.Contains(buildScript, want) {
			t.Fatalf("build-g15b-images.sh missing %q", want)
		}
	}
}

func g15bReadManifest(t *testing.T, name string) string {
	t.Helper()
	return g15bReadDeployFile(t, name)
}

func g15bReadDeployFile(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(g15bRepoRoot(t), "deploy", "k8s", "g15b", name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func g15bReadScript(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(g15bRepoRoot(t), "scripts", name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func g15bFindKind(t *testing.T, name, kind string) map[string]any {
	t.Helper()
	for _, doc := range g15bDecodeDocs(t, g15bReadManifest(t, name)) {
		if doc["kind"] == kind {
			return doc
		}
	}
	t.Fatalf("%s: kind %s not found", name, kind)
	return nil
}

func g15bDecodeDocs(t *testing.T, body string) []map[string]any {
	t.Helper()
	dec := yaml.NewDecoder(strings.NewReader(body))
	var out []map[string]any
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("decode yaml: %v", err)
		}
		if len(doc) > 0 {
			out = append(out, doc)
		}
	}
	return out
}

func g15bRepoRoot(t *testing.T) string {
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

func g15bMap(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("missing key %q in %v", key, m)
	}
	out, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("key %q has type %T, want map", key, v)
	}
	return out
}

func g15bSlice(t *testing.T, m map[string]any, key string) []any {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("missing key %q in %v", key, m)
	}
	out, ok := v.([]any)
	if !ok {
		t.Fatalf("key %q has type %T, want slice", key, v)
	}
	return out
}

func g15bContainer(t *testing.T, containers []any, name string) map[string]any {
	t.Helper()
	for _, raw := range containers {
		c, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("container has type %T, want map", raw)
		}
		if c["name"] == name {
			return c
		}
	}
	t.Fatalf("container %q not found", name)
	return nil
}

func g15bStripYAMLComments(body string) string {
	var out bytes.Buffer
	for _, line := range strings.Split(body, "\n") {
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return out.String()
}
