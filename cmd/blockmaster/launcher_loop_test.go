package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/launcher"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG15d_BlockmasterLauncherTickWritesBlockvolumeManifest(t *testing.T) {
	h, err := master.New(master.Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	defer func() { _ = h.Close(context.Background()) }()
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "10.0.0.2:9201",
		CtrlAddr: "10.0.0.2:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "default",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	outDir := t.TempDir()
	if err := runLifecycleLauncherTick(h, flags{
		launcherManifestDir:   outDir,
		launcherNamespace:     "kube-system",
		launcherImage:         "sw-block:test",
		launcherMasterAddr:    "blockmaster.kube-system.svc.cluster.local:9333",
		launcherDurableRoot:   "/var/lib/sw-block",
		launcherISCSIPortBase: 3260,
	}); err != nil {
		t.Fatalf("launcher tick: %v", err)
	}
	path := filepath.Join(outDir, "sw-blockvolume-pvc-a-r1.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	body := string(raw)
	for _, want := range []string{
		"kind: Deployment",
		"name: sw-blockvolume-pvc-a-r1",
		"--master=blockmaster.kube-system.svc.cluster.local:9333",
		"--volume-id=pvc-a",
		"--replica-id=r1",
		"--iscsi-listen=127.0.0.1:3260",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("manifest missing %q:\n%s", want, body)
		}
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("pvc-a"); ok {
		t.Fatal("launcher tick must not mint authority")
	}
}

func TestG15d_BlockmasterLauncherTickCanRenderHostPathState(t *testing.T) {
	h, err := master.New(master.Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	defer func() { _ = h.Close(context.Background()) }()
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "10.0.0.2:9201",
		CtrlAddr: "10.0.0.2:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "default",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	outDir := t.TempDir()
	if err := runLifecycleLauncherTick(h, flags{
		launcherManifestDir:   outDir,
		launcherNamespace:     "kube-system",
		launcherImage:         "sw-block:test",
		launcherMasterAddr:    "blockmaster.kube-system.svc.cluster.local:9333",
		launcherDurableRoot:   "/var/lib/sw-block",
		launcherStateHostPath: "/var/lib/sw-block",
		launcherISCSIPortBase: 3260,
	}); err != nil {
		t.Fatalf("launcher tick: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(outDir, "sw-blockvolume-pvc-a-r1.yaml"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	body := string(raw)
	stateSection := yamlStateVolumeSection(t, body)
	for _, want := range []string{
		"hostPath:",
		"path: /var/lib/sw-block",
		"type: DirectoryOrCreate",
	} {
		if !strings.Contains(stateSection, want) {
			t.Fatalf("state volume missing %q:\n%s", want, stateSection)
		}
	}
	if strings.Contains(stateSection, "emptyDir:") {
		t.Fatalf("hostPath state volume must not render emptyDir:\n%s", stateSection)
	}
}

func yamlStateVolumeSection(t *testing.T, body string) string {
	t.Helper()
	lines := strings.Split(body, "\n")
	volumesIndent := -1
	stateStart := -1
	stateIndent := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		indent := len(line) - len(strings.TrimLeft(line, " "))
		if volumesIndent < 0 {
			if trimmed == "volumes:" {
				volumesIndent = indent
			}
			continue
		}
		if indent <= volumesIndent && trimmed != "" {
			break
		}
		if trimmed == "- name: state" {
			stateStart = i
			stateIndent = indent
			break
		}
	}
	if stateStart < 0 {
		t.Fatalf("state volume section not found:\n%s", body)
	}
	stateEnd := len(lines)
	for i := stateStart + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		indent := len(lines[i]) - len(strings.TrimLeft(lines[i], " "))
		if indent == stateIndent && strings.HasPrefix(trimmed, "- name:") {
			stateEnd = i
			break
		}
		if indent < stateIndent && trimmed != "" {
			stateEnd = i
			break
		}
	}
	return strings.Join(lines[stateStart:stateEnd], "\n")
}

func TestG15d_BlockmasterLauncherTickRendersNVMeManifestFromLifecycleProtocol(t *testing.T) {
	h, err := master.New(master.Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	defer func() { _ = h.Close(context.Background()) }()
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
		Protocol:          "nvme",
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "10.0.0.2:9201",
		CtrlAddr: "10.0.0.2:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "default",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	outDir := t.TempDir()
	if err := runLifecycleLauncherTick(h, flags{
		launcherManifestDir:  outDir,
		launcherNamespace:    "kube-system",
		launcherImage:        "sw-block:test",
		launcherMasterAddr:   "blockmaster.kube-system.svc.cluster.local:9333",
		launcherDurableRoot:  "/var/lib/sw-block",
		launcherNVMePortBase: 4420,
	}); err != nil {
		t.Fatalf("launcher tick: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(outDir, "sw-blockvolume-pvc-a-r1.yaml"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	body := string(raw)
	for _, want := range []string{
		"--nvme-listen=127.0.0.1:4420",
		"--nvme-subsysnqn=nqn.2026-05.io.seaweedfs:pvc-a",
		"--nvme-ns=1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("nvme manifest missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "--iscsi-listen=") || strings.Contains(body, "--iscsi-iqn=") {
		t.Fatalf("nvme manifest must not render iscsi args:\n%s", body)
	}
}

func TestG15e_BlockmasterLauncherTickRemovesManifestAfterVolumeDelete(t *testing.T) {
	h, err := master.New(master.Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	defer func() { _ = h.Close(context.Background()) }()
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "10.0.0.2:9201",
		CtrlAddr: "10.0.0.2:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "default",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	outDir := t.TempDir()
	f := flags{
		launcherManifestDir:   outDir,
		launcherNamespace:     "kube-system",
		launcherImage:         "sw-block:test",
		launcherMasterAddr:    "blockmaster.kube-system.svc.cluster.local:9333",
		launcherDurableRoot:   "/var/lib/sw-block",
		launcherISCSIPortBase: 3260,
	}
	path := filepath.Join(outDir, "sw-blockvolume-pvc-a-r1.yaml")
	if err := runLifecycleLauncherTick(h, f); err != nil {
		t.Fatalf("first launcher tick: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("manifest missing before delete: %v", err)
	}
	if err := stores.Placements.DeletePlacement("pvc-a"); err != nil {
		t.Fatalf("delete placement: %v", err)
	}
	if err := stores.Volumes.DeleteVolume("pvc-a"); err != nil {
		t.Fatalf("delete volume: %v", err)
	}
	if err := runLifecycleLauncherTick(h, f); err != nil {
		t.Fatalf("second launcher tick: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("manifest still exists after delete or stat err=%v", err)
	}
}

func TestLifecycleLauncherKubernetesApplyTracksRenderedNamespaces(t *testing.T) {
	rendered := []launcher.RenderedManifest{
		{
			Name: "sw-blockvolume-pvc-a-r1",
			YAML: []byte(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sw-blockvolume-pvc-a-r1
  namespace: default
  labels:
    app: sw-blockvolume
    sw-block.seaweedfs.com/volume: pvc-a
    sw-block.seaweedfs.com/replica: r1
`),
		},
		{
			Name: "sw-blockvolume-pvc-b-r1",
			YAML: []byte(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sw-blockvolume-pvc-b-r1
  namespace: apps
  labels:
    app: sw-blockvolume
    sw-block.seaweedfs.com/volume: pvc-b
    sw-block.seaweedfs.com/replica: r1
`),
		},
	}
	namespaces := renderedNamespaces(rendered, "kube-system")
	wantNamespaces := []string{"kube-system", "default", "apps"}
	if !reflect.DeepEqual(namespaces, wantNamespaces) {
		t.Fatalf("namespaces=%v want %v", namespaces, wantNamespaces)
	}
	defaultRendered := renderedForNamespace(rendered, "default")
	if len(defaultRendered) != 1 || defaultRendered[0].Name != "sw-blockvolume-pvc-a-r1" {
		t.Fatalf("default rendered=%+v", defaultRendered)
	}
}
