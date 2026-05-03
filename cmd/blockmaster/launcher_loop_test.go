package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/host/master"
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
