package master

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestMasterLifecycleStore_OptionalNilByDefault(t *testing.T) {
	h := newTestMaster(t, "")
	defer closeTestMaster(t, h)
	if h.Lifecycle() != nil {
		t.Fatal("Lifecycle() = non-nil without LifecycleStoreDir")
	}
}

func TestMasterLifecycleStore_OpensRegistrationStoresWithoutAuthority(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if stores == nil {
		t.Fatal("Lifecycle() = nil with LifecycleStoreDir")
	}
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("create desired volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "node-b",
		Addr:     "127.0.0.1:9102",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "pool-b",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register second node: %v", err)
	}
	results := lifecycle.ReconcilePlacement(stores.Volumes.ListVolumes(), stores.Nodes.ListNodes(), stores.Placements)
	if len(results) != 1 || !results[0].Applied {
		t.Fatalf("reconcile results=%+v want one applied", results)
	}
	if _, ok := stores.Placements.GetPlacement("vol-a"); !ok {
		t.Fatal("placement intent not written")
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatal("lifecycle registration must not mint authority")
	}
}

func newTestMaster(t *testing.T, lifecycleDir string) *Host {
	t.Helper()
	h, err := New(Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: lifecycleDir,
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	return h
}

func closeTestMaster(t *testing.T, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}
