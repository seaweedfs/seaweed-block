package durable_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func qd1CrashProvider(t *testing.T, impl durable.ImplName, root string) (*durable.DurableProvider, *durable.StorageBackend) {
	t.Helper()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	cfg := durable.ProviderConfig{Impl: impl, StorageRoot: root, NumBlocks: 128, BlockSize: 4096, ChunkBlocks: 4}
	if impl == durable.ImplWALStoreChunk {
		remote := storage.NewMemChunkRemote()
		cfg.ChunkRemoteFor = func(string) (storage.ChunkRemote, error) { return remote, nil }
	}
	p, err := durable.NewDurableProvider(cfg, newStubView(healthyProj(id)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	return p, raw.(*durable.StorageBackend)
}

func TestQD1BackendSyncCrashWriter(t *testing.T) {
	impl := durable.ImplName(os.Getenv("QD1_CRASH_IMPL"))
	if impl == "" {
		t.Skip("subprocess only")
	}
	p, b := qd1CrashProvider(t, impl, os.Getenv("QD1_CRASH_ROOT"))
	_ = p // Deliberately no Close: the parent reopens after abrupt process exit.
	before := b.WriteProfile().BackendSyncOps
	for i := 0; i < 64; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		if _, err := b.Write(context.Background(), int64(i)*4096, data); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 2; j++ {
			if err := b.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := b.WriteProfile().BackendSyncOps - before; n != 128 {
		t.Fatalf("Sync calls=%d", n)
	}
	fmt.Printf("QD1_CRASH_READY impl=%s Syncs=128 blocks=64 no_Close=true\n", impl)
	os.Exit(0)
}

func TestQD1BackendSyncCrashReopen(t *testing.T) {
	for _, impl := range []durable.ImplName{durable.ImplSmartWAL, durable.ImplWALStoreChunk} {
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			childCtx, cancelChild := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelChild()
			cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestQD1BackendSyncCrashWriter$", "-test.v")
			cmd.WaitDelay = 2 * time.Second
			cmd.Env = append(os.Environ(), "QD1_CRASH_IMPL="+string(impl), "QD1_CRASH_ROOT="+root)
			out, err := cmd.CombinedOutput()
			t.Logf("%s", out)
			if err != nil {
				t.Fatal(err)
			}
			marker := fmt.Sprintf("QD1_CRASH_READY impl=%s Syncs=128 blocks=64 no_Close=true", impl)
			if !bytes.Contains(out, []byte(marker)) {
				t.Fatal("child did not reach acknowledged cut")
			}
			p, b := qd1CrashProvider(t, impl, root)
			defer p.Close()
			for i := 0; i < 64; i++ {
				got := make([]byte, 4096)
				if _, err := b.Read(context.Background(), int64(i)*4096, got); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, bytes.Repeat([]byte{byte(i + 1)}, 4096)) {
					t.Fatalf("acknowledged block %d lost after abrupt exit", i)
				}
			}
		})
	}
}
