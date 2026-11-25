//go:build chunkstore_integration

package durable_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The B arm uses a real filer so the child and parent observe the same HEAD.
// Run with CHUNKSTORE_WEED=/path/to/weed and -tags chunkstore_integration.
func qd1LayoutWeed(t *testing.T) string {
	t.Helper()
	weed := os.Getenv("CHUNKSTORE_WEED")
	if weed == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	dir := t.TempDir()
	used := map[int]bool{}
	port := func() int {
		t.Helper()
		for i := 0; i < 200; i++ {
			p := 20000 + rand.Intn(20000)
			if used[p] || used[p+10000] {
				continue
			}
			ok := true
			for _, n := range []int{p, p + 10000} {
				l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", n))
				if err != nil {
					ok = false
					break
				}
				l.Close()
			}
			if ok {
				used[p], used[p+10000] = true, true
				return p
			}
		}
		t.Fatal("no free local port pair")
		return 0
	}
	mp, vp, fp := port(), port(), port()
	run := func(name string, args ...string) {
		t.Helper()
		f, err := os.Create(filepath.Join(dir, name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(weed, args...)
		cmd.Dir, cmd.Stdout, cmd.Stderr = dir, f, f
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			_ = f.Close()
			if t.Failed() {
				b, _ := os.ReadFile(f.Name())
				if len(b) > 4000 {
					b = b[len(b)-4000:]
				}
				t.Logf("%s log tail:\n%s", name, b)
			}
		})
	}
	wait := func(addr string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err == nil {
				_ = c.Close()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("%s did not start", addr)
	}
	for _, name := range []string{"m", "v", "f"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	master := fmt.Sprintf("127.0.0.1:%d", mp)
	run("master", "master", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", mp), "-mdir="+filepath.Join(dir, "m"), "-volumeSizeLimitMB=256", "-defaultReplication=000")
	wait(master)
	run("volume", "volume", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", vp), "-dir="+filepath.Join(dir, "v"), "-master="+master, "-max=32")
	run("filer", "filer", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", fp), "-master="+master, "-defaultStoreDir="+filepath.Join(dir, "f"))
	grpc := fmt.Sprintf("127.0.0.1:%d", fp+10000)
	wait(grpc)
	return grpc
}

func qd1LayoutProvider(t *testing.T, root, grpc, base, idText string) (*durable.DurableProvider, *durable.StorageBackend) {
	t.Helper()
	id, err := layout.ParseVolumeID(idText)
	if err != nil {
		t.Fatal(err)
	}
	client, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{FilerGRPC: grpc, Collection: "swblock", AssignPath: base})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := chunkstore.NewBCatalog(client, client, base)
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 128, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1"}
	cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return cat.Store(ref) }
	cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
		return chunkstore.NewLayoutRemote(cat, ref, me)
	}
	identity := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	p, err := durable.NewDurableProvider(cfg, newStubView(healthyProj(identity)))
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

func TestQD1LayoutSyncCrashWriter(t *testing.T) {
	grpc := os.Getenv("QD1_LAYOUT_GRPC")
	if grpc == "" {
		t.Skip("subprocess only")
	}
	p, b := qd1LayoutProvider(t, os.Getenv("QD1_LAYOUT_ROOT"), grpc, os.Getenv("QD1_LAYOUT_BASE"), os.Getenv("QD1_LAYOUT_ID"))
	_ = p // Exit without Close; the parent reopens the same root and filer.
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
	fmt.Println("QD1_LAYOUT_CRASH_READY Syncs=128 blocks=64 no_Close=true")
	os.Exit(0)
}

func TestQD1LayoutSyncCrashReopen(t *testing.T) {
	grpc := qd1LayoutWeed(t)
	base := fmt.Sprintf("/swblock/qd1-%d", time.Now().UnixNano())
	client, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{FilerGRPC: grpc, Collection: "swblock", AssignPath: base})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cat, err := chunkstore.NewBCatalog(client, client, base)
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateVolume(context.Background(), chunkstore.VolumeRef{Name: "v1", ID: id}, layout.Geometry{SizeBytes: 128 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "local")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQD1LayoutSyncCrashWriter$", "-test.v")
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = append(os.Environ(), "QD1_LAYOUT_GRPC="+grpc, "QD1_LAYOUT_ROOT="+root, "QD1_LAYOUT_BASE="+base, "QD1_LAYOUT_ID="+id.String())
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("QD1_LAYOUT_CRASH_READY Syncs=128 blocks=64 no_Close=true")) {
		t.Fatal("child did not reach acknowledged sync")
	}
	p, b := qd1LayoutProvider(t, root, grpc, base, id.String())
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
}
