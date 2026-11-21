//go:build chunkstore_integration

// The chunk remote against real SeaweedFS processes (weed master, volume,
// filer) started as the current user. The cluster helpers are the D1
// PoC's. Run with:
//
//	CHUNKSTORE_WEED=/path/to/weed go test -tags chunkstore_integration -run Integration -v ./core/frontend/chunkstore/
package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type weedCluster struct {
	master, filerHTTP, filerGRPC string
	dir                          string
	weed                         string
}

func freePortPair(t *testing.T, used map[int]bool) int {
	t.Helper()
	for i := 0; i < 200; i++ {
		p := 20000 + rand.Intn(20000)
		if used[p] || used[p+10000] {
			continue
		}
		ok := true
		for _, q := range []int{p, p + 10000} {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", q))
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
	t.Fatal("no free port pair")
	return 0
}

func waitTCP(t *testing.T, addr string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s not listening after %v", addr, d)
}

func startWeed(t *testing.T) *weedCluster {
	t.Helper()
	return startWeedWith(t, "")
}

// startWeedWith starts the cluster with security (the contents of a
// security.toml in its working directory; "" for none).
func startWeedWith(t *testing.T, security string) *weedCluster {
	t.Helper()
	weed := os.Getenv("CHUNKSTORE_WEED")
	if weed == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	dir := t.TempDir()
	used := map[int]bool{}
	mp, vp, fp := freePortPair(t, used), freePortPair(t, used), freePortPair(t, used)
	c := &weedCluster{
		master:    fmt.Sprintf("127.0.0.1:%d", mp),
		filerHTTP: fmt.Sprintf("127.0.0.1:%d", fp),
		filerGRPC: fmt.Sprintf("127.0.0.1:%d", fp+10000),
		dir:       dir,
		weed:      weed,
	}
	run := func(name string, args ...string) {
		logf, err := os.Create(filepath.Join(dir, name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(weed, args...)
		cmd.Stdout, cmd.Stderr = logf, logf
		cmd.Dir = dir
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			logf.Close()
			if t.Failed() {
				b, _ := os.ReadFile(logf.Name())
				if len(b) > 4000 {
					b = b[len(b)-4000:]
				}
				t.Logf("---- %s log tail ----\n%s", name, b)
			}
		})
	}
	for _, d := range []string{"m", "v", "f"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if security != "" {
		if err := os.WriteFile(filepath.Join(dir, "security.toml"), []byte(security), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run("master", "master", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", mp), "-mdir="+filepath.Join(dir, "m"), "-volumeSizeLimitMB=256", "-defaultReplication=000")
	waitTCP(t, c.master, 20*time.Second)
	run("volume", "volume", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", vp), "-dir="+filepath.Join(dir, "v"), "-master="+c.master, "-max=32")
	run("filer", "filer", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", fp), "-master="+c.master, "-defaultStoreDir="+filepath.Join(dir, "f"))
	waitTCP(t, c.filerGRPC, 30*time.Second)

	// Wait until an assign succeeds (volume registered, volumes growable).
	cl := c.client(t, "/swblock/vols/probe")
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cl.Put(ctx, []byte("probe"))
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster never accepted a write: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return c
}

func (c *weedCluster) client(t *testing.T, assignPath string) *SWFS {
	t.Helper()
	cl, err := DialSWFS(SWFSConfig{FilerGRPC: c.filerGRPC, Collection: "swblock", AssignPath: assignPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func (c *weedCluster) remote(t *testing.T, root string) *ChunkRemote {
	t.Helper()
	r, err := NewSWFSChunkRemote(c.client(t, root), root)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestIntegration_ChunkRemoteContract(t *testing.T) {
	c := startWeed(t)
	ctx := context.Background()
	const root = "/swblock/chunks/contract"
	a, b := c.remote(t, root), c.remote(t, root)

	if h, err := a.GetHead(ctx); err != nil || h.Gen != 0 {
		t.Fatalf("absent HEAD = %+v, %v", h, err)
	}
	d := bytes.Repeat([]byte("chunk-bytes-"), 1000)
	const key = "chunks/e1/c4/nonce-1"
	if err := a.Put(ctx, key, d); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := a.Put(ctx, key, []byte("other")); !errors.Is(err, storage.ErrChunkKeyExists) {
		t.Fatalf("rebind on the same client: %v", err)
	}
	if err := b.Put(ctx, key, []byte("other")); !errors.Is(err, storage.ErrChunkKeyExists) {
		t.Fatalf("rebind from another client (real o_excl): %v", err)
	}
	if got, err := b.Get(ctx, key); err != nil || !bytes.Equal(got, d) {
		t.Fatalf("Get from another client: %d bytes, %v", len(got), err)
	}

	h1 := storage.ChunkHead{Epoch: 1, Holder: "a", RemoteLSN: 7, Chunks: map[uint64]storage.ChunkRef{4: ref(key, d)}}
	if gen, err := a.CASHead(ctx, storage.ChunkHeadVersion{}, h1); err != nil || gen != 1 {
		t.Fatalf("first CAS = %d, %v", gen, err)
	}
	if _, err := b.CASHead(ctx, storage.ChunkHeadVersion{}, h1); !isPrecondition(err) {
		t.Fatalf("racing first CAS: %v; want precondition (real o_excl)", err)
	}
	got, err := b.GetHead(ctx)
	if err != nil || got.Gen != 1 || got.Chunks[4] != h1.Chunks[4] || got.RemoteLSN != 7 {
		t.Fatalf("HEAD from another client = %+v, %v", got, err)
	}
	// Takeover at epoch 2, then the old holder's CAS at the version it read.
	if gen, err := b.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, storage.ChunkHead{Epoch: 2, Holder: "b", RemoteLSN: 7, Chunks: h1.Chunks}); err != nil || gen != 2 {
		t.Fatalf("takeover CAS = %d, %v", gen, err)
	}
	_, err = a.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, storage.ChunkHead{Epoch: 1, Holder: "a", RemoteLSN: 9})
	if !isPrecondition(err) {
		t.Fatalf("old holder's CAS after takeover: %v; want precondition (real FailedPrecondition)", err)
	}
	t.Logf("old holder refused: %v", err)
	if _, err := a.CASHead(ctx, storage.ChunkHeadVersion{Gen: 2, Epoch: 2}, storage.ChunkHead{Epoch: 1, Holder: "a"}); !isPrecondition(err) {
		t.Fatalf("epoch-lowering CAS: %v; want precondition", err)
	}
	e, err := c.client(t, root).LookupEntry(ctx, root, headName)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.GetChunks()) != 0 || string(e.Extended[extGen]) != "2" || string(e.Extended[extEpoch]) != "2" {
		t.Fatalf("stored HEAD entry: %d filer chunks, gen %q, epoch %q", len(e.GetChunks()), e.Extended[extGen], e.Extended[extEpoch])
	}
}

func TestIntegration_ChunkLayerCaptureThenFreshHost(t *testing.T) {
	c := startWeed(t)
	ctx := context.Background()
	const root = "/swblock/chunks/v1"
	const blocks, chunkBlocks = 512, 64 // 256 KiB chunks
	dir := t.TempDir()
	attach := func(name string, fresh bool) (*storage.WALStore, storage.ChunkLayer) {
		p := filepath.Join(dir, name)
		s, err := storage.CreateWALStoreWithOptions(p, blocks, 4096, storage.WALStoreOptions{ChunkBlocks: chunkBlocks})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		l, err := storage.AttachChunkLayer(s, c.remote(t, root), storage.ChunkLayerConfig{
			ChunkBlocks: chunkBlocks, Epoch: 1, Holder: name, FreshFromHead: fresh,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s, l
	}
	src, layer := attach("a", true) // a new, empty source volume
	want := map[uint32][]byte{}
	rng := rand.New(rand.NewSource(1))
	for _, lba := range []uint32{0, 63, 64, 300, 511} {
		b := make([]byte, 4096)
		rng.Read(b)
		want[lba] = b
		if _, err := src.Write(lba, b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatalf("Capture: %v", err)
	}
	t.Logf("capture of 4 dirty chunks: %v", time.Since(start))
	_ = src.Close()

	dst, _ := attach("b", true)
	zero := make([]byte, 4096)
	for lba := uint32(0); lba < blocks; lba++ {
		got, err := dst.Read(lba)
		if err != nil {
			t.Fatalf("fresh host read %d: %v", lba, err)
		}
		exp, ok := want[lba]
		if !ok {
			exp = zero
		}
		if !bytes.Equal(got, exp) {
			t.Fatalf("fresh host block %d differs", lba)
		}
	}
}
