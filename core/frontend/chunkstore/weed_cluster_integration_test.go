//go:build chunkstore_integration

// Real SeaweedFS processes (weed master, volume, filer) started as the
// current user, for the integration tests of this package. Run with:
//
//	CHUNKSTORE_WEED=/path/to/weed go test -tags chunkstore_integration -run Integration -v ./core/frontend/chunkstore/
package chunkstore

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
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
