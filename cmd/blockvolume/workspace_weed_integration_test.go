//go:build subprocess && chunkstore_integration

package main_test

// The process helper follows the existing chunkstore integration harness.
// Only real loopback SeaweedFS processes and per-test temporary state are used.
import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type w4WeedCluster struct {
	master, filerHTTP, filerGRPC string
	dir                          string
	weed                         string
	procs                        map[string]*w4WeedProc
}

func w4FreePortPair(t *testing.T, used map[int]bool) int {
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

func w4WaitTCP(t *testing.T, addr string, d time.Duration) {
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

func startWorkspaceWeed(t *testing.T) *w4WeedCluster {
	t.Helper()
	return startWorkspaceWeedAt(t, t.TempDir())
}

// A caller-owned root lets process qualification retain its filer evidence
// when external-service cleanup is uncertain. Existing tests keep TempDir.
func startWorkspaceWeedAt(t *testing.T, dir string) *w4WeedCluster {
	t.Helper()
	return startWorkspaceWeedWith(t, dir, w4WeedOpts{VolumeSizeLimitMB: 256, VolumeMax: 32})
}

// w4WeedOpts sizes the weed volume server: VolumeMax volumes of
// VolumeSizeLimitMB each (the default holds about 8 GiB of chunks).
type w4WeedOpts struct {
	VolumeSizeLimitMB, VolumeMax int
}

func startWorkspaceWeedWith(t *testing.T, dir string, o w4WeedOpts) *w4WeedCluster {
	t.Helper()
	weed := os.Getenv("CHUNKSTORE_WEED")
	if weed == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	used := map[int]bool{}
	mp, vp, fp := w4FreePortPair(t, used), w4FreePortPair(t, used), w4FreePortPair(t, used)
	c := &w4WeedCluster{
		master:    fmt.Sprintf("127.0.0.1:%d", mp),
		filerHTTP: fmt.Sprintf("127.0.0.1:%d", fp),
		filerGRPC: fmt.Sprintf("127.0.0.1:%d", fp+10000),
		dir:       dir,
		weed:      weed,
	}
	c.procs = map[string]*w4WeedProc{}
	run := func(name string, args ...string) {
		c.startProc(t, name, args...)
		t.Cleanup(func() {
			p := c.procs[name]
			if !c.stopProcWithin(name, 5*time.Second) {
				t.Errorf("weed %s did not exit after kill", name)
			}
			p.log.Close()
			if t.Failed() {
				b, _ := os.ReadFile(p.log.Name())
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
	run("master", "master", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", mp), "-mdir="+filepath.Join(dir, "m"), fmt.Sprintf("-volumeSizeLimitMB=%d", o.VolumeSizeLimitMB), "-defaultReplication=000")
	w4WaitTCP(t, c.master, 20*time.Second)
	run("volume", "volume", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", vp), "-dir="+filepath.Join(dir, "v"), "-master="+c.master, fmt.Sprintf("-max=%d", o.VolumeMax))
	run("filer", "filer", "-ip=127.0.0.1", fmt.Sprintf("-port=%d", fp), "-master="+c.master, "-defaultStoreDir="+filepath.Join(dir, "f"))
	w4WaitTCP(t, c.filerGRPC, 30*time.Second)
	// blockvolume refuses a chunk path whose storage rule does not set fsync.
	c.configureFsync(t)

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

// w4WeedProc is one weed process; a restart replaces it under the same name,
// arguments and log, so the cluster's cleanup always ends the current one.
type w4WeedProc struct {
	args []string
	log  *os.File
	cmd  *exec.Cmd
	done chan struct{}
}

func (c *w4WeedCluster) startProc(t *testing.T, name string, args ...string) {
	t.Helper()
	p := c.procs[name]
	if p == nil {
		logf, err := os.Create(filepath.Join(c.dir, name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		p = &w4WeedProc{args: args, log: logf}
		c.procs[name] = p
	}
	cmd := exec.Command(c.weed, p.args...)
	cmd.Stdout, cmd.Stderr = p.log, p.log
	cmd.Dir = c.dir
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	p.cmd, p.done = cmd, make(chan struct{})
	go func(done chan struct{}) { _ = cmd.Wait(); close(done) }(p.done)
}

// stopProcWithin kills name's current process and reports whether it exited.
func (c *w4WeedCluster) stopProcWithin(name string, d time.Duration) bool {
	p := c.procs[name]
	if p == nil || p.cmd == nil {
		return true
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// stopProc and restartProc take one weed process (master, volume, filer) down
// and bring it back with the same ports and data directory.
func (c *w4WeedCluster) stopProc(t *testing.T, name string) {
	t.Helper()
	if !c.stopProcWithin(name, 10*time.Second) {
		t.Fatalf("weed %s did not exit after kill", name)
	}
}

func (c *w4WeedCluster) restartProc(t *testing.T, name string) {
	t.Helper()
	c.startProc(t, name)
}

func (c *w4WeedCluster) client(t *testing.T, assignPath string) *chunkstore.SWFS {
	t.Helper()
	cl, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{FilerGRPC: c.filerGRPC, Collection: "swblock", AssignPath: assignPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func (c *w4WeedCluster) remote(t *testing.T, root string) *chunkstore.ChunkRemote {
	t.Helper()
	r, err := chunkstore.NewSWFSChunkRemote(c.client(t, root), root)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// configureFsync sets the filer storage rule every chunk path needs (fsync)
// on "/", so it covers any --chunk-path a test uses.
func (c *w4WeedCluster) configureFsync(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cmd := exec.CommandContext(ctx, c.weed, "shell", "-master="+c.master, "-filer="+c.filerHTTP)
		cmd.Dir = c.dir
		cmd.Stdin = strings.NewReader("fs.configure -locationPrefix=/ -fsync -apply\n")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		cmd.WaitDelay = 2 * time.Second
		err := cmd.Run()
		cancel()
		if err == nil {
			cl := c.client(t, "/")
			lctx, lcancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = chunkstore.CheckPathRule(lctx, cl, cl, "/swblock", "")
			lcancel()
			if err == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("configure the filer fsync rule: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
