//go:build subprocess && chunkstore_integration

package main_test

// The process helper follows the existing chunkstore integration harness.
// Only real loopback SeaweedFS processes and per-test temporary state are used.
import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type w4WeedCluster struct {
	master, filerHTTP, filerGRPC string
	dir                          string
	weed                         string
	procs                        map[string]*w4WeedProc
}

// testListenHost is the address the test harness binds and advertises for
// its weed cluster, blockmaster and blockvolume ports: 127.0.0.1 unless
// SWBLOCK_TEST_LISTEN_HOST names another (a lab run whose processes span two
// hosts). Unset, every test behaves exactly as before.
func testListenHost() string {
	if h := os.Getenv("SWBLOCK_TEST_LISTEN_HOST"); h != "" {
		return h
	}
	return "127.0.0.1"
}

// ephemeralPorts is the host's range for outgoing connections' source ports
// (Linux ip_local_port_range; the Linux default when unreadable). A port in it
// can be taken by any outgoing connection between the free check and weed's
// bind, so a port pair never uses it.
func ephemeralPorts() (int, int) {
	lo, hi := 32768, 60999
	if b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		var a, z int
		if n, _ := fmt.Sscan(string(b), &a, &z); n == 2 && a > 0 && z >= a {
			lo, hi = a, z
		}
	}
	return lo, hi
}

func w4FreePortPair(t *testing.T, used map[int]bool) int {
	t.Helper()
	lo, hi := ephemeralPorts()
	for i := 0; i < 200; i++ {
		p := 20000 + rand.Intn(20000)
		if used[p] || used[p+10000] {
			continue
		}
		if (p >= lo && p <= hi) || (p+10000 >= lo && p+10000 <= hi) {
			continue
		}
		ok := true
		for _, q := range []int{p, p + 10000} {
			l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", testListenHost(), q))
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
	// A fault-injection front keeps the advertised filer endpoint unchanged.
	// The one real filer binds a private loopback address; no second filer is registered.
	FilerBind  string
	FilerFront func(*w4WeedCluster)
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
		master:    fmt.Sprintf("%s:%d", testListenHost(), mp),
		filerHTTP: fmt.Sprintf("%s:%d", testListenHost(), fp),
		filerGRPC: fmt.Sprintf("%s:%d", testListenHost(), fp+10000),
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
	run("master", "master", "-ip="+testListenHost(), fmt.Sprintf("-port=%d", mp), "-mdir="+filepath.Join(dir, "m"), fmt.Sprintf("-volumeSizeLimitMB=%d", o.VolumeSizeLimitMB), "-defaultReplication=000")
	w4WaitTCP(t, c.master, 20*time.Second)
	run("volume", "volume", "-ip="+testListenHost(), fmt.Sprintf("-port=%d", vp), "-dir="+filepath.Join(dir, "v"), "-master="+c.master, fmt.Sprintf("-max=%d", o.VolumeMax))
	filerArgs := []string{"filer", "-ip=" + testListenHost(), fmt.Sprintf("-port=%d", fp), "-master=" + c.master, "-defaultStoreDir=" + filepath.Join(dir, "f")}
	if o.FilerBind != "" {
		filerArgs = append(filerArgs, "-ip.bind="+o.FilerBind)
		c.filerHTTP = net.JoinHostPort(o.FilerBind, fmt.Sprint(fp))
		c.filerGRPC = net.JoinHostPort(o.FilerBind, fmt.Sprint(fp+10000))
	}
	run("filer", filerArgs...)
	w4WaitTCP(t, c.filerGRPC, 30*time.Second)
	if o.FilerFront != nil {
		o.FilerFront(c)
	}

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

// remote reads ref's HEAD under base as the chunk engine sees it, through the
// layout remote of an observer that holds nothing: its epoch, holder and
// claim match no HEAD, so it never rolls a session back or writes.
func (c *w4WeedCluster) remote(t *testing.T, base string, ref chunkstore.VolumeRef) *chunkstore.LayoutRemote {
	t.Helper()
	cl := c.client(t, base)
	cat, err := chunkstore.NewBCatalog(cl, cl, base)
	if err != nil {
		t.Fatal(err)
	}
	r, err := chunkstore.NewLayoutRemote(cat, ref, layout.HolderID{Epoch: math.MaxUint64, Holder: "test-observer", ClaimID: "observer"})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
