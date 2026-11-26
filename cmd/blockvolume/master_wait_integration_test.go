//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"slices"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type masterWaitView struct {
	Reason string
	Since  time.Time
	Until  time.Time
}

// masterWaitOf reads /status MasterWait for v1 (ok false when absent).
func masterWaitOf(statusAddr string) (masterWaitView, bool) { return masterWaitOfVolume(statusAddr, "v1") }

// masterWaitOfVolume reads /status MasterWait for vol.
func masterWaitOfVolume(statusAddr, vol string) (masterWaitView, bool) {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + statusAddr + "/status?volume=" + vol)
	if err != nil {
		return masterWaitView{}, false
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var st struct {
		MasterWait *struct {
			Reason           string
			SinceMs, UntilMs int64
		}
	}
	if json.Unmarshal(raw, &st) != nil || st.MasterWait == nil {
		return masterWaitView{}, false
	}
	v := masterWaitView{Reason: st.MasterWait.Reason, Since: time.UnixMilli(st.MasterWait.SinceMs)}
	if st.MasterWait.UntilMs != 0 {
		v.Until = time.UnixMilli(st.MasterWait.UntilMs)
	}
	return v, true
}

func stillRunning(t *testing.T, d *daemon, what string) {
	t.Helper()
	select {
	case <-d.done:
		t.Fatalf("%s: %s exited (%v); it must wait", what, d.name, d.err)
	default:
	}
}

// waitForBarrier starts a waiting process with the master down, restarts the
// master and returns the process and the barrier time /status shows.
func waitForBarrier(t *testing.T, env *handoffEnv, name string) (*daemon, time.Time) {
	t.Helper()
	b := env.hosts["s1"]
	first := env.start(t, name+"-holder", "s1")
	env.adopt(t, first, "s1")
	env.masterProcess.kill(t)
	if code := exitCode(first.wait(90 * time.Second)); code != 75 {
		t.Fatalf("the holder exited %v with the master down; want one fence exit 75", first.err)
	}
	waiting := env.start(t, name, "s1")
	time.Sleep(45 * time.Second) // past the old fixed 30 s wait
	stillRunning(t, waiting, "45 s with the master unreachable")
	mw, ok := masterWaitOf(b.status)
	if !ok || mw.Reason != "unreachable" || !mw.Until.IsZero() || time.Since(mw.Since) < 40*time.Second {
		t.Fatalf("/status MasterWait %+v (present %v); want unreachable for 40 s or more, no bound", mw, ok)
	}
	env.masterProcess = env.h.start(t, name+"-master-back", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	var until time.Time
	poll(t, 30*time.Second, "/status never showed the master's barrier", func() bool {
		mw, ok := masterWaitOf(b.status)
		if ok && mw.Reason == "barrier" && !mw.Until.IsZero() {
			until = mw.Until
			return true
		}
		return false
	})
	waiting.waitLog(t, 10*time.Second, "re-grant refused until")
	return waiting, until
}

// A start that finds blockmaster unreachable waits inside the process with
// no bound (no exit 75 every ~38 s), shows why on /status, and when the
// master returns shows the master's barrier time and serves within 2 s of
// it, without a restart.
func TestMasterRestartShowsBarrierUntil(t *testing.T) {
	env := startHandoffEnv(t, 1)
	b := env.hosts["s1"]
	waiting, until := waitForBarrier(t, env, "bv-wait")
	t.Logf("barrier until %s (%v from now)", until.UTC().Format(time.RFC3339Nano), time.Until(until).Round(time.Millisecond))
	deadline := until.Add(2 * time.Second)
	for !volumeFrontendReady(b.status, "v1") {
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatal("the waiting process never served after the barrier")
		}
		stillRunning(t, waiting, "after the barrier")
		time.Sleep(100 * time.Millisecond)
	}
	late := time.Since(until)
	t.Logf("served %v after the barrier time", late.Round(time.Millisecond))
	if late > 2*time.Second {
		t.Fatalf("served %v after the master's barrier time; want <= 2 s", late.Round(time.Millisecond))
	}
	if _, ok := masterWaitOf(b.status); ok {
		t.Fatal("/status still shows MasterWait while serving")
	}
}

// SIGTERM (systemctl stop) during either wait ends the process with status 0
// within 10 s: while the master is unreachable, and at the barrier.
func TestStopDuringMasterWaitReturnsPromptly(t *testing.T) {
	env := startHandoffEnv(t, 1)
	stopIn := func(d *daemon, what string) {
		t.Helper()
		at := time.Now()
		d.term(t, 10*time.Second)
		if d.err != nil {
			t.Fatalf("%s: exit %v after SIGTERM; want status 0", what, d.err)
		}
		t.Logf("%s: stopped in %v", what, time.Since(at).Round(time.Millisecond))
	}
	waiting, _ := waitForBarrier(t, env, "bv-stop")
	stopIn(waiting, "at the barrier")

	env.masterProcess.kill(t)
	unreachable := env.start(t, "bv-stop-unreachable", "s1")
	poll(t, 30*time.Second, "/status never showed unreachable", func() bool {
		mw, ok := masterWaitOf(env.hosts["s1"].status)
		return ok && mw.Reason == "unreachable"
	})
	stopIn(unreachable, "unreachable")
}

// A process waiting at the barrier with its cached assignment at e, frozen
// while a forced move gives the line to s2 at e+1, then resumed: the
// master's typed MOVED refusal ends it with exit 78 moved at once. It never
// served.
func TestWaitingVolumeMovedDuringOutageExits78(t *testing.T) {
	env := startHandoffEnv(t, 2)
	b := env.hosts["s1"]
	holder := env.start(t, "bv-moved-holder", "s1")
	e := env.adopt(t, holder, "s1")
	holder.kill(t)
	time.Sleep(35 * time.Second) // the lease has lapsed; the barrier has not
	waiting := env.start(t, "bv-moved-waiting", "s1")
	poll(t, 30*time.Second, "/status never showed the barrier", func() bool {
		mw, ok := masterWaitOf(b.status)
		return ok && mw.Reason == "barrier"
	})
	if err := waiting.proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	env.forceMove(t, e, "s2", "force-moved-during-wait")
	if err := waiting.proc.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	err := waiting.wait(60 * time.Second)
	if code := exitCode(err); code != 78 || !waiting.has("blockvolume: exiting status=78 reason=moved") {
		t.Fatalf("the waiting process exit %v; want 78 reason=moved", err)
	}
	if waiting.has("adopted as PRIMARY") && volumeFrontendReady(b.status, "v1") {
		t.Fatal("the moved process served")
	}
}

// A unit for a volume the master does not assign to this host stops: with
// the master answering, the wait is bounded (30 s from contact) and exits
// 75 assignment_timeout, so the unit still reaches its start limit.
func TestObsoleteUnitStopsWithReachableMaster(t *testing.T) {
	env := startHandoffEnv(t, 1)
	b := env.hosts["s1"]
	b.volume, b.root = "v-unknown", t.TempDir()
	at := time.Now()
	d := env.h.start(t, "bv-obsolete", env.h.volume, env.volArgsFor(b)...)
	err := d.wait(2 * time.Minute)
	took := time.Since(at)
	t.Logf("obsolete unit exited %v after %v", err, took.Round(time.Millisecond))
	if exitCode(err) != 75 || !d.has("reason=assignment_timeout") || took < 25*time.Second {
		t.Fatalf("obsolete unit exit %v after %v; want 75 assignment_timeout after the 30 s bound", err, took)
	}
}

// revokeAndExit seals v1 on s1, revokes it at e and waits for the holder's
// clean exit and recorded ACK: the volume is then admissible for delete.
func revokeAndExit(t *testing.T, env *handoffEnv, hostID string, holder *daemon, e uint64) {
	t.Helper()
	b := env.hosts[hostID]
	out, _ := env.h.command(t, "ops", "chunk", "detach", "--plain", "--volume", "v1", "--status-addr", b.status, "--timeout", "120s")
	var a struct {
		HeadGen uint64 `json:"headGen"`
	}
	if err := json.Unmarshal([]byte(out), &a); err != nil || a.HeadGen == 0 {
		t.Fatalf("plain detach: %v %q", err, out)
	}
	env.h.command(t, "ops", "workspace", "revoke", "--master", env.h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--sealed-gen", fmt.Sprint(a.HeadGen))
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("holder after the revocation: %v; want a clean exit", err)
	}
	poll(t, 30*time.Second, "the ACK was never recorded", func() bool {
		out, _ := env.h.command(t, "ops", "workspace", "show", "--master", env.h.masterAddr, "--volume", "v1")
		return showsAck(out, e, a.HeadGen)
	})
}

// A process waiting on its cached assignment while the volume's delete is
// committed exits 78 deleted at once (the master's typed DELETED refusal);
// while the delete is only admitted (abortable) it keeps the bounded wait and
// exits 75 assignment_timeout, never 78.
func TestWaitingVolumeDeletedDuringOutageExits78(t *testing.T) {
	t.Run("committed", func(t *testing.T) {
		// The waiting s1 process reaches the master through a proxy the test
		// cuts: while it cannot reach the master (no bound), the line moves to
		// s2, s2 is revoked with an ACK and the delete commits. When the
		// proxy returns, the renewal at its cached epoch meets the committed
		// deletion.
		env := startHandoffEnv(t, 2)
		holder := env.start(t, "bv-del-holder", "s1")
		e := env.adopt(t, holder, "s1")
		holder.kill(t)
		px := startMasterProxy(t, env.h.masterAddr)
		args := env.volArgs("s1")
		for i := range args {
			if args[i] == "--master" {
				args[i+1] = px.addr
			}
		}
		time.Sleep(35 * time.Second) // the lease lapsed; the barrier has not passed
		waiting := env.h.start(t, "bv-del-waiting", env.h.volume, args...)
		poll(t, 30*time.Second, "/status never showed the barrier", func() bool {
			mw, ok := masterWaitOf(env.hosts["s1"].status)
			return ok && mw.Reason == "barrier"
		})
		px.cut()
		env.forceMove(t, e, "s2", "force-before-delete")
		s2 := env.start(t, "bv-del-s2", "s2")
		e2 := env.adopt(t, s2, "s2")
		revokeAndExit(t, env, "s2", s2, e2)
		out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC,
			"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-while-waiting")
		var del struct{ Completed bool }
		if code != 0 || json.Unmarshal([]byte(out), &del) != nil || !del.Completed {
			t.Fatalf("volume-delete while a process waits: exit %d %s %s; want completed", code, out, errOut)
		}
		stillRunning(t, waiting, "while the master was unreachable")
		px.restore()
		err := waiting.wait(60 * time.Second)
		if exitCode(err) != 78 || !waiting.has("blockvolume: exiting status=78 reason=deleted") {
			t.Fatalf("waiting process after the committed delete: %v; want 78 reason=deleted", err)
		}
	})
	t.Run("admitted", func(t *testing.T) {
		env := startHandoffEnv(t, 1)
		holder := env.start(t, "bv-adm-holder", "s1")
		e := env.adopt(t, holder, "s1")
		revokeAndExit(t, env, "s1", holder, e)
		conn, err := grpc.NewClient(env.h.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := control.NewLifecycleServiceClient(conn).MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v1", Token: "admit-only"}); err != nil {
			t.Fatal("admit the delete:", err)
		}
		at := time.Now()
		waiting := env.start(t, "bv-adm-waiting", "s1")
		err = waiting.wait(2 * time.Minute)
		if exitCode(err) != 75 || !waiting.has("reason=assignment_timeout") || time.Since(at) < 25*time.Second {
			t.Fatalf("waiting process under an admitted delete: %v after %v; want the bounded 75 assignment_timeout", err, time.Since(at))
		}
	})
}

// After a master restart every lapsed volume gets the same barrier time; with
// 20 volume processes (one per volume) waiting, each must serve within 2 s of
// its barrier: the guards' jittered wake and the publisher's busy refusals
// must not keep them in step. Twenty processes: run only when
// SWB_MANY_VOLUMES=1 (not in every CI pass).
func TestManyVolumesServeTogetherAfterMasterRestart(t *testing.T) {
	if os.Getenv("SWB_MANY_VOLUMES") != "1" {
		t.Skip("20 volume processes: set SWB_MANY_VOLUMES=1")
	}
	const n = 20
	var targets []string
	for i := 2; i <= n; i++ {
		targets = append(targets, fmt.Sprintf("v%d", i))
	}
	env := startHandoffEnvVolumes(t, 1, targets)
	hosts := []bvHost{env.hosts["s1"]}
	hosts[0].volume = "v1"
	for _, v := range targets {
		hosts = append(hosts, env.hosts["n-"+v])
	}
	start := func(round string) []*daemon {
		ds := make([]*daemon, len(hosts))
		for i, b := range hosts {
			ds[i] = env.h.start(t, fmt.Sprintf("bv-%s-%s", round, b.volume), env.h.volume, env.volArgsFor(b)...)
		}
		return ds
	}
	first := start("first")
	for _, b := range hosts {
		poll(t, 3*time.Minute, b.volume+" never served", func() bool { return volumeFrontendReady(b.status, b.volume) })
	}
	env.masterProcess.kill(t)
	for i, d := range first {
		if code := exitCode(d.wait(90 * time.Second)); code != 75 {
			t.Fatalf("%s holder exited %v with the master down; want 75", hosts[i].volume, d.err)
		}
	}
	waiting := start("waiting")
	wBefore := storeWriteP99(t, env.h.dir)
	time.Sleep(10 * time.Second)
	env.masterProcess = env.h.start(t, "blockmaster-back", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")

	// One watcher per volume, every 50 ms: its barrier time from /status,
	// then the moment it serves.
	until := make([]time.Time, len(hosts))
	served := make([]time.Time, len(hosts))
	seen := make([]map[string]bool, len(hosts)) // the MasterWait reasons each watcher saw
	var wg sync.WaitGroup
	deadline := time.Now().Add(3 * time.Minute)
	for i, b := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seen[i] = map[string]bool{}
			for time.Now().Before(deadline) {
				if mw, ok := masterWaitOfVolume(b.status, b.volume); ok {
					seen[i][mw.Reason] = true
					if until[i].IsZero() && mw.Reason == "barrier" {
						until[i] = mw.Until
					}
				}
				if volumeFrontendReady(b.status, b.volume) {
					served[i] = time.Now()
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	for i, b := range hosts {
		if served[i].IsZero() {
			t.Fatalf("%s never served after the master returned", b.volume)
		}
	}
	// After a master restart every lapsed volume's barrier is the master's
	// reload plus TTL plus skew: the same T. A volume whose watcher missed it
	// on /status is measured against the latest T the others showed.
	var common time.Time
	captured := 0
	for _, u := range until {
		if !u.IsZero() {
			captured++
			if u.After(common) {
				common = u
			}
		}
	}
	if captured < len(hosts)/2 {
		t.Fatalf("only %d of %d volumes showed the barrier on /status", captured, len(hosts))
	}
	var worst time.Duration
	for i, b := range hosts {
		stillRunning(t, waiting[i], b.volume)
		ref, how := until[i], "its barrier"
		if ref.IsZero() {
			ref, how = common, "the common barrier (reasons seen: "+fmt.Sprint(seen[i])+")"
		}
		late := served[i].Sub(ref)
		t.Logf("%s served %v after %s", b.volume, late.Round(time.Millisecond), how)
		worst = max(worst, late)
	}
	w := storeWriteP99(t, env.h.dir)
	t.Logf("%d volumes: worst %v after the barrier; master store device W p99 %v (measured around the restart: %v)",
		len(hosts), worst.Round(time.Millisecond), w.Round(time.Microsecond), []time.Duration{wBefore.Round(time.Microsecond), w.Round(time.Microsecond)})
	w = max(w, wBefore)
	// The bound is T + 0.1 s + N x W for the store device; "within 2 s of
	// the barrier" is a claim about a device that does not stall. Assert it
	// only where this run measured such a device; otherwise report.
	if w > storeWriteP99Limit {
		t.Logf("2 s bar not asserted: the store device stalled in this run (W p99 %v > %v)", w.Round(time.Millisecond), storeWriteP99Limit)
		return
	}
	if worst > 2*time.Second {
		t.Fatalf("a volume served %v after its barrier with W p99 %v; want every one within 2 s", worst.Round(time.Millisecond), w.Round(time.Microsecond))
	}
}

// storeWriteP99Limit is the store write p99 below which the 20-volume bar is
// asserted (a quiet NVMe store measured 2.3 ms).
const storeWriteP99Limit = 20 * time.Millisecond

// storeWriteP99 measures the p99 of 100 authority store writes (the grant's
// durable write) in a directory beside the master's store.
func storeWriteP99(t *testing.T, dir string) time.Duration {
	t.Helper()
	d, err := os.MkdirTemp(dir, "w-probe-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(d)
	s, err := authority.NewFileAuthorityStore(d)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var ws []time.Duration
	for i := 0; i < 100; i++ {
		r := authority.DurableRecord{VolumeID: fmt.Sprintf("w%03d", i), ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:2", WriteSeq: 1,
			Workspace: &authority.WorkspaceState{Lease: &authority.WorkspaceLease{ReplicaID: "r1", Epoch: 1, ExpiresAtNS: time.Now().UnixNano()}}}
		start := time.Now()
		if err := s.Put(r); err != nil {
			t.Fatal(err)
		}
		ws = append(ws, time.Since(start))
	}
	slices.Sort(ws)
	return ws[98]
}

// masterProxy forwards TCP connections to the master; cut closes its
// listener and every open connection (the master is unreachable through it),
// restore listens again on the same address.
type masterProxy struct {
	t      *testing.T
	addr   string
	target string
	mu     sync.Mutex
	ln     net.Listener
	conns  []net.Conn
}

func startMasterProxy(t *testing.T, target string) *masterProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &masterProxy{t: t, addr: ln.Addr().String(), target: target}
	p.serve(ln)
	t.Cleanup(p.cut)
	return p
}

func (p *masterProxy) serve(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
}

func (p *masterProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *masterProxy) restore() {
	p.t.Helper()
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		p.t.Fatalf("proxy relisten %s: %v", p.addr, err)
	}
	p.serve(ln)
}

// A volume process whose server id is not registered (a misconfigured unit,
// or a clone) never serves and never exits 75: the master refuses it with the
// typed IDENTITY detail, and it waits visibly, /status MasterWait reason
// "identity refused: unregistered", until its registration is fixed. A
// restart cannot fix a registration, so a 75 loop would only burn the start
// limit. SIGTERM still ends it at once.
func TestUnregisteredServerWaitsVisibly(t *testing.T) {
	env := startHandoffEnv(t, 1)
	b := env.hosts["s1"]
	args := env.volArgs("s1")
	for i := range args {
		if args[i] == "--server-id" {
			args[i+1] = "s-unregistered"
		}
	}
	d := env.h.start(t, "bv-unregistered", env.h.volume, args...)
	poll(t, 30*time.Second, "/status never showed the identity refusal", func() bool {
		mw, ok := masterWaitOf(b.status)
		return ok && mw.Reason == "identity refused: unregistered" && mw.Until.IsZero()
	})
	time.Sleep(45 * time.Second) // past the 30 s bound a reachable master would give
	stillRunning(t, d, "45 s refused as unregistered")
	if volumeFrontendReady(b.status, "v1") {
		t.Fatal("an unregistered server served")
	}
	if mw, ok := masterWaitOf(b.status); !ok || mw.Reason != "identity refused: unregistered" || time.Since(mw.Since) < 40*time.Second {
		t.Fatalf("/status MasterWait %+v (present %v); want identity refused for 40 s or more", mw, ok)
	}
	d.term(t, 10*time.Second)
	if d.err != nil {
		t.Fatalf("exit %v after SIGTERM; want status 0", d.err)
	}
}
