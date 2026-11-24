//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

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
func masterWaitOf(statusAddr string) (masterWaitView, bool) {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + statusAddr + "/status?volume=v1")
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
func revokeAndExit(t *testing.T, env *handoffEnv, holder *daemon, e uint64) {
	t.Helper()
	b := env.hosts["s1"]
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
		env := startHandoffEnv(t, 1)
		holder := env.start(t, "bv-del-holder", "s1")
		e := env.adopt(t, holder, "s1")
		revokeAndExit(t, env, holder, e)
		waiting := env.start(t, "bv-del-waiting", "s1")
		waiting.waitLog(t, 30*time.Second, "blockvolume: volume v1 adopted as PRIMARY at r1@")
		out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC,
			"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-while-waiting")
		var del struct{ Completed bool }
		if code != 0 || json.Unmarshal([]byte(out), &del) != nil || !del.Completed {
			t.Fatalf("volume-delete while a process waits: exit %d %s %s; want completed", code, out, errOut)
		}
		err := waiting.wait(30 * time.Second)
		if exitCode(err) != 78 || !waiting.has("blockvolume: exiting status=78 reason=deleted") {
			t.Fatalf("waiting process after the committed delete: %v; want 78 reason=deleted", err)
		}
	})
	t.Run("admitted", func(t *testing.T) {
		env := startHandoffEnv(t, 1)
		holder := env.start(t, "bv-adm-holder", "s1")
		e := env.adopt(t, holder, "s1")
		revokeAndExit(t, env, holder, e)
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
