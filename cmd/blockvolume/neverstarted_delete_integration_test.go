//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// INV-WS-DELETE-NO-LIVE-HOLDER-001, the guard half: a restore target created but never
// attached (no blockvolume ever started for it) is deletable already on 1.0.
// volume-delete completes without a revoke, its HEAD becomes a deleted
// tombstone, and the source volume and its snapshot are untouched. The 1.0.1
// red half is a target whose blockvolume started, was assigned and exited
// before it claimed HEAD (the TestNeverStartedLine... tests below).
func TestNeverAttachedRestoreTargetIsDeletable(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('N', 0))
	env.snapshot(t, "s1", "v1", "s1")

	// The target exists in the catalog; its blockvolume never starts.
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)

	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v2-never-started")
	if code != 0 || !handoffDeleteCompleted(out) {
		t.Fatalf("volume-delete of a never-attached target must complete; exit %d:\n%s\n%s", code, out, errOut)
	}
	shown := env.headOf(t, "v2")
	if !strings.Contains(shown, `"state":"deleted"`) {
		t.Fatalf("the deleted target's HEAD is not a deleted tombstone:\n%s", shown)
	}
	// The source and its snapshot are untouched.
	if got := env.readBlock(t, "s1", 0); string(got) != string(cutCrashBlock('N', 0)) {
		t.Fatalf("the source's block changed")
	}
	if snaps := env.snapshots(t, "v1", ""); len(snaps) != 1 {
		t.Fatalf("the source has %d snapshots after the target's delete; want 1", len(snaps))
	}
}

// 1.0.1 (INV-WS-DELETE-NO-LIVE-HOLDER-001; the fence of NeverStarted -> Deleting): once a
// never-attached target's delete is admitted, a late start of its blockvolume
// must not claim it: the process exits without serving and the catalog still
// says deleted. Recorded on 1.0 to see where it stands.
func TestNeverStartedDeleteFencesALateStart(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('L', 0))
	env.snapshot(t, "s1", "v1", "s1")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v2-late")
	if code != 0 {
		t.Fatalf("volume-delete of a never-attached target: exit %d:\n%s\n%s", code, out, errOut)
	}
	b := env.hosts["n-v2"]
	late := env.h.start(t, "blockvolume-v2-late", env.h.volume, env.volArgsFor(b)...)
	err := late.wait(60 * time.Second)
	served := late.has("durable: HEAD claim for volume=v2 done") || volumeFrontendReady(b.status, "v2")
	shown := env.headOf(t, "v2")
	t.Logf("late start after the delete: exit %v, served %v, HEAD %s", err, served, firstLine(shown))
	if served || err == nil || strings.Contains(fmt.Sprint(err), "still running") || !strings.Contains(shown, `"state":"deleted"`) {
		t.Fatalf("1.0.1: a start after the delete must exit without claiming or serving and leave the volume deleted; exit %v, served %v, HEAD:\n%s", err, served, shown)
	}
}

// 1.0.1 (INV-WS-DELETE-NO-LIVE-HOLDER-001; the race): a start of a never-attached target and its
// volume-delete run at the same time, five times. Exactly one wins each time:
// either the volume is deleted and the process never served, or the process
// claimed and served and the delete is refused as for a live holder.
func TestNeverStartedDeleteRacesAStart(t *testing.T) {
	targets := []string{"v2", "v3", "v4", "v5", "v6"}
	env := startHandoffEnvWith(t, 1, targets)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('R', 0))
	env.snapshot(t, "s1", "v1", "s1")
	for _, v := range targets {
		env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", v,
			"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
		b := env.hosts["n-"+v]
		type delResult struct {
			out  string
			code int
		}
		deleted := make(chan delResult, 1)
		d := env.h.start(t, "blockvolume-"+v+"-race", env.h.volume, env.volArgsFor(b)...)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
			defer cancel()
			out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "volume-delete", "--volume", v, "--chunk-filer", env.cluster.filerGRPC,
				"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-"+v+"-race")
			if err != nil {
				code = -1
			}
			deleted <- delResult{out + errOut, code}
		}()
		r := <-deleted
		_ = d.wait(30 * time.Second)
		// Served means the process claimed HEAD (no I/O is admitted before the
		// claim) or its frontend became ready; adopting the assignment comes
		// before the claim and is not serving.
		served := d.has("durable: HEAD claim for volume="+v+" done") || volumeFrontendReady(b.status, v)
		shown := env.headOf(t, v)
		isDeleted := strings.Contains(shown, `"state":"deleted"`)
		t.Logf("%s: delete exit %d, served %v, HEAD deleted %v: %s", v, r.code, served, isDeleted, firstLine(r.out))
		switch {
		case r.code == 0 && isDeleted && !served:
		case r.code != 0 && served && !isDeleted:
		default:
			t.Errorf("1.0.1: %s: not exactly one winner: delete exit %d, served %v, HEAD deleted %v", v, r.code, served, isDeleted)
		}
		d.kill(t)
	}
}

func (env *handoffEnv) workspaceShow(t *testing.T, v string) string {
	out, errOut, _ := env.h.run(t, "ops", "workspace", "show", "--master", env.h.masterAddr, "--volume", v)
	return out + errOut
}

func (env *handoffEnv) deleteNoRevoke(ctx context.Context, v, request string) (string, int) {
	out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "volume-delete", "--volume", v, "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", request)
	if err != nil {
		code = -1
	}
	return out + errOut, code
}

// deleteAfterTheHorizon is v1's delete of a line HEAD never reached: the
// delete may be refused only while the stranded line's lease can still be
// live ("lease expiry plus clock-skew margin has not elapsed"); nobody
// renews it, so the same delete (same request id, which resumes it) run
// past the horizon it names completes. Any other refusal is returned as it
// is. The immediate delete of a never-started line, without waiting, is the
// 1.0.1 fix.
func (env *handoffEnv) deleteAfterTheHorizon(t *testing.T, v, request string) (string, int) {
	t.Helper()
	out, code := env.deleteNoRevoke(context.Background(), v, request)
	if code == 0 || !strings.Contains(out, "has not elapsed") {
		return out, code
	}
	wait := 75 * time.Second
	if i := strings.Index(out, "live until "); i >= 0 {
		if f := strings.Fields(out[i+len("live until "):]); len(f) > 0 {
			if until, err := time.Parse(time.RFC3339Nano, strings.TrimRight(f[0], ":")); err == nil {
				wait = time.Until(until) + 5*time.Second
			}
		}
	}
	if wait < 0 || wait > 90*time.Second {
		t.Fatalf("the refusal names a horizon %v away (want within 90 s):\n%s", wait, out)
	}
	t.Logf("%s: refused until its lease horizon; retrying in %v:\n%s", v, wait.Round(time.Second), firstLine(out))
	time.Sleep(wait)
	return env.deleteNoRevoke(context.Background(), v, request)
}

// lineOf returns the master's durable authority record for v (the line:
// replica, epoch, and any workspace lease), as stored in --authority-store.
func (env *handoffEnv) lineOf(t *testing.T, v string) string {
	t.Helper()
	dir := ""
	for i, a := range env.masterArgs {
		if a == "--authority-store" && i+1 < len(env.masterArgs) {
			dir = env.masterArgs[i+1]
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, v+".v3auth.json"))
	if os.IsNotExist(err) {
		return "(no line)"
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(strings.Fields(string(data)), " ")
}

// requireNeverStarted checks the state by its facts: the master's line for
// v is at epoch e (with or without a lease, as lease says) and HEAD is below
// it (epoch headEpoch).
func (env *handoffEnv) requireNeverStarted(t *testing.T, v string, e, headEpoch uint64, lease bool) {
	t.Helper()
	line, head := env.lineOf(t, v), env.headOf(t, v)
	ok := strings.Contains(line, fmt.Sprintf(`"Epoch":%d,`, e)) && strings.Contains(head, fmt.Sprintf(`"epoch":%d,`, headEpoch)) &&
		strings.Contains(line, `"Lease":`) == lease
	if !ok {
		t.Fatalf("setup: %s is not a line at epoch %d (lease %v) over HEAD at epoch %d:\nline: %s\nHEAD: %s", v, e, lease, headEpoch, line, head)
	}
	t.Logf("%s: line %s\nHEAD %s", v, line, head)
}

// strandStoreDown (S1): v's first start runs with the store (filer) down.
// The process heartbeats and the master writes the line at epoch 1; local
// readiness blocks before the assignment is applied, no lease is granted,
// and the process exits. The filer comes back. Line at 1, no lease, HEAD 0.
func (env *handoffEnv) strandStoreDown(t *testing.T, v string) {
	t.Helper()
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", v,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	env.cluster.stopProc(t, "filer")
	d := env.h.start(t, "blockvolume-"+v+"-store-down", env.h.volume, env.volArgsFor(env.hosts["n-"+v])...)
	err := d.wait(120 * time.Second)
	env.cluster.restartProc(t, "filer")
	w4WaitTCP(t, env.cluster.filerGRPC, 30*time.Second)
	if err == nil || d.has("adopted as PRIMARY") {
		t.Fatalf("setup: %s's start with the store down exited %v, adopted %v", v, err, d.has("adopted as PRIMARY"))
	}
	env.requireNeverStarted(t, v, 1, 0, false)
}

// strandAtGrant (S2): v's first start with the right flags is stopped
// (SIGSTOP) the moment the master shows its lease granted, after the
// adoption and before the HEAD claim, then killed. Line at 1 with a granted
// lease, HEAD 0. When the claim wins the race the next candidate is tried;
// it returns the stranded volume.
func (env *handoffEnv) strandAtGrant(t *testing.T, candidates ...string) string {
	t.Helper()
	for _, v := range candidates {
		env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", v,
			"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
		d := env.h.start(t, "blockvolume-"+v+"-stopped-at-grant", env.h.volume, env.volArgsFor(env.hosts["n-"+v])...)
		deadline := time.Now().Add(60 * time.Second)
		for !d.has("adopted as PRIMARY") && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		for time.Now().Before(deadline) {
			out, _, _, _ := runCLI(context.Background(), env.h, "ops", "workspace", "show", "--master", env.h.masterAddr, "--volume", v)
			if strings.Contains(out, "lease state: granted") {
				break
			}
		}
		if err := d.proc.Signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("setup: stop %s: %v", v, err)
		}
		claimed := !strings.Contains(env.headOf(t, v), `"epoch":0,"holder":""`)
		_ = d.proc.Signal(syscall.SIGKILL)
		_ = d.wait(10 * time.Second)
		if claimed {
			t.Logf("%s: the claim won the race; trying the next candidate", v)
			continue
		}
		env.requireNeverStarted(t, v, 1, 0, true)
		return v
	}
	t.Fatalf("setup: the claim won the race for every candidate %v", candidates)
	return ""
}

// INV-WS-DELETE-NO-LIVE-HOLDER-001 (on v1 past the stranded line's lease
// horizon: deleteAfterTheHorizon; at once is 1.0.1): a line whose epoch HEAD never
// reached has acknowledged nothing, so volume-delete completes WITHOUT a
// revoke; HEAD becomes a deleted tombstone, the source is untouched, and no
// granted lease stays on the deleted volume. Routes, each without root:
//   - S1: a first start with the store down (line, no lease);
//   - S2: a first start stopped at the lease grant (line and lease), deleted
//     at once and again after the lease lapsed (65 s);
//   - clean move: the volume moved clean to a target that never starts (a
//     line and a lease at the new epoch, HEAD at the old one, detached).
//
// On 1.0 every delete is refused as volume_in_use (live authority needs a
// revocation ACK that no process can give).
func TestNeverStartedLineIsDeletableWithoutARevoke(t *testing.T) {
	deleted := func(t *testing.T, env *handoffEnv, v, request string) {
		t.Helper()
		before := env.workspaceShow(t, v)
		out, code := env.deleteAfterTheHorizon(t, v, request)
		completed := code == 0 && handoffDeleteCompleted(out)
		head, after := env.headOf(t, v), env.workspaceShow(t, v)
		t.Logf("delete exit %d, completed %v, HEAD %s\nworkspace show before:\n%s\nafter:\n%s\ndelete output:\n%s", code, completed, head, before, after, out)
		if !completed {
			t.Fatalf("volume-delete of %s (line above HEAD) must complete without a revoke, past the lease horizon; exit %d", v, code)
		}
		if !strings.Contains(head, `"state":"deleted"`) || strings.Contains(after, "lease state: granted") {
			t.Fatalf("a completed delete must leave a deleted HEAD and no granted lease")
		}
	}
	t.Run("S1-store-down", func(t *testing.T) {
		env := startSourceWithSnapshot(t, "v2")
		env.strandStoreDown(t, "v2")
		deleted(t, env, "v2", "del-v2-s1")
		requireSourceUntouched(t, env)
	})
	t.Run("S2-stopped-at-grant", func(t *testing.T) {
		env := startSourceWithSnapshot(t, "v3", "v3b", "v3c")
		v := env.strandAtGrant(t, "v3", "v3b", "v3c")
		deleted(t, env, v, "del-"+v+"-s2")
		requireSourceUntouched(t, env)
	})
	t.Run("S2-lease-lapsed", func(t *testing.T) {
		env := startSourceWithSnapshot(t, "v4", "v4b", "v4c")
		v := env.strandAtGrant(t, "v4", "v4b", "v4c")
		time.Sleep(65 * time.Second)
		deleted(t, env, v, "del-"+v+"-s2-lapsed")
	})
	t.Run("clean-move-target-never-started", func(t *testing.T) {
		env := startHandoffEnv(t, 2)
		must, mustU := fatalOn(t)
		old := env.start(t, "blockvolume-s1", "s1")
		e := env.adopt(t, old, "s1")
		env.writeBlock(t, "s1", 0, pattern("never-start-move"))
		g := env.seal(t, "s1", e)
		l := env.sealedLSN(t, e, g)
		env.revoke(t, e, g)
		must(old.wait(30 * time.Second))
		env.waitAck(t, e, g)
		out, _ := env.h.command(t, env.moveClean(e, "s2", "move-v1-1")...)
		e2 := mustU(movedEpochTo(out, e, g, l, "s2"))
		env.requireNeverStarted(t, "v1", e2, e, true)
		deleted(t, env, "v1", "del-v1-moved")
	})
}

func requireSourceUntouched(t *testing.T, env *handoffEnv) {
	t.Helper()
	if got := env.readBlock(t, "s1", 0); string(got) != string(cutCrashBlock('A', 0)) {
		t.Fatalf("the source's block changed")
	}
	if snaps := env.snapshots(t, "v1", ""); len(snaps) != 1 {
		t.Fatalf("the source has %d snapshots after the target's delete; want 1", len(snaps))
	}
}

// After that delete (on v1 past the lease horizon), a start of the target with the store up
// must not claim it: it exits without serving and the HEAD stays deleted.
// S1 and S2.
func TestNeverStartedLineDeleteFencesALateStart(t *testing.T) {
	for _, c := range []struct {
		name    string
		targets []string
	}{
		{"S1-store-down", []string{"v2"}},
		{"S2-stopped-at-grant", []string{"v3", "v3b", "v3c"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			env := startSourceWithSnapshot(t, c.targets...)
			v := c.targets[0]
			if c.name == "S1-store-down" {
				env.strandStoreDown(t, v)
			} else {
				v = env.strandAtGrant(t, c.targets...)
			}
			if out, code := env.deleteAfterTheHorizon(t, v, "del-"+v+"-late"); code != 0 {
				t.Fatalf("volume-delete of %s (line above HEAD), past the lease horizon: exit %d:\n%s", v, code, out)
			}
			b := env.hosts["n-"+v]
			late := env.h.start(t, "blockvolume-"+v+"-late", env.h.volume, env.volArgsFor(b)...)
			err := late.wait(90 * time.Second)
			served := volumeFrontendReady(b.status, v)
			head := env.headOf(t, v)
			if served || err == nil || strings.Contains(fmt.Sprint(err), "still running") || !strings.Contains(head, `"state":"deleted"`) {
				t.Fatalf("1.0.1: a start after the delete must exit without claiming or serving; exit %v, served %v, HEAD %s", err, served, head)
			}
		})
	}
}

// 1.0.1 red: a start (store up) of an S1 target and its volume-delete race,
// with the start delayed by 0 s to 3 s. Each time exactly one wins: the
// volume is deleted and never served, or it served and the delete was
// refused. With a 3 s lead the delete must win.
func TestNeverStartedLineDeleteRacesAStart(t *testing.T) {
	targets := []string{"v2", "v3", "v4", "v5", "v6"}
	delays := []time.Duration{0, 0, 200 * time.Millisecond, time.Second, 3 * time.Second}
	env := startSourceWithSnapshot(t, targets...)
	for i, v := range targets {
		env.strandStoreDown(t, v)
		b := env.hosts["n-"+v]
		type delResult struct {
			out  string
			code int
		}
		deleted := make(chan delResult, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
			defer cancel()
			out, code := env.deleteNoRevoke(ctx, v, "del-"+v+"-race")
			deleted <- delResult{out, code}
		}()
		time.Sleep(delays[i])
		d := env.h.start(t, "blockvolume-"+v+"-race", env.h.volume, env.volArgsFor(b)...)
		r := <-deleted
		_ = d.wait(60 * time.Second)
		served := volumeFrontendReady(b.status, v)
		isDeleted := strings.Contains(env.headOf(t, v), `"state":"deleted"`)
		t.Logf("%s (start +%v): delete exit %d, served %v, HEAD deleted %v: %s", v, delays[i], r.code, served, isDeleted, firstLine(r.out))
		switch {
		case r.code == 0 && isDeleted && !served:
		case r.code != 0 && served && !isDeleted && delays[i] < 3*time.Second:
		default:
			t.Errorf("1.0.1: %s: want exactly one winner (the delete with a 3 s lead): delete exit %d, served %v, HEAD deleted %v", v, r.code, served, isDeleted)
		}
		d.kill(t)
	}
}

// Master restart, one direction (on v1 past the lease horizon): the master is SIGKILLed and
// restarted between the stranding and the delete; the delete still completes
// without a revoke, from what the master and the HEAD keep durably. S1 and S2.
func TestNeverStartedDeleteAfterAMasterRestart(t *testing.T) {
	for _, s2 := range []bool{false, true} {
		name := map[bool]string{false: "S1-store-down", true: "S2-stopped-at-grant"}[s2]
		t.Run(name, func(t *testing.T) {
			env := startSourceWithSnapshot(t, "v2", "v2b", "v2c")
			v := "v2"
			if s2 {
				v = env.strandAtGrant(t, "v2", "v2b", "v2c")
			} else {
				env.strandStoreDown(t, v)
			}
			restartKilledMaster(t, env)
			out, code := env.deleteAfterTheHorizon(t, v, "del-"+v+"-after-restart")
			if head := env.headOf(t, v); code != 0 || !strings.Contains(head, `"state":"deleted"`) {
				t.Fatalf("after a master restart, volume-delete of %s must complete (past the lease horizon); exit %d, HEAD %s:\n%s", v, code, head, out)
			}
		})
	}
}

// Guard, green on 1.0 (master restart, the other direction): a target whose
// holder DID claim its HEAD, was SIGKILLed, and whose lease the restarted
// master no longer holds in memory, is not never-started: volume-delete
// without a revoke and an ACK is refused and the HEAD is not tombstoned.
func TestClaimedTargetAfterAMasterRestartIsNotNeverStarted(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	d := env.startTarget(t, "v2", "blockvolume-v2")
	if head := env.headOf(t, "v2"); strings.Contains(head, `"holder":""`) {
		t.Fatalf("setup: v2 serves but its HEAD is not claimed: %s", head)
	}
	d.kill(t)
	restartKilledMaster(t, env)
	out, code := env.deleteNoRevoke(context.Background(), "v2", "del-v2-claimed")
	if head := env.headOf(t, "v2"); code == 0 || strings.Contains(head, `"state":"deleted"`) {
		t.Fatalf("a claimed target was deleted without a revoke after a master restart; exit %d, HEAD %s:\n%s", code, head, out)
	}
}
