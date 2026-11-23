//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// V1-SPEC r2.65 item 4, the guard half: a restore target created but never
// attached (no blockvolume ever started for it) is deletable already on 1.0.
// volume-delete completes without a revoke, its HEAD becomes a deleted
// tombstone, and the source volume and its snapshot are untouched. The 1.0.1
// red half is a target whose blockvolume started and was refused before it
// served (the master assigned it); that case needs a small file system.
func TestNeverAttachedRestoreTargetIsDeletable(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('N', 0))
	env.snapshot(t, "s1", "v1", "s1")

	// The target exists in the catalog; its blockvolume never starts.
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)

	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v2-never-started")
	var del struct{ Completed bool }
	if code != 0 || json.Unmarshal([]byte(out), &del) != nil || !del.Completed {
		t.Fatalf("volume-delete of a never-attached target must complete; exit %d:\n%s\n%s", code, out, errOut)
	}
	shown, _, _ := env.h.run(t, "ops", "chunk", "show", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
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

// 1.0.1 (V1-SPEC r2.65 item 4; the fence of NeverStarted -> Deleted): once a
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
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v2-late")
	if code != 0 {
		t.Fatalf("volume-delete of a never-attached target: exit %d:\n%s\n%s", code, out, errOut)
	}
	b := env.hosts["n-v2"]
	late := env.h.start(t, "blockvolume-v2-late", env.h.volume, env.volArgsFor(b)...)
	err := late.wait(60 * time.Second)
	served := late.has("adopted as PRIMARY") || volumeFrontendReady(b.status, "v2")
	shown, _, _ := env.h.run(t, "ops", "chunk", "show", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	t.Logf("late start after the delete: exit %v, served %v, HEAD %s", err, served, firstLine(shown))
	if served || err == nil || strings.Contains(fmt.Sprint(err), "still running") || !strings.Contains(shown, `"state":"deleted"`) {
		t.Fatalf("1.0.1: a start after the delete must exit without claiming or serving and leave the volume deleted; exit %v, served %v, HEAD:\n%s", err, served, shown)
	}
}

// 1.0.1 (r2.65 item 4; the race): a start of a never-attached target and its
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
		env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", v,
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
		served := d.has("adopted as PRIMARY") || volumeFrontendReady(b.status, v)
		shown, _, _ := env.h.run(t, "ops", "chunk", "show", "--volume", v, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
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

// strandTarget makes the assigned-never-claimed state of 1.0.1 (r2.65 item
// 4) without a small file system: the restore target's blockvolume starts
// with half the cluster spec's --durable-blocks (a wrong flag), the master
// assigns it and grants a lease, recovery never becomes promotion-ready and
// the process exits 75; its HEAD was never claimed (epoch 0, no holder).
func (env *handoffEnv) strandTarget(t *testing.T, v string) {
	t.Helper()
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", v,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	args := append([]string{}, env.volArgsFor(env.hosts["n-"+v])...)
	for i := range args {
		if args[i] == "--durable-blocks" {
			args[i+1] = "32768"
		}
	}
	d := env.h.start(t, "blockvolume-"+v+"-wrong-geometry", env.h.volume, args...)
	err := d.wait(90 * time.Second)
	head := env.headOf(t, v)
	if exitCode(err) != 75 || !d.has("adopted as PRIMARY") || !strings.Contains(head, `"epoch":0`) || !strings.Contains(head, `"holder":""`) {
		t.Fatalf("setup: %s is not assigned-never-claimed: exit %v, adopted %v, HEAD %s", v, err, d.has("adopted as PRIMARY"), head)
	}
}

func (env *handoffEnv) headOf(t *testing.T, v string) string {
	out, _, _ := env.h.run(t, "ops", "chunk", "show", "--volume", v, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	return firstLine(out)
}

func (env *handoffEnv) deleteNoRevoke(ctx context.Context, v, request string) (string, int) {
	out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "volume-delete", "--volume", v, "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", request)
	if err != nil {
		code = -1
	}
	return out + errOut, code
}

func startSourceWithSnapshot(t *testing.T, targets ...string) *handoffEnv {
	env := startHandoffEnvWith(t, 1, targets)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('A', 0))
	env.snapshot(t, "s1", "v1", "s1")
	return env
}

// 1.0.1 red (r2.65 item 4): a target that was assigned and never claimed
// holds nothing to seal or ACK, so volume-delete completes WITHOUT a revoke;
// its HEAD becomes a deleted tombstone and the source is untouched. On 1.0
// the delete is refused as volume_in_use (live authority needs a revocation
// ACK that no process can give).
func TestAssignedNeverClaimedTargetIsDeletableWithoutARevoke(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.strandTarget(t, "v2")
	out, code := env.deleteNoRevoke(context.Background(), "v2", "del-v2-assigned")
	var del struct{ Completed bool }
	if code != 0 || json.Unmarshal([]byte(firstLine(out)), &del) != nil || !del.Completed {
		t.Fatalf("1.0.1: volume-delete of an assigned-never-claimed target must complete without a revoke; exit %d:\n%s", code, out)
	}
	if head := env.headOf(t, "v2"); !strings.Contains(head, `"state":"deleted"`) {
		t.Fatalf("the deleted target's HEAD is not a deleted tombstone: %s", head)
	}
	if got := env.readBlock(t, "s1", 0); string(got) != string(cutCrashBlock('A', 0)) {
		t.Fatalf("the source's block changed")
	}
	if snaps := env.snapshots(t, "v1", ""); len(snaps) != 1 {
		t.Fatalf("the source has %d snapshots after the target's delete; want 1", len(snaps))
	}
}

// 1.0.1 red: after that delete, a start of the target with the right flags
// must not claim it: it exits without serving and the HEAD stays deleted.
func TestAssignedNeverClaimedDeleteFencesALateStart(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.strandTarget(t, "v2")
	if out, code := env.deleteNoRevoke(context.Background(), "v2", "del-v2-assigned-late"); code != 0 {
		t.Fatalf("1.0.1: volume-delete of an assigned-never-claimed target: exit %d:\n%s", code, out)
	}
	b := env.hosts["n-v2"]
	late := env.h.start(t, "blockvolume-v2-late", env.h.volume, env.volArgsFor(b)...)
	err := late.wait(90 * time.Second)
	served := volumeFrontendReady(b.status, "v2")
	head := env.headOf(t, "v2")
	if served || err == nil || strings.Contains(fmt.Sprint(err), "still running") || !strings.Contains(head, `"state":"deleted"`) {
		t.Fatalf("1.0.1: a start after the delete must exit without claiming or serving; exit %v, served %v, HEAD %s", err, served, head)
	}
}

// 1.0.1 red: the right-flag start of an assigned-never-claimed target and
// its volume-delete race, with the start delayed by 0 s to 3 s. Each time
// exactly one wins: the volume is deleted and never served, or it served
// and the delete was refused. With a 3 s lead the delete must win.
func TestAssignedNeverClaimedDeleteRacesAStart(t *testing.T) {
	targets := []string{"v2", "v3", "v4", "v5", "v6"}
	delays := []time.Duration{0, 0, 200 * time.Millisecond, time.Second, 3 * time.Second}
	env := startSourceWithSnapshot(t, targets...)
	for i, v := range targets {
		env.strandTarget(t, v)
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

// 1.0.1 red (master restart, one direction): the master is SIGKILLed and
// restarted between the stranding and the delete; the delete still completes
// without a revoke from what the master and the HEAD keep durably.
func TestAssignedNeverClaimedDeleteAfterAMasterRestart(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.strandTarget(t, "v2")
	restartKilledMaster(t, env)
	out, code := env.deleteNoRevoke(context.Background(), "v2", "del-v2-after-restart")
	if head := env.headOf(t, "v2"); code != 0 || !strings.Contains(head, `"state":"deleted"`) {
		t.Fatalf("1.0.1: after a master restart, volume-delete of an assigned-never-claimed target must complete; exit %d, HEAD %s:\n%s", code, head, out)
	}
}

// Guard, green on 1.0 (master restart, the other direction): a target whose
// holder DID claim its HEAD, was SIGKILLed, and whose lease the restarted
// master no longer holds in memory, is not never-started: volume-delete
// without a revoke and an ACK is refused and the HEAD is not tombstoned.
func TestClaimedTargetAfterAMasterRestartIsNotNeverStarted(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v2",
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
