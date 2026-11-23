//go:build subprocess && chunkstore_integration

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
