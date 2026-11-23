//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Local headroom for a restore and a fork target. The durable root of each
// target is on a small file system (CHUNKSTORE_SMALL_ROOT, a directory on it)
// that a filler file (CHUNKSTORE_SMALL_FILLER, on the same file system) keeps
// below the volume size + WAL. The runner prepares both (a loop-mounted ext4
// needs root); without them the test is skipped.
//
// The check's limit: it compares one volume's size + WAL with the free space
// at that moment, not the sum over every volume on the file system. Two
// 256 MiB + 64 MiB targets both pass on the 512 MiB file system once the
// filler is gone, though together they could outgrow it.

const (
	headroomVolumeBytes = 65536 * 4096 // the harness's volume size
	headroomWALBytes    = 64 << 20     // the default WAL
)

func freeBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

// requireOnlyReplicaFloor fails unless the only regular file under dir is
// the volume's replica floor: a refused create writes no store, WAL or
// marker. The floor is written when the assignment arrives, before the store
// is opened and refused (a known limit: the check precedes the store, not
// every sidecar).
func requireOnlyReplicaFloor(t *testing.T, dir, vol string) {
	t.Helper()
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Base(p) != vol+".replica-floor" {
			t.Fatalf("a refused create left %s (%d bytes)", p, info.Size())
		}
		return nil
	})
}

// refusedForHeadroom starts the target's blockvolume and requires it to
// refuse the create the way the durable store's open fails at startup: it
// exits on its own with status 1, its log says "blockvolume: durable storage
// open:" and names the free bytes and the need (size + WAL), and its durable
// root holds no store (only its replica floor). Status 1 is not
// RestartPreventExitStatus=78 in the shipped unit, so systemd starts it
// again every 5 s, refusing each time, until space is freed or its start
// limit stops it. It returns the refusal line.
func (env *handoffEnv) refusedForHeadroom(t *testing.T, target, name string) string {
	t.Helper()
	b := env.hosts["n-"+target]
	d := env.h.start(t, name, env.h.volume, env.volArgsFor(b)...)
	err := d.wait(60 * time.Second)
	select {
	case <-d.done:
	default:
		t.Fatalf("%s: blockvolume did not exit within 60 s of a create without room (%v)", name, err)
	}
	if err == nil {
		t.Fatalf("%s: blockvolume created %s without room for it and exited cleanly", name, target)
	}
	want := fmt.Sprintf("volume %s needs %d (size %d + WAL %d)", target, headroomVolumeBytes+headroomWALBytes, headroomVolumeBytes, headroomWALBytes)
	line := d.line(want)
	if line == "" || !strings.Contains(line, "bytes free") {
		t.Fatalf("%s: no refusal naming the free bytes and %q", name, want)
	}
	if code := exitCode(err); code != 1 || !strings.Contains(line, "blockvolume: durable storage open:") {
		t.Fatalf("%s: exit status %d, refusal %q; want status 1 from the durable storage open at startup", name, code, line)
	}
	t.Logf("%s refused (exit status 1): %s", name, strings.TrimSpace(line))
	requireOnlyReplicaFloor(t, b.root, target)
	return strings.TrimSpace(line)
}

// TestHeadroomRefusesARestoreAndAForkTargetThenServesThem: a snapshot of v1
// is restored into v2 and v4 and forked into v3, each target's durable root
// on a small file system with less free space than the volume size + WAL.
// Each target's blockvolume refuses the create, naming both numbers, exits
// with status 1, and writes no store; the source still serves its data and
// its snapshot is still listed, and restores into another target (v5) that
// serves it.
//
// A refused target exists in the catalog and holds the master's line, but
// cannot start: today it cannot be volume-deleted either (the delete wants a
// clean revocation ACK its holder cannot send), a known limit of 1.0. The
// documented way out is tested: once space is freed, v2 and v3 start and
// serve the snapshot, and v4 starts, is sealed, revoked and acknowledged, and
// then deleted.
func TestHeadroomRefusesARestoreAndAForkTargetThenServesThem(t *testing.T) {
	small, filler := os.Getenv("CHUNKSTORE_SMALL_ROOT"), os.Getenv("CHUNKSTORE_SMALL_FILLER")
	if small == "" || filler == "" {
		t.Skip("CHUNKSTORE_SMALL_ROOT and CHUNKSTORE_SMALL_FILLER name a small file system and its filler file")
	}
	need := int64(headroomVolumeBytes + headroomWALBytes)
	if free := freeBytes(t, small); free >= need {
		t.Fatalf("the small file system has %d bytes free, not less than the %d a volume needs: fix the runner", free, need)
	}

	env := startLadderEnv(t, "v2", "v3", "v4", "v5")
	for _, v := range []string{"v2", "v3", "v4"} {
		b := env.hosts["n-"+v]
		b.root = filepath.Join(small, t.Name(), "vol-n-"+v)
		env.hosts["n-"+v] = b
		if err := os.MkdirAll(b.root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	img := ladderImage("headroom", []uint64{0, 3}, 4)
	env.writeImage(t, "s1", img)
	env.snapshot(t, "s1", "v1", "base")

	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "base", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	env.h.command(t, "ops", "chunk", "fork", "--source", "v1", "--snapshot", "base", "--target", "v3",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "base", "--target", "v4",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	for _, v := range []string{"v2", "v3", "v4"} {
		env.refusedForHeadroom(t, v, "blockvolume-"+v+"-no-room")
	}

	// The stranded target: not deletable while its holder cannot start.
	deleteV4 := func() (string, string, int) {
		return env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v4", "--chunk-filer", env.cluster.filerGRPC,
			"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v4")
	}
	out, errOut, code := deleteV4()
	if code == 0 || !strings.Contains(out+errOut, "live authority requires clean revocation ACK") {
		t.Fatalf("volume-delete of the refused target v4: exit %d %s %s; want refused for the missing ACK (a known limit)", code, out, errOut)
	}
	// The snapshot is still restorable into another target that has room.
	env.restoreInto(t, "v1", "base", "v5")
	env.requireImage(t, "n-v5", img)

	env.requireImage(t, "s1", img) // the source is intact
	if l := env.snapshots(t, "v1", "base"); len(l) != 1 || l[0].State != "active" {
		t.Fatalf("the source's snapshot after the refusals: %+v", l)
	}

	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if free := freeBytes(t, small); free < need {
		t.Fatalf("after removing the filler the small file system has %d bytes free; the %d a volume needs does not fit: fix the runner", free, need)
	}
	for _, v := range []string{"v2", "v3"} {
		env.startTarget(t, v, "blockvolume-"+v+"-with-room")
		env.requireImage(t, "n-"+v, img)
	}

	// The documented way out for the stranded target: with room it starts,
	// and the usual delete steps (seal, revoke, ACK, delete) complete.
	_, mustU := fatalOn(t)
	d4 := env.startTarget(t, "v4", "blockvolume-v4-with-room")
	e4 := mustU(adoptedEpoch(d4.line("blockvolume: volume v4 adopted as PRIMARY at r1@")))
	b4 := env.hosts["n-v4"]
	out, errOut = env.h.command(t, "ops", "chunk", "detach", "--plain", "--volume", "v4", "--status-addr", b4.status, "--timeout", "120s")
	g4 := mustU(sealedGen(out, errOut, e4))
	env.h.command(t, "ops", "workspace", "revoke", "--master", env.h.masterAddr, "--volume", "v4", "--replica", "r1",
		"--epoch", fmt.Sprint(e4), "--sealed-gen", fmt.Sprint(g4))
	if err := d4.wait(30 * time.Second); err != nil {
		t.Fatalf("v4 after the revocation: %v; want a clean exit by itself", err)
	}
	poll(t, 30*time.Second, "workspace show never reported v4's ACK", func() bool {
		out, _ := env.h.command(t, "ops", "workspace", "show", "--master", env.h.masterAddr, "--volume", "v4")
		return showsAck(out, e4, g4)
	})
	out, errOut, code = deleteV4()
	var del struct{ Completed bool }
	if code != 0 || json.Unmarshal([]byte(out), &del) != nil || !del.Completed {
		t.Fatalf("volume-delete of v4 after the way out: exit %d %s %s; want completed", code, out, errOut)
	}
}
