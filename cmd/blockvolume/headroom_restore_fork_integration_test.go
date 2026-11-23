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

// requireNoFiles fails if any regular file exists under dir: a refused
// create leaves nothing behind.
func requireNoFiles(t *testing.T, dir string) {
	t.Helper()
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			t.Fatalf("a refused create left %s (%d bytes)", p, info.Size())
		}
		return nil
	})
}

// refusedForHeadroom starts the target's blockvolume and requires it to
// refuse the create: it exits on its own with its exit line (status and
// reason), its log names the free bytes and the need (size + WAL), and its
// durable root holds no file. It returns the exit line.
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
	exit := d.line("blockvolume: exiting status=")
	if exit == "" {
		t.Fatalf("%s: no 'blockvolume: exiting status=N reason=...' line", name)
	}
	t.Logf("%s refused: %s; %s", name, strings.TrimSpace(line), strings.TrimSpace(exit))
	requireNoFiles(t, b.root)
	return strings.TrimSpace(exit)
}

// TestHeadroomRefusesARestoreAndAForkTargetThenServesThem: a snapshot of v1
// is restored into v2 and v4 and forked into v3, each target's durable root
// on a small file system with less free space than the volume size + WAL.
// Each target's blockvolume refuses the create, naming both numbers, exits
// with the same exit line, and leaves no file; the source still serves its
// data and its snapshot is still listed. The operator's two ways out:
//   - give up the stranded target: volume-delete of v4 (which exists in the
//     catalog but cannot start) completes, and the source's snapshot is
//     then restored into another target, v5, which serves it;
//   - free space: once the filler is removed, v2 and v3 start, serve, and
//     read back the snapshot.
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
	exits := map[string]string{}
	for _, v := range []string{"v2", "v3", "v4"} {
		line := env.refusedForHeadroom(t, v, "blockvolume-"+v+"-no-room")
		exits[line[strings.Index(line, "status="):]] = v
	}
	if len(exits) != 1 {
		t.Fatalf("the refusals exited differently: %v", exits)
	}

	// The stranded target given up: deleted, and the snapshot restored elsewhere.
	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v4", "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v4")
	var del struct{ Completed bool }
	if code != 0 || json.Unmarshal([]byte(out), &del) != nil || !del.Completed {
		t.Fatalf("volume-delete of the refused target v4: exit %d %s %s; want completed", code, out, errOut)
	}
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
}
