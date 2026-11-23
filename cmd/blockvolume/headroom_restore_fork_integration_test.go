//go:build subprocess && chunkstore_integration

package main_test

import (
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
// refuse the create: it exits on its own, its log names the free bytes and
// the need (size + WAL), and its durable root holds no file.
func (env *handoffEnv) refusedForHeadroom(t *testing.T, target, name string) {
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
	t.Logf("%s refused: %s", name, strings.TrimSpace(line))
	requireNoFiles(t, b.root)
}

// TestHeadroomRefusesARestoreAndAForkTargetThenServesThem: a snapshot of v1
// is restored into v2 and forked into v3, each target's durable root on a
// small file system with less free space than the volume size + WAL. Each
// target's blockvolume refuses the create, naming both numbers, and leaves no
// file; the source still serves its data and its snapshot is still listed.
// Once space is freed, the same targets start, serve, and read back the
// snapshot.
func TestHeadroomRefusesARestoreAndAForkTargetThenServesThem(t *testing.T) {
	small, filler := os.Getenv("CHUNKSTORE_SMALL_ROOT"), os.Getenv("CHUNKSTORE_SMALL_FILLER")
	if small == "" || filler == "" {
		t.Skip("CHUNKSTORE_SMALL_ROOT and CHUNKSTORE_SMALL_FILLER name a small file system and its filler file")
	}
	need := int64(headroomVolumeBytes + headroomWALBytes)
	if free := freeBytes(t, small); free >= need {
		t.Fatalf("the small file system has %d bytes free, not less than the %d a volume needs: fix the runner", free, need)
	}

	env := startLadderEnv(t, "v2", "v3")
	for _, v := range []string{"v2", "v3"} {
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
	env.refusedForHeadroom(t, "v2", "blockvolume-v2-no-room")
	env.refusedForHeadroom(t, "v3", "blockvolume-v3-no-room")

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
