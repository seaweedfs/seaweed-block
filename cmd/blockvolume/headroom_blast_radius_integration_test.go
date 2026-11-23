//go:build subprocess && chunkstore_integration

package main_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestHeadroomRefusedTargetBlastRadiusAndAnotherRoot: two restore targets of
// a snapshot of v1 (v2, v3) are refused for local headroom on the small file
// system (as in TestHeadroomRefusesARestoreAndAForkTargetThenServesThem; run
// it with testdata/headroom-small-fs.sh). Then:
//
//   - another root on the same host is a way out: v3's blockvolume, started
//     again on the same host (same server and endpoints) with --durable-root
//     on a file system with room and nothing freed, starts and serves the
//     snapshot;
//   - no blast radius: while v2 stays refused (in the catalog, never
//     started), deleting the source's snapshot is allowed (deleting), and the
//     source volume is then deleted through the volume-lifecycle page's steps
//     (seal, revoke, ACK, volume-delete), which complete. Each step answers
//     within its bound; nothing is blocked by the refused target.
func TestHeadroomRefusedTargetBlastRadiusAndAnotherRoot(t *testing.T) {
	small, filler := os.Getenv("CHUNKSTORE_SMALL_ROOT"), os.Getenv("CHUNKSTORE_SMALL_FILLER")
	if small == "" || filler == "" {
		t.Skip("CHUNKSTORE_SMALL_ROOT and CHUNKSTORE_SMALL_FILLER name a small file system and its filler file")
	}
	if free, need := freeBytes(t, small), int64(headroomVolumeBytes+headroomWALBytes); free >= need {
		t.Fatalf("the small file system has %d bytes free, not less than the %d a volume needs: fix the runner", free, need)
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(small, t.Name())) })

	env := startHandoffEnvWith(t, 1, []string{"v2", "v3"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	src := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, src, "s1")
	for _, v := range []string{"v2", "v3"} {
		b := env.hosts["n-"+v]
		b.root = filepath.Join(small, t.Name(), "vol-n-"+v)
		env.hosts["n-"+v] = b
		if err := os.MkdirAll(b.root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	img := ladderImage("blastrad", []uint64{0, 3}, 4)
	env.writeImage(t, "s1", img)
	env.snapshot(t, "s1", "v1", "base")
	for _, v := range []string{"v2", "v3"} {
		env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "base", "--target", v,
			"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
		env.refusedForHeadroom(t, v, "blockvolume-"+v+"-no-room")
	}

	// Another root with room on the same host, nothing freed.
	b3 := env.hosts["n-v3"]
	b3.root = filepath.Join(env.h.dir, "vol-n-v3-with-room")
	env.hosts["n-v3"] = b3
	d3 := env.h.start(t, "blockvolume-v3-other-root", env.h.volume, env.volArgsFor(b3)...)
	d3.waitLog(t, 60*time.Second, "blockvolume: volume v3 adopted as PRIMARY at r1@")
	poll(t, 60*time.Second, "v3 on another root never reported its frontend ready", func() bool {
		return volumeFrontendReady(b3.status, "v3")
	})
	env.requireImage(t, "n-v3", img)

	// Blast radius, v2 still refused: delete the source's snapshot, then the source volume.
	out, errOut, code := env.h.run(t, "ops", "chunk", "snapshot-delete", "--volume", "v1", "--name", "base",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	if code != 0 || !strings.Contains(out, `"state":"deleting"`) {
		t.Fatalf("snapshot-delete of the source snapshot while a restore target is refused: exit %d %s %s; want deleting", code, out, errOut)
	}
	g := env.seal(t, "s1", e)
	env.revoke(t, e, g)
	if err := src.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)
	out, errOut, code = env.volumeDelete(t, "del-v1")
	requireDeleteCompleted(t, out, errOut, code, false)
}
