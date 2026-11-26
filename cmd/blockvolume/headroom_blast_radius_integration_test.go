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
// system (as in TestHeadroomRefusesRestoreTargetsThenServesThem; run
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
		env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "base", "--target", v,
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
	out, errOut, code := env.h.run(t, "ops", "chunk", "snapshot-delete", "--master", env.h.masterAddr, "--volume", "v1", "--name", "base",
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

// TestHeadroomRefusedRestoreTargetSurvivesItsSourceDeleteAndGC: a restore
// target (v2) refused for local headroom has never hydrated, so its HEAD
// names its source's chunks. While it waits, the source's snapshot is
// deleted and then the source volume (seal, revoke, ACK, volume-delete), and
// the collector runs two applying passes a grace apart (its shortest grace;
// about 25 minutes). Then space is freed and v2 starts: it must read back
// the snapshot exactly, at once and again after its restore reaches
// independence. A read that differs or reads zero is a data-loss path and
// fails the test as such; a target that refuses to start fails with its
// reason.
func TestHeadroomRefusedRestoreTargetSurvivesItsSourceDeleteAndGC(t *testing.T) {
	small, filler := os.Getenv("CHUNKSTORE_SMALL_ROOT"), os.Getenv("CHUNKSTORE_SMALL_FILLER")
	if small == "" || filler == "" {
		t.Skip("CHUNKSTORE_SMALL_ROOT and CHUNKSTORE_SMALL_FILLER name a small file system and its filler file")
	}
	need := int64(headroomVolumeBytes + headroomWALBytes)
	if free := freeBytes(t, small); free >= need {
		t.Fatalf("the small file system has %d bytes free, not less than the %d a volume needs: fix the runner", free, need)
	}
	requireBudget(t, 2*ladderGCWait+10*time.Minute)
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(small, t.Name())) })

	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	src := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, src, "s1")
	b2 := env.hosts["n-v2"]
	b2.root = filepath.Join(small, t.Name(), "vol-n-v2")
	env.hosts["n-v2"] = b2
	if err := os.MkdirAll(b2.root, 0o700); err != nil {
		t.Fatal(err)
	}
	img := ladderImage("srcgone", []uint64{0, 3, 9}, 4)
	env.writeImage(t, "s1", img)
	env.snapshot(t, "s1", "v1", "base")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "base", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	env.refusedForHeadroom(t, "v2", "blockvolume-v2-no-room")

	// The source goes: its snapshot, then the volume, by the documented steps.
	env.deleteSnapshot(t, "v1", "base")
	g := env.seal(t, "s1", e)
	env.revoke(t, e, g)
	if err := src.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)
	out, errOut, code := env.volumeDelete(t, "del-v1")
	requireDeleteCompleted(t, out, errOut, code, false)
	deleted := time.Now()

	// Two collector passes a grace apart.
	requireBudget(t, time.Until(deleted.Add(ladderGCWait))+ladderGCWait+5*time.Minute)
	time.Sleep(time.Until(deleted.Add(ladderGCWait)))
	objs1, n1 := env.gc(t, true)
	first := time.Now()
	requireBudget(t, ladderGCWait+5*time.Minute)
	time.Sleep(time.Until(first.Add(ladderGCWait)))
	objs2, n2 := env.gc(t, true)
	for _, o := range append(objs1, objs2...) {
		t.Logf("GC reclaimed %s %s (fid %s)", o.Volume, o.Key, o.FID)
	}
	t.Logf("GC: first pass reclaimed %d, second %d", n1, n2)

	// The refused target, now with room, must read the snapshot exactly.
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	d2 := env.h.start(t, "blockvolume-v2-with-room", env.h.volume, env.volArgsFor(b2)...)
	adopted := false
	for end := time.Now().Add(60 * time.Second); time.Now().Before(end) && !adopted; time.Sleep(200 * time.Millisecond) {
		select {
		case <-d2.done:
			t.Fatalf("v2 refused to start after its source was deleted and collected: %v; %q", d2.err, strings.TrimSpace(d2.line("blockvolume:")))
		default:
		}
		adopted = d2.has("blockvolume: volume v2 adopted as PRIMARY at r1@")
	}
	if !adopted {
		t.Fatal("v2 never adopted after its source was deleted and collected")
	}
	poll(t, 60*time.Second, "v2 never reported its frontend ready", func() bool { return volumeFrontendReady(b2.status, "v2") })
	requireImageExact(t, env, "n-v2", img, "after its source was deleted and collected")
	env.hydrate(t, "v2")
	requireImageExact(t, env, "n-v2", img, "after it was read in full (hydrated)")
}

// requireImageExact reads every block of img on the host and fails, marked
// as a data-loss path, on any block that differs (or reads zero).
func requireImageExact(t *testing.T, env *handoffEnv, hostID string, img map[uint64][]byte, when string) {
	t.Helper()
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	bad := 0
	for lba, want := range img {
		if got := c.read(t, lba, 4096); string(got) != string(want) {
			bad++
		}
	}
	if bad != 0 {
		t.Fatalf("DATA LOSS PATH: %s, %d of %d snapshot blocks on %s differ from what was written", when, bad, len(img), hostID)
	}
	env.requireEdges(t, hostID)
	t.Logf("%s: all %d snapshot blocks on %s read back exactly", when, len(img), hostID)
}
