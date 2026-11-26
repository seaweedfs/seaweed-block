//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Local headroom for restore targets. The durable root of each
// target is on a small file system (CHUNKSTORE_SMALL_ROOT, a directory on it)
// that a filler file (CHUNKSTORE_SMALL_FILLER, on the same file system) keeps
// below the volume size + WAL. testdata/headroom-small-fs.sh prepares both on
// a fresh loop-mounted ext4 for each run and removes it afterwards (root
// only); without them the test is skipped, as in CI.
//
// The check's limit: it compares one volume's size + WAL with the free space
// at that moment, not the sum over every volume on the file system. Once the
// filler is gone, four 256 MiB + 64 MiB targets (1280 MiB together) all start
// on the 768 MiB file system; the test asserts that sum exceeds the file
// system's size.

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

func fsBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	return int64(st.Blocks) * int64(st.Bsize)
}

// requireOnlyReplicaFloor requires the regular files under dir to be exactly
// the volume's replica floor: a refused create writes no store, WAL or
// marker. The floor is written when the assignment arrives, before the store
// is opened and refused (a known limit: the check precedes the store, not
// every sidecar).
func requireOnlyReplicaFloor(t *testing.T, dir, vol string) {
	t.Helper()
	var files []string
	if err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, filepath.Base(p))
		}
		return nil
	}); err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	if len(files) != 1 || files[0] != vol+".replica-floor" {
		t.Fatalf("a refused create left %v under %s; want exactly %s.replica-floor", files, dir, vol)
	}
}

// refusedForHeadroom starts the target's blockvolume and requires it to
// refuse the create the way the durable store's open fails at startup: it
// exits on its own with status 1, its log says "blockvolume: durable storage
// open:" and names the free bytes and the need (size + WAL), and its durable
// root holds no store (only its replica floor). Status 1 is not
// RestartPreventExitStatus=78 in the shipped unit, so systemd starts it
// again every 5 s, refusing each time, until space is freed or its start
// limit stops it. It returns the refused process.
func (env *handoffEnv) refusedForHeadroom(t *testing.T, target, name string) *daemon {
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
	return d
}

// logUint reads the number re captures from the first line of d's log that
// it matches.
func logUint(t *testing.T, d *daemon, re *regexp.Regexp) uint64 {
	t.Helper()
	data, _ := os.ReadFile(d.log)
	m := re.FindSubmatch(data)
	if m == nil {
		t.Fatalf("%s: no line matching %s", d.name, re)
	}
	n, err := strconv.ParseUint(string(m[1]), 10, 64)
	if err != nil || n == 0 {
		t.Fatalf("%s: %q: %v", d.name, m[0], err)
	}
	return n
}

// deleteServingTarget runs the volume-lifecycle page's delete for a target
// that serves (d on host b at epoch e): seal, revoke and wait for its ACK
// and clean exit, then volume-delete, which must complete.
func (env *handoffEnv) deleteServingTarget(t *testing.T, vol string, b bvHost, d *daemon, e uint64) {
	t.Helper()
	out, errOut := env.h.command(t, "ops", "chunk", "detach", "--plain", "--volume", vol, "--status-addr", b.status, "--timeout", "120s")
	var a struct {
		VolumeID  string `json:"volumeID"`
		HeadGen   uint64 `json:"headGen"`
		HeadEpoch uint64 `json:"headEpoch"`
	}
	if err := json.Unmarshal([]byte(out), &a); err != nil || a.VolumeID != vol || a.HeadGen == 0 || a.HeadEpoch != e ||
		strings.TrimSpace(errOut) != fmt.Sprintf("sealed HEAD generation: %d", a.HeadGen) {
		t.Fatalf("plain detach of %s: %v %q %q; want %s sealed at epoch %d with its generation line", vol, err, out, errOut, vol, e)
	}
	g := a.HeadGen
	env.h.command(t, "ops", "workspace", "revoke", "--master", env.h.masterAddr, "--volume", vol, "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--sealed-gen", fmt.Sprint(g))
	if err := d.wait(30 * time.Second); err != nil {
		t.Fatalf("%s after the revocation: %v; want a clean exit by itself", vol, err)
	}
	poll(t, 30*time.Second, fmt.Sprintf("workspace show never reported %s's ACK", vol), func() bool {
		out, _ := env.h.command(t, "ops", "workspace", "show", "--master", env.h.masterAddr, "--volume", vol)
		return showsAck(out, e, g)
	})
	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", vol, "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-"+vol)
	if code != 0 || !handoffDeleteCompleted(out) {
		t.Fatalf("volume-delete of %s after the way out: exit %d %s %s; want completed", vol, code, out, errOut)
	}
}

// TestHeadroomRefusesRestoreTargetsThenServesThem: a snapshot of v1 is
// restored into v2, v4 and v6 (fork is not in v1), each target's durable
// root on a small file system with less free space than the volume size +
// WAL.
// Each target's blockvolume refuses the create, naming both numbers, exits
// with status 1, and writes no store; the source still serves its data and
// its snapshot is still listed, and restores into another target (v5) that
// serves it.
//
// A refused target exists in the catalog and holds the master's line, but
// cannot start, and until it has started once it is stranded (a known limit
// of 1.0): a volume-delete wants a clean revocation ACK its holder cannot
// send, and a forced move to a host with room (n-v7) is refused because the
// master knows no holder of it. The one way out is run: once space is freed,
// v2 starts and serves the snapshot, and v4 and v6 start, serve, and are
// sealed, revoked, acknowledged and deleted.
func TestHeadroomRefusesRestoreTargetsThenServesThem(t *testing.T) {
	small, filler := os.Getenv("CHUNKSTORE_SMALL_ROOT"), os.Getenv("CHUNKSTORE_SMALL_FILLER")
	if small == "" || filler == "" {
		t.Skip("CHUNKSTORE_SMALL_ROOT and CHUNKSTORE_SMALL_FILLER name a small file system and its filler file")
	}
	need := int64(headroomVolumeBytes + headroomWALBytes)
	if free := freeBytes(t, small); free >= need {
		t.Fatalf("the small file system has %d bytes free, not less than the %d a volume needs: fix the runner", free, need)
	}

	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(small, t.Name())) })
	env := startLadderEnv(t, "v2", "v4", "v5", "v6", "v7")
	for _, v := range []string{"v2", "v4", "v6"} {
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

	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "base", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	for _, v := range []string{"v4", "v6"} {
		env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "base", "--target", v,
			"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	}
	refused := map[string]*daemon{}
	for _, v := range []string{"v2", "v4", "v6"} {
		refused[v] = env.refusedForHeadroom(t, v, "blockvolume-"+v+"-no-room")
	}

	// The stranded target: not deletable while its holder cannot start.
	deleteV4 := func() (string, string, int) {
		return env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v4", "--chunk-filer", env.cluster.filerGRPC,
			"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v4-while-refused")
	}
	out, errOut, code := deleteV4()
	if code != 1 || !strings.Contains(out+errOut, "live authority requires clean revocation ACK") {
		t.Fatalf("volume-delete of the refused target v4: exit %d %s %s; want refused for the missing ACK (a known limit)", code, out, errOut)
	}
	// The snapshot is still restorable into another target that has room.
	env.restoreInto(t, "v1", "base", "v5")
	env.requireImage(t, "n-v5", img)

	env.requireImage(t, "s1", img) // the source is intact
	if l := env.snapshots(t, "v1", "base"); len(l) != 1 {
		t.Fatalf("the source's snapshot after the refusals: %+v", l)
	}

	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if free := freeBytes(t, small); free < need {
		t.Fatalf("after removing the filler the small file system has %d bytes free; the %d a volume needs does not fit: fix the runner", free, need)
	}
	for _, v := range []string{"v2"} {
		env.startTarget(t, v, "blockvolume-"+v+"-with-room")
		env.requireImage(t, "n-"+v, img)
	}

	// Way out 1 for a stranded target: with room it starts, and the usual
	// delete steps (seal, revoke, ACK, delete) complete.
	adopted := func(vol string) *regexp.Regexp {
		return regexp.MustCompile(`blockvolume: volume ` + vol + ` adopted as PRIMARY at r1@(\d+) `)
	}
	d4 := env.startTarget(t, "v4", "blockvolume-v4-with-room")
	env.deleteServingTarget(t, "v4", env.hosts["n-v4"], d4, logUint(t, d4, adopted("v4")))

	// A forced move is no way out for a refused target: the master knows no
	// holder of it (it never held a lease), so every dry run is refused and
	// nothing is sent. Its one way out is the first: with room, it starts on
	// its own host and is deleted the usual way.
	e6 := logUint(t, refused["v6"], regexp.MustCompile(`assignment stream delivery .*volume="v6" .*epoch=(\d+)`))
	b7 := env.hosts["n-v7"]
	move := []string{"ops", "workspace", "force-move", "--master", env.h.masterAddr, "--volume", "v6",
		"--target-host", b7.id, "--target-data", b7.data, "--target-ctrl", b7.ctrl, "--request-id", "force-v6",
		"--confirm-forced", fmt.Sprintf("v6@%d", e6), "--dry-run"}
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(3 * time.Second) {
		out, errOut, code := env.h.run(t, move...)
		if code == 0 || previewAdmitted(out) || !strings.Contains(out+errOut, `the master knows no holder of volume "v6"`) || !strings.Contains(out+errOut, "nothing was sent") {
			t.Fatalf("forced move of the refused, never-started v6: exit %d %s %s; want refused, the master knowing no holder, nothing sent", code, out, errOut)
		}
	}
	d6 := env.startTarget(t, "v6", "blockvolume-v6-with-room")
	env.requireImage(t, "n-v6", img)
	env.deleteServingTarget(t, "v6", env.hosts["n-v6"], d6, logUint(t, d6, adopted("v6")))

	// The check is per volume, not a sum: all four targets started on this
	// file system, though together they need more than its whole size.
	if total, size := 4*need, fsBytes(t, small); total <= size {
		t.Fatalf("four targets need %d bytes, not more than the %d-byte file system: the run does not show the per-volume limit", total, size)
	} else {
		t.Logf("four targets needing %d bytes together all started on a %d-byte file system (the check is per volume)", total, size)
	}
}
