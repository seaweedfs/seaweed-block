//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// Rung D of the snapshot ladder: deletes. D1-D3 are the logical delete: a
// deleted snapshot leaves every other snapshot restorable, whichever one is
// deleted (the newest, a middle one, one whose content recurs later). No
// collector runs in them (a delete only marks the snapshot and frees its
// name), so they prove nothing about reclamation. D4 runs the collector and
// measures what it reclaims.
//
// deleteSnapshot deletes vol's snapshot name and checks the answer names it.
func (env *handoffEnv) deleteSnapshot(t *testing.T, vol, name string) {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "snapshot-delete", "--master", env.h.masterAddr, "--volume", vol, "--name", name,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	var body struct {
		VolumeID string `json:"volumeID"`
		Name     string `json:"name"`
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil || body.Name != name {
		t.Fatalf("delete %s of %s: %v\n%s", name, vol, err, out)
	}
}

// holderDeleteSnapshot deletes vol's snapshot name through its holder's
// control route (POST /chunk/snapshot/delete on hostID's status listener),
// which runs the holder's cleaning pass right after the delete. It returns
// the HTTP status and body.
//
// A snapshot delete is one conditional update of HEAD. The holder's
// cleaning (kicked by the previous delete) also updates HEAD, so a delete
// that read HEAD just before can lose its update ("HEAD generation moved")
// and change nothing; ops chunk snapshot-delete reports that as a retryable
// conflict. Only that answer is retried, within 30 s; any other is returned.
func (env *handoffEnv) holderDeleteSnapshot(t *testing.T, hostID, vol, name string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"volumeID": vol, "name": name})
	c := &http.Client{Timeout: 2 * time.Minute}
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		resp, err := c.Post("http://"+env.hosts[hostID].status+"/chunk/snapshot/delete", "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK || !strings.Contains(string(out), "HEAD generation moved") || time.Now().After(end) {
			return resp.StatusCode, string(out)
		}
		t.Logf("holder delete of %s lost its HEAD update to a concurrent change; retrying: %s", name, out)
	}
}

// poolObjects counts the chunk objects in vol's pool (every shard
// directory): what the volume stores, whatever names it.
func (env *handoffEnv) poolObjects(t *testing.T, vol string) int {
	t.Helper()
	id, err := layout.ParseVolumeID(env.layoutID(vol))
	if err != nil {
		t.Fatal(err)
	}
	pool := path.Join(env.chunkPath, layout.VolumeDir(vol, id), layout.PoolDir)
	n := 0
	for shard := range env.filerNames(t, pool) {
		n += len(env.filerNames(t, path.Join(pool, shard)))
	}
	return n
}

// requireSnapshotNames checks vol's snapshot list is exactly names, in order.
func (env *handoffEnv) requireSnapshotNames(t *testing.T, vol string, names ...string) {
	t.Helper()
	list := env.snapshots(t, vol, "")
	ok := len(list) == len(names)
	for i := 0; ok && i < len(list); i++ {
		ok = list[i].Name == names[i]
	}
	if !ok {
		t.Fatalf("snapshots of %s: %+v; want exactly %v", vol, list, names)
	}
}

// restoreChecked restores snap of v1 into target until its status says
// independent and checks it equals img, with every block of later that img
// does not hold reading zero.
func (env *handoffEnv) restoreChecked(t *testing.T, snap, target string, img map[uint64][]byte, later ...map[uint64][]byte) {
	t.Helper()
	env.restoreInto(t, "v1", snap, target)
	env.hydrate(t, target)
	env.requireImage(t, "n-"+target, img, later...)
}

// TestSnapshotLadderD1DeleteTheNewest: three snapshots over overlapping
// chunks; the newest is deleted; the other two restore as they were.
func TestSnapshotLadderD1DeleteTheNewest(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	img1 := ladderImage("d1one", []uint64{0, 4}, 4)
	img2 := ladderImage("d1two", []uint64{4, 8}, 3)
	img3 := ladderImage("d1three", []uint64{0, 8}, 2)
	for i, img := range []map[uint64][]byte{img1, img2, img3} {
		env.writeImage(t, "s1", img)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	env.deleteSnapshot(t, "v1", "s3")
	env.requireSnapshotNames(t, "v1", "s1", "s2")
	env.restoreChecked(t, "s1", "v2", img1, img2, img3)
	env.restoreChecked(t, "s2", "v3", mergeImages(img1, img2), img3)
}

// TestSnapshotLadderD2DeleteAMiddleOne: four snapshots, each sharing chunks
// with its neighbours; the second is deleted; the first, the third and the
// fourth restore as they were. No collector runs: whether the deleted one's
// chunks that a neighbour still uses would survive collection is not tested
// here.
func TestSnapshotLadderD2DeleteAMiddleOne(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3", "v4")
	img1 := ladderImage("d2one", []uint64{0, 2}, 4)
	img2 := ladderImage("d2two", []uint64{2, 5}, 3) // chunk 2 shared with s1, chunk 5 kept by s3
	img3 := ladderImage("d2three", []uint64{0, 9}, 2)
	img4 := ladderImage("d2four", []uint64{9, 12}, 2)
	for i, img := range []map[uint64][]byte{img1, img2, img3, img4} {
		env.writeImage(t, "s1", img)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	env.deleteSnapshot(t, "v1", "s2")
	env.requireSnapshotNames(t, "v1", "s1", "s3", "s4")
	env.restoreChecked(t, "s1", "v2", img1, img2, img3, img4)
	env.restoreChecked(t, "s3", "v3", mergeImages(img1, img2, img3), img4)
	env.restoreChecked(t, "s4", "v4", mergeImages(img1, img2, img3, img4))
}

// TestSnapshotLadderD3DeleteWhenContentRecurs: the A, B, A volume of A4
// (one block holds A, then B, then A again, a snapshot after each), then the
// live block is overwritten with C; the first snapshot is deleted; the
// third, whose content equals the first's, and the second restore as they
// were, not as HEAD is now. No collector runs here: this is the logical
// delete and restore, not the reclamation of a recurring chunk.
func TestSnapshotLadderD3DeleteWhenContentRecurs(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	a, b, c := pattern("ladder-d3-blk-A!"), pattern("ladder-d3-blk-B!"), pattern("ladder-d3-blk-C!")
	for i, content := range [][]byte{a, b, a} {
		env.writeBlock(t, "s1", 7, content)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	env.writeBlock(t, "s1", 7, c) // HEAD now differs from every snapshot
	env.deleteSnapshot(t, "v1", "s1")
	env.requireSnapshotNames(t, "v1", "s2", "s3")
	env.restoreChecked(t, "s3", "v2", map[uint64][]byte{7: a})
	env.restoreChecked(t, "s2", "v3", map[uint64][]byte{7: b})
}

// TestSnapshotLadderD4DeleteAllButOneThenCollect: three snapshots, each
// overwriting the same four chunks; the third and then the second are
// deleted through the holder, whose cleaning pass runs right after each
// delete. The third's chunks are still HEAD's, so deleting it frees nothing;
// the second's four chunks are named by nothing after its delete, and the
// cleaning removes exactly those from the pool. No data is lost: HEAD reads
// the newest content and the remaining snapshot restores as it was.
func TestSnapshotLadderD4DeleteAllButOneThenCollect(t *testing.T) {
	env := startLadderEnv(t, "v2")
	chunks := []uint64{0, 1, 2, 3}
	imgs := []map[uint64][]byte{ladderImage("d4one", chunks, 4), ladderImage("d4two", chunks, 4), ladderImage("d4three", chunks, 4)}
	for i, img := range imgs {
		env.writeImage(t, "s1", img)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	stored := env.poolObjects(t, "v1")
	t.Logf("after three snapshots the pool holds %d chunk objects", stored)

	collect := func(name string, freed int) {
		t.Helper()
		if code, out := env.holderDeleteSnapshot(t, "s1", "v1", name); code != http.StatusOK {
			t.Fatalf("holder delete of %s: status %d: %s", name, code, out)
		}
		want := stored - freed
		end := time.Now().Add(60 * time.Second)
		got := env.poolObjects(t, "v1")
		for got != want && time.Now().Before(end) {
			time.Sleep(500 * time.Millisecond)
			got = env.poolObjects(t, "v1")
		}
		if got != want {
			t.Fatalf("after deleting %s the pool holds %d chunk objects; want %d (%d freed)", name, got, want, freed)
		}
		stored = got
	}
	collect("s3", 0)           // its chunks are HEAD's
	collect("s2", len(chunks)) // named by nothing now
	env.requireSnapshotNames(t, "v1", "s1")
	env.requireImage(t, "s1", imgs[2])
	env.restoreChecked(t, "s1", "v2", imgs[0], imgs[1], imgs[2])
}
