//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Rung D of the snapshot ladder: deletes. A deleted snapshot leaves every
// other snapshot restorable, whichever one is deleted (the newest, a middle
// one, one whose content recurs later), and the collector then reclaims the
// chunk data only the deleted snapshots referenced.
//
// The collector's grace may not be shorter than the operation deadline plus
// the clock skew plus the CAS apply bound (11 minutes with the defaults), and
// an unreferenced chunk is reclaimed only by a second mark at least the grace
// after the first, on a chunk older than the grace: D4 runs for about 25
// minutes.

const (
	ladderGCGrace = 11 * time.Minute
	// ladderGCWait is the grace plus the collector's clock skew allowance
	// and its one-second timestamp resolution, with a margin.
	ladderGCWait = ladderGCGrace + time.Minute + 15*time.Second
)

// deleteSnapshot deletes vol's snapshot name and checks the answer names it.
func (env *handoffEnv) deleteSnapshot(t *testing.T, vol, name string) {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "snapshot-delete", "--volume", vol, "--name", name,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	var body struct {
		VolumeID string `json:"volumeID"`
		Name     string `json:"name"`
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil || body.Name != name {
		t.Fatalf("delete %s of %s: %v\n%s", name, vol, err, out)
	}
}

// requireSnapshotNames checks vol's snapshot list is exactly names, in order.
func (env *handoffEnv) requireSnapshotNames(t *testing.T, vol string, names ...string) {
	t.Helper()
	list := env.snapshots(t, vol, "")
	ok := len(list) == len(names)
	for i := 0; ok && i < len(list); i++ {
		ok = list[i].Name == names[i] && list[i].State == "active"
	}
	if !ok {
		t.Fatalf("snapshots of %s: %+v; want exactly %v, active", vol, list, names)
	}
}

// restoreChecked restores snap of v1 into target to independence and checks
// it equals img.
func (env *handoffEnv) restoreChecked(t *testing.T, snap, target string, img map[uint64][]byte) {
	t.Helper()
	env.restoreInto(t, "v1", snap, target)
	env.waitIndependent(t, target)
	env.requireImage(t, "n-"+target, img)
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
	env.restoreChecked(t, "s1", "v2", img1)
	env.restoreChecked(t, "s2", "v3", mergeImages(img1, img2))
}

// TestSnapshotLadderD2DeleteAMiddleOne: four snapshots, each sharing chunks
// with its neighbours; the second is deleted; the first, the third and the
// fourth restore as they were (the deleted one's chunks that a neighbour or a
// later snapshot still uses are kept).
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
	env.restoreChecked(t, "s1", "v2", img1)
	env.restoreChecked(t, "s3", "v3", mergeImages(img1, img2, img3))
	env.restoreChecked(t, "s4", "v4", mergeImages(img1, img2, img3, img4))
}

// TestSnapshotLadderD3DeleteWhenContentRecurs: the A, B, A volume of A4
// (one block holds A, then B, then A again, a snapshot after each); the
// first snapshot is deleted; the third, whose content equals the first's,
// and the second restore as they were.
func TestSnapshotLadderD3DeleteWhenContentRecurs(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	a, b := pattern("ladder-d3-blk-A!"), pattern("ladder-d3-blk-B!")
	for i, content := range [][]byte{a, b, a} {
		env.writeBlock(t, "s1", 7, content)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	env.deleteSnapshot(t, "v1", "s1")
	env.requireSnapshotNames(t, "v1", "s2", "s3")
	env.restoreChecked(t, "s3", "v2", map[uint64][]byte{7: a})
	env.restoreChecked(t, "s2", "v3", map[uint64][]byte{7: b})
}

// gcObject is one chunk object a collector pass names as eligible.
type gcObject struct {
	Volume string `json:"volume"`
	Key    string `json:"key"`
	FID    string `json:"fid"`
}

// gc runs one collector pass (applying or a preview) and returns the chunk
// objects it found eligible and how many it reclaimed.
func (env *handoffEnv) gc(t *testing.T, apply bool) ([]gcObject, int) {
	t.Helper()
	args := []string{"ops", "chunk", "gc", "--master", env.h.masterAddr, "--grace", ladderGCGrace.String(),
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath}
	if apply {
		args = append(args, "--apply")
	}
	out, _ := env.h.command(t, args...)
	var body struct {
		MarkSaved       bool       `json:"mark_saved"`
		UnknownOutcomes []string   `json:"unknown_outcomes"`
		FailedDeletes   []any      `json:"failed_deletes"`
		WouldDelete     []gcObject `json:"would_delete"`
		Candidates      int
		Reclaimed       int
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil || (apply && !body.MarkSaved) || len(body.UnknownOutcomes) != 0 || len(body.FailedDeletes) != 0 {
		t.Fatalf("gc pass (apply %v): %v\n%s", apply, err, out)
	}
	t.Logf("gc pass (apply %v): %d candidates, %d eligible, %d reclaimed", apply, body.Candidates, len(body.WouldDelete), body.Reclaimed)
	return body.WouldDelete, body.Reclaimed
}

// exclusiveBytes is what the usage command says deleting snapshot name of vol
// makes eligible for collection: the chunk bytes no other holder references.
func (env *handoffEnv) exclusiveBytes(t *testing.T, vol, name string) uint64 {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "usage", "--volume", vol,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	var u struct {
		Snapshots []struct {
			Name           string `json:"name"`
			ExclusiveBytes uint64 `json:"exclusiveBytes"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(out), &u); err != nil {
		t.Fatalf("usage of %s: %v\n%s", vol, err, out)
	}
	for _, s := range u.Snapshots {
		if s.Name == name {
			return s.ExclusiveBytes
		}
	}
	t.Fatalf("usage of %s has no snapshot %s\n%s", vol, name, out)
	return 0
}

// blobStored reports whether the weed store still holds the chunk data of
// fid (a volume server answers 200 for it, 404 once it is deleted).
func (env *handoffEnv) blobStored(t *testing.T, fid string) bool {
	t.Helper()
	c := &http.Client{Timeout: 10 * time.Second}
	vid, _, ok := strings.Cut(fid, ",")
	if !ok {
		t.Fatalf("chunk object fid %q has no volume id", fid)
	}
	resp, err := c.Get("http://" + env.cluster.master + "/dir/lookup?volumeId=" + vid)
	if err != nil {
		t.Fatal(err)
	}
	var lookup struct {
		Locations []struct {
			URL string `json:"url"`
		} `json:"locations"`
	}
	err = json.NewDecoder(resp.Body).Decode(&lookup)
	resp.Body.Close()
	if err != nil || len(lookup.Locations) == 0 {
		t.Fatalf("weed lookup of volume %s: %v %+v", vid, err, lookup)
	}
	resp, err = c.Get("http://" + lookup.Locations[0].URL + "/" + fid)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusNotFound:
		return false
	}
	t.Fatalf("weed read of %s: status %d", fid, resp.StatusCode)
	return false
}

// TestSnapshotLadderD4DeleteAllButOneThenCollect: three snapshots, each
// overwriting the same four chunks; the third and then the second are
// deleted. The third's chunks are still HEAD's, so only the chunks the
// second's cut uploaded are referenced by nothing; usage says deleting it
// makes chunk bytes eligible. Two collector passes the grace apart reclaim
// the CHUNK DATA of exactly those chunks: a preview before the second pass
// names one eligible object per chunk the second's cut uploaded, each still
// readable from the weed store; the applying pass reclaims those and only
// those, and each is then gone from the store. HEAD still reads the newest
// content and the remaining snapshot restores as it was.
//
// What is measured is chunk data in the weed store, not filer entries, and
// the bound is exact only for objects the collector can enumerate. Excluded,
// known limits of today's format:
//   - a reclaimed binding's filer entry is kept for ever (marked reclaimed),
//     so filer entry counts and usage's stored bindings and stored bytes do
//     not fall after collection and are not compared;
//   - a needle uploaded before its binding was written (a retried upload)
//     cannot be enumerated and is never reclaimed; a run without upload
//     retries makes none, and the test does not look for them.
//
// So after collection the store's enumerable chunk data is at most what HEAD
// and the remaining snapshot reference; unenumerable needles are not bounded.
func TestSnapshotLadderD4DeleteAllButOneThenCollect(t *testing.T) {
	env := startLadderEnv(t, "v2")
	chunks := []uint64{0, 1, 2, 3}
	imgs := []map[uint64][]byte{ladderImage("d4one", chunks, 4), ladderImage("d4two", chunks, 4), ladderImage("d4three", chunks, 4)}
	uploads := make([]int, len(imgs))
	for i, img := range imgs {
		env.writeImage(t, "s1", img)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
		uploads[i] = env.lastCutUploads(t, "s1", "v1")
	}
	if uploads[1] != len(chunks) {
		t.Fatalf("the second snapshot's cut uploaded %d chunks; want the %d it overwrote", uploads[1], len(chunks))
	}
	env.deleteSnapshot(t, "v1", "s3")
	if b := env.exclusiveBytes(t, "v1", "s2"); b == 0 {
		t.Fatal("usage says deleting s2 frees no chunk bytes; its four overwritten chunks are referenced by nothing else")
	} else {
		t.Logf("deleting s2 makes %d chunk bytes eligible", b)
	}
	env.deleteSnapshot(t, "v1", "s2")
	env.requireSnapshotNames(t, "v1", "s1")
	deleted := time.Now()

	time.Sleep(time.Until(deleted.Add(ladderGCWait))) // the chunks are older than the grace
	if _, n := env.gc(t, true); n != 0 {
		t.Fatalf("the first mark reclaimed %d chunks; a chunk is reclaimed only by a second mark", n)
	}
	first := time.Now()
	time.Sleep(time.Until(first.Add(ladderGCWait))) // a second mark the grace after the first

	eligible, _ := env.gc(t, false)
	if len(eligible) != uploads[1] {
		t.Fatalf("%d chunk objects eligible: %+v; want the %d the second snapshot's cut uploaded", len(eligible), eligible, uploads[1])
	}
	want := map[string]bool{}
	for _, o := range eligible {
		if o.Volume != "v1" || !env.blobStored(t, o.FID) {
			t.Fatalf("eligible object %+v is not v1 chunk data in the store", o)
		}
		want[o.FID] = true
	}
	reclaimedObjs, n := env.gc(t, true)
	if n != len(want) || len(reclaimedObjs) != len(want) {
		t.Fatalf("the second mark reclaimed %d of %d eligible: %+v", n, len(want), reclaimedObjs)
	}
	for _, o := range reclaimedObjs {
		if !want[o.FID] {
			t.Fatalf("the second mark reclaimed %+v, which the preview did not name", o)
		}
		if env.blobStored(t, o.FID) {
			t.Fatalf("reclaimed chunk %+v is still in the store", o)
		}
	}
	env.requireImage(t, "s1", imgs[2])
	env.restoreChecked(t, "s1", "v2", imgs[0])
}
