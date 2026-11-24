//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"
)

// Rung D of the snapshot ladder: deletes. D1-D3 are the logical delete: a
// deleted snapshot leaves every other snapshot restorable, whichever one is
// deleted (the newest, a middle one, one whose content recurs later). No
// collector runs in them (a delete only marks the snapshot and frees its
// name), so they prove nothing about reclamation. D4 runs the collector and
// measures what it reclaims.
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

// restoreChecked restores snap of v1 into target until its status says
// independent and checks it equals img, with every block of later that img
// does not hold reading zero.
func (env *handoffEnv) restoreChecked(t *testing.T, snap, target string, img map[uint64][]byte, later ...map[uint64][]byte) {
	t.Helper()
	env.restoreInto(t, "v1", snap, target)
	env.waitIndependent(t, target)
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

// gcObject is one chunk object a collector pass names as eligible.
type gcObject struct {
	Volume string `json:"volume"`
	Key    string `json:"key"`
	FID    string `json:"fid"`
}

// chunkKeys lists the names (unescaped) of v1's chunk-key bindings, read
// straight from the filer's directory listing, not through the collector.
func (env *handoffEnv) chunkKeys(t *testing.T, vol string) map[string]bool {
	t.Helper()
	c := &http.Client{Timeout: 10 * time.Second}
	dir := path.Join(env.chunkPath, vol, "keys") + "/"
	keys := map[string]bool{}
	for last := ""; ; {
		req, _ := http.NewRequest("GET", "http://"+env.cluster.filerHTTP+dir+"?limit=1000&lastFileName="+url.QueryEscape(last), nil)
		req.Header.Set("Accept", "application/json")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			Entries []struct {
				FullPath string `json:"FullPath"`
			} `json:"Entries"`
			LastFileName          string `json:"LastFileName"`
			ShouldDisplayLoadMore bool   `json:"ShouldDisplayLoadMore"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("filer listing of %s: status %d %v", dir, resp.StatusCode, err)
		}
		for _, e := range page.Entries {
			name, err := url.PathUnescape(path.Base(e.FullPath))
			if err != nil {
				t.Fatalf("chunk key %q: %v", e.FullPath, err)
			}
			keys[name] = true
		}
		if !page.ShouldDisplayLoadMore || page.LastFileName == "" || page.LastFileName == last {
			return keys
		}
		last = page.LastFileName
	}
}

// objectsByKey indexes collector objects by key and counts the records that
// are not distinct v1 chunk objects (another volume, an empty field, or a
// key or fid named again).
func objectsByKey(objs []gcObject) (map[string]gcObject, int) {
	byKey, fids, odd := map[string]gcObject{}, map[string]bool{}, 0
	for _, o := range objs {
		_, dup := byKey[o.Key]
		if o.Volume != "v1" || o.Key == "" || o.FID == "" || fids[o.FID] || dup {
			odd++
			continue
		}
		byKey[o.Key], fids[o.FID] = o, true
	}
	return byKey, odd
}

// requireBudget fails the test before it starts anything when the test
// binary's deadline leaves less than need: a wait cut short by the package
// timeout would skip the harness's cleanup.
func requireBudget(t *testing.T, need time.Duration) {
	t.Helper()
	if dl, ok := t.Deadline(); ok && time.Until(dl) < need {
		t.Fatalf("this test needs %v; the test deadline leaves %v (run with -timeout 45m)", need, time.Until(dl).Round(time.Second))
	}
}

// gc runs one collector pass (applying or a preview) and returns the chunk
// objects it found eligible and how many it reclaimed.
func (env *handoffEnv) gc(t *testing.T, apply bool) ([]gcObject, int) {
	t.Helper()
	args := []string{"ops", "chunk", "gc", "--chunk-collection", "swblock", "--master", env.h.masterAddr, "--grace", ladderGCGrace.String(),
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

// TestSnapshotLadderD4DeleteAllButOneThenCollect is a MEASUREMENT, not a
// pass/fail rung: what the collector reclaims after deletes, against an
// expected set taken independently of it. Three snapshots, each overwriting
// the same four chunks; the third and then the second are deleted. The
// third's chunks are still HEAD's, so the bindings the second's cut created
// (those that appear in the filer between the first and the second
// snapshot) are referenced by nothing. Two collector passes the grace apart
// run; the test reports, in one MEASUREMENT line, how many of the expected
// bindings a preview names and the applying pass reclaims, how many of
// those are gone from the weed store, what else either pass named, and the
// surplus left. It fails only when the harness cannot measure (the setup
// did not create the expected bindings, a command failed) or when data is
// lost: HEAD must still read the newest content and the remaining snapshot
// restore as it was.
//
// What is measured is chunk data in the weed store for the bindings the
// deleted snapshot alone created, not filer entries and not the whole store.
// The known surplus of today's format, not measured:
//   - a reclaimed binding's filer entry is kept for ever (marked reclaimed),
//     so filer entry counts and usage's stored bindings and stored bytes do
//     not fall after collection;
//   - a needle uploaded before its binding was written (a retried upload)
//     cannot be enumerated and is never reclaimed;
//   - disk space returns only when weed compacts its volume.
func TestSnapshotLadderD4DeleteAllButOneThenCollect(t *testing.T) {
	requireBudget(t, 2*ladderGCWait+10*time.Minute)
	env := startLadderEnv(t, "v2")
	chunks := []uint64{0, 1, 2, 3}
	imgs := []map[uint64][]byte{ladderImage("d4one", chunks, 4), ladderImage("d4two", chunks, 4), ladderImage("d4three", chunks, 4)}
	uploads := make([]int, len(imgs))
	keysAfter := make([]map[string]bool, len(imgs))
	for i, img := range imgs {
		env.writeImage(t, "s1", img)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
		uploads[i] = env.lastCutUploads(t, "s1", "v1")
		keysAfter[i] = env.chunkKeys(t, "v1")
	}
	expected := map[string]bool{} // the bindings the second snapshot's cut created
	for k := range keysAfter[1] {
		if !keysAfter[0][k] {
			expected[k] = true
		}
	}
	t.Logf("the second snapshot's cut uploaded %d chunks and created %d bindings", uploads[1], len(expected))
	if len(expected) < len(chunks) {
		t.Fatalf("the second snapshot's cut created %d bindings; want at least one per overwritten chunk (%d)", len(expected), len(chunks))
	}
	if uploads[1] != len(chunks) {
		t.Fatalf("the second snapshot's cut uploaded %d chunks; want the %d it overwrote", uploads[1], len(chunks))
	}
	env.deleteSnapshot(t, "v1", "s3")
	exclusive := env.exclusiveBytes(t, "v1", "s2")
	env.deleteSnapshot(t, "v1", "s2")
	env.requireSnapshotNames(t, "v1", "s1")
	deleted := time.Now()

	requireBudget(t, time.Until(deleted.Add(ladderGCWait))+ladderGCWait+5*time.Minute)
	time.Sleep(time.Until(deleted.Add(ladderGCWait))) // the chunks are older than the grace
	_, firstReclaimed := env.gc(t, true)
	first := time.Now()
	requireBudget(t, ladderGCWait+5*time.Minute)
	time.Sleep(time.Until(first.Add(ladderGCWait))) // a second mark the grace after the first

	eligible, _ := env.gc(t, false)
	named, oddPreview := objectsByKey(eligible)
	namedExpected, namedOther, storedBefore := 0, 0, 0
	for k, o := range named {
		if !expected[k] {
			namedOther++
			continue
		}
		namedExpected++
		if env.blobStored(t, o.FID) {
			storedBefore++
		}
	}
	reclaimedObjs, n := env.gc(t, true)
	reclaimed, oddApply := objectsByKey(reclaimedObjs)
	reclaimedExpected, reclaimedOther, goneAfter := 0, 0, 0
	for k, o := range reclaimed {
		if !expected[k] {
			reclaimedOther++
			continue
		}
		reclaimedExpected++
		if !env.blobStored(t, o.FID) {
			goneAfter++
		}
	}
	t.Logf("D4 MEASUREMENT: expected %d bindings (s2's cut uploaded %d chunks; usage: deleting s2 frees %d bytes); "+
		"first mark reclaimed %d; preview named %d of them (%d still in the store), %d others, %d malformed; "+
		"apply reclaimed %d (%d of them, %d now gone from the store), %d others, %d malformed; surplus left: %d of the expected",
		len(expected), uploads[1], exclusive, firstReclaimed, namedExpected, storedBefore, namedOther, oddPreview,
		n, reclaimedExpected, goneAfter, reclaimedOther, oddApply, len(expected)-goneAfter)
	env.requireImage(t, "s1", imgs[2])
	env.restoreChecked(t, "s1", "v2", imgs[0], imgs[1], imgs[2])
}
