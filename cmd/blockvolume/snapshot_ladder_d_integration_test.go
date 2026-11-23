//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// Rung D of the snapshot ladder: deletes. A deleted snapshot leaves every
// other snapshot restorable, whichever one is deleted (the newest, a middle
// one, one whose content recurs later), and after the collector has run the
// store holds only what the remaining manifests reference.
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

// ladderUsage is what the usage command says about a volume's store.
type ladderUsage struct {
	ManifestBindings int  `json:"manifestBindings"`
	StoredBindings   int  `json:"storedBindings"`
	StoredUpperBound bool `json:"storedUpperBound"`
}

func (env *handoffEnv) usage(t *testing.T, vol string) ladderUsage {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "usage", "--volume", vol,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	var u ladderUsage
	if err := json.Unmarshal([]byte(out), &u); err != nil {
		t.Fatalf("usage of %s: %v\n%s", vol, err, out)
	}
	return u
}

// gc runs one applying collector pass and returns how many chunks it
// reclaimed.
func (env *handoffEnv) gc(t *testing.T) int {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "gc", "--master", env.h.masterAddr, "--apply", "--grace", ladderGCGrace.String(),
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	var body struct {
		MarkSaved       bool     `json:"mark_saved"`
		UnknownOutcomes []string `json:"unknown_outcomes"`
		FailedDeletes   []any    `json:"failed_deletes"`
		Candidates      int
		Reclaimed       int
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil || !body.MarkSaved || len(body.UnknownOutcomes) != 0 || len(body.FailedDeletes) != 0 {
		t.Fatalf("gc pass: %v\n%s", err, out)
	}
	t.Logf("gc pass: %d candidates, %d reclaimed", body.Candidates, body.Reclaimed)
	return body.Reclaimed
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

// TestSnapshotLadderD4DeleteAllButOneThenCollect: three snapshots, each
// overwriting the same chunks; the second and the third are deleted. The
// second's chunks are then referenced by nothing (the third's are still
// HEAD's), and the store holds more than the manifests reference. Two
// collector passes the grace apart reclaim exactly that surplus: the store
// then holds only what the remaining snapshot and HEAD reference, and the
// remaining snapshot and HEAD read as they were.
func TestSnapshotLadderD4DeleteAllButOneThenCollect(t *testing.T) {
	env := startLadderEnv(t, "v2")
	chunks := []uint64{0, 1, 2, 3}
	imgs := []map[uint64][]byte{ladderImage("d4one", chunks, 4), ladderImage("d4two", chunks, 4), ladderImage("d4three", chunks, 4)}
	for i, img := range imgs {
		env.writeImage(t, "s1", img)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	env.deleteSnapshot(t, "v1", "s3")
	env.deleteSnapshot(t, "v1", "s2")
	env.requireSnapshotNames(t, "v1", "s1")
	written := time.Now()

	before := env.usage(t, "v1")
	surplus := before.StoredBindings - before.ManifestBindings
	t.Logf("before the collector: %+v", before)
	if surplus < len(chunks) || !before.StoredUpperBound {
		t.Fatalf("after the deletes the store holds %d unreferenced chunks; want at least the %d of s2", surplus, len(chunks))
	}

	time.Sleep(time.Until(written.Add(ladderGCWait))) // the chunks are older than the grace
	reclaimed := env.gc(t)
	first := time.Now()
	time.Sleep(time.Until(first.Add(ladderGCWait))) // a second mark the grace after the first
	reclaimed += env.gc(t)

	after := env.usage(t, "v1")
	t.Logf("after the collector: %+v", after)
	if after.StoredBindings != after.ManifestBindings || after.StoredUpperBound || reclaimed != surplus {
		t.Fatalf("after the collector the store holds %d chunks, the manifests reference %d (upper bound %v); reclaimed %d of %d",
			after.StoredBindings, after.ManifestBindings, after.StoredUpperBound, reclaimed, surplus)
	}
	if after.ManifestBindings != before.ManifestBindings {
		t.Fatalf("the collector changed what the manifests reference: %d, was %d", after.ManifestBindings, before.ManifestBindings)
	}
	env.requireImage(t, "s1", imgs[2])
	env.restoreChecked(t, "s1", "v2", imgs[0])
}
