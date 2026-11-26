//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// Rung E: kill the real holder at applied Begin, object and Commit writes.
// The filer's reply is held until SIGKILL, and HEAD is observed before restart.
// Per-write kernel controls remain TestSnapshotSession_InterruptAtEveryStep.

// filerNames lists the entry names (unescaped) of a filer directory, read
// straight from the filer (not through the product); a missing directory
// lists as empty.
func (env *handoffEnv) filerNames(t *testing.T, dir string) map[string]bool {
	t.Helper()
	c := &http.Client{Timeout: 10 * time.Second}
	names := map[string]bool{}
	for last := ""; ; {
		req, _ := http.NewRequest("GET", "http://"+env.cluster.filerHTTP+dir+"/?limit=1000&lastFileName="+url.QueryEscape(last), nil)
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
		if resp.StatusCode == http.StatusNotFound {
			return names
		}
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("filer listing of %s: status %d %v", dir, resp.StatusCode, err)
		}
		for _, e := range page.Entries {
			name, err := url.PathUnescape(path.Base(e.FullPath))
			if err != nil {
				t.Fatalf("filer entry %q: %v", e.FullPath, err)
			}
			names[name] = true
		}
		if !page.ShouldDisplayLoadMore || page.LastFileName == "" || page.LastFileName == last {
			return names
		}
		last = page.LastFileName
	}
}

// TestSnapshotLadderE1CrashDuringASnapshot retains the data/replay/restore
// oracle, but reaches each crash boundary causally through the stock filer.
// A missed boundary is inconclusive, never a successful BEFORE-only campaign.
func TestSnapshotLadderE1CrashDuringASnapshot(t *testing.T) {
	boundaries := []string{"BEGIN", "OBJECT", "COMMIT"}
	var targets []string
	for i := 0; i <= len(boundaries); i++ {
		targets = append(targets, fmt.Sprintf("v%d", i+2))
	}
	if testListenHost() != "127.0.0.1" {
		t.Fatal("causal filer fixture requires loopback")
	}
	var boundary *snapshotFilerBoundary
	cluster := startWorkspaceWeedWith(t, t.TempDir(), w4WeedOpts{VolumeSizeLimitMB: 256, VolumeMax: 32,
		FilerBind: "127.0.0.2", FilerFront: func(c *w4WeedCluster) { boundary = newSnapshotFilerBoundary(t, c) }})
	env := startHandoffEnvOnCluster(t, 1, targets, procedureTLSConfig(), false, cluster)
	id, err := layout.ParseVolumeID(env.layoutID("v1"))
	if err != nil {
		t.Fatal(err)
	}
	boundary.id = id
	boundary.dir = path.Join(env.chunkPath, layout.VolumeDir("v1", id))
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, holder, "s1")
	marker := pattern("ladder-e1-marker")
	env.writeBlock(t, "s1", 0, marker)

	type result struct {
		out, errOut string
		code        int
		err         error
	}
	var images []map[uint64][]byte // what each round's snapshot must hold
	cum := map[uint64][]byte{0: marker}
	steps := map[string]int{}
	for k, kind := range boundaries {
		img := ladderImage(fmt.Sprintf("e1r%d", k), []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 3)
		env.writeImage(t, "s1", img)
		cum = mergeImages(cum, img)
		name, req := fmt.Sprintf("e1-%d", k), fmt.Sprintf("r-e1-%d", k)

		reached := boundary.arm(kind, name)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		done := make(chan result, 1)
		go func() {
			out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status,
				"--name", name, "--request-id", req)
			done <- result{out, errOut, code, err}
		}()
		select {
		case <-reached:
		case <-time.After(30 * time.Second):
			cancel()
			boundary.unblock()
			<-done
			t.Fatalf("inconclusive: snapshot never reached applied %s boundary", kind)
		}
		holder.kill(t)
		killed := time.Now()
		// Restart can abort the old session, so read the real backend first.
		crashed := boundary.head(t)
		_, published := crashed.Snapshots[name]
		if kind == "COMMIT" && !published || kind != "COMMIT" && (published || crashed.Activity.Kind != layout.ActSnap) {
			t.Fatalf("boundary %s disagrees with pre-restart HEAD: %+v", kind, crashed)
		}
		boundary.unblock()
		var r result
		select {
		case r = <-done:
		case <-time.After(60 * time.Second):
			cancel()
			select { // join the worker before failing
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("the snapshot worker did not finish within 10 s of its cancellation")
			}
			t.Fatalf("round %d: the snapshot command did not return within 60 s of the holder's kill", k)
		}
		cancel()
		answered := r.err == nil && r.code == 0

		// Restarted within the lease, as the unit's RestartSec does: the same
		// epoch, serving the blocks written before the kill.
		holder = env.start(t, fmt.Sprintf("blockvolume-v1-r%d", k), "s1")
		if got := env.adopt(t, holder, "s1"); got != e {
			t.Fatalf("round %d: the restarted holder adopted epoch %d; want %d (restarted %v after the kill: within the lease?)", k, got, e, time.Since(killed).Round(time.Millisecond))
		}
		adopted := time.Since(killed)
		poll(t, 60*time.Second, fmt.Sprintf("round %d: the restarted holder never reported its frontend ready", k), func() bool {
			return env.frontendReadyAt("s1")
		})
		env.requireImage(t, "s1", cum)

		pre := env.snapshots(t, "v1", name)
		step := kind
		steps[step]++
		if published != (len(pre) == 1) {
			t.Fatalf("round %d: pre-restart HEAD published=%v but restart lists %+v", k, published, pre)
		}
		t.Logf("round %d: applied %s, pre-restart HEAD gen=%d activity=%s published=%v; exit %d err %v; adopted %v after kill",
			k, step, crashed.Gen, crashed.Activity.Kind, published, r.code, r.err, adopted.Round(time.Millisecond))
		switch {
		case len(pre) > 1:
			t.Fatalf("round %d: %s listed %d times: %+v", k, name, len(pre), pre)
		case answered && len(pre) == 0:
			t.Fatalf("round %d: %s answered success before the kill but is not listed: %s", k, name, r.out)
		}
		// 16 bytes: pattern repeats it to exactly one block.
		post := map[uint64][]byte{uint64(50+k) * ladderChunkBlocks: pattern(fmt.Sprintf("e1-post-%02d-blk!!", k))}
		env.writeImage(t, "s1", post)

		out, _ := env.h.command(t, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status,
			"--name", name, "--request-id", req)
		var info snapshotInfo
		if err := json.Unmarshal([]byte(out), &info); err != nil || info.Name != name {
			t.Fatalf("round %d: retry of %s: %v %s", k, name, err, out)
		}
		l := env.snapshots(t, "v1", name)
		if len(l) != 1 || l[0].Root == "" {
			t.Fatalf("round %d: after the retry %s is listed as %+v; its retry answered %+v", k, name, l, info)
		}
		if len(pre) == 1 && l[0].Root != pre[0].Root {
			t.Fatalf("round %d: %s was listed as %+v before the retry and as %+v after it: a listed snapshot was taken again", k, name, pre[0], l[0])
		}
		if len(pre) == 1 {
			images = append(images, cum) // a replay: the post-restart block is not in it
		} else {
			images = append(images, mergeImages(cum, post)) // cut anew after the restart
		}
		cum = mergeImages(cum, post)
	}
	t.Logf("kill steps: %v; per-write cases pinned by TestSnapshotSession_InterruptAtEveryStep", steps)
	if steps["BEGIN"] != 1 || steps["OBJECT"] != 1 || steps["COMMIT"] != 1 {
		t.Fatalf("inconclusive: no kill landed inside a snapshot (steps %v)", steps)
	}

	next := ladderImage("e1next", []uint64{30, 31}, 3)
	env.writeImage(t, "s1", next)
	env.snapshot(t, "s1", "v1", "e1-next")
	final := mergeImages(cum, next)

	for k := range boundaries {
		later := []map[uint64][]byte{final}
		env.restoreChecked(t, fmt.Sprintf("e1-%d", k), targets[k], images[k], later...)
	}
	env.restoreChecked(t, "e1-next", targets[len(boundaries)], final)
	t.Logf("final restore oracle reached: %d crash snapshots plus next snapshot read back", len(boundaries))
}
