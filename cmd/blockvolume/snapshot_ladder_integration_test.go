//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The snapshot test ladder, rungs A (one volume): each rung adds one thing.
// It tests what a snapshot contains through the product's own read path for
// a snapshot, a restore into a new volume, so the same tests hold on any
// chunk format. Volumes are 65536 blocks of 4096 bytes in chunks of 1024
// blocks (chunk c holds LBAs c*1024 .. c*1024+1023).

const ladderChunkBlocks = 1024

// TestSnapshotLadderA1OneBlockOneSnapshot: write one block, take a snapshot,
// and read the snapshot back block by block: it holds that block and zeros
// around it, as the volume did at the snapshot.
func TestSnapshotLadderA1OneBlockOneSnapshot(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	src := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, src, "s1")
	block := pattern("ladder-a1-block!")
	env.writeBlock(t, "s1", 5, block)

	env.snapshot(t, "s1", "v1", "a1")
	env.restoreInto(t, "v1", "a1", "v2")
	want := map[uint64][]byte{5: block}
	env.requireBlocks(t, "n-v2", 0, 16, want)
	env.requireBlocks(t, "s1", 0, 16, want) // the source is unchanged
}

// TestSnapshotLadderA2IncrementalSnapshot: write, snapshot, write a few
// blocks in two chunks, snapshot again. Snapshot 2 holds the volume at its
// point, snapshot 1 is unchanged, and the second snapshot uploaded only the
// two chunks that changed.
func TestSnapshotLadderA2IncrementalSnapshot(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2", "v3"})
	src := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, src, "s1")
	a, b, c := pattern("ladder-a2-blk-A!"), pattern("ladder-a2-blk-B!"), pattern("ladder-a2-blk-C!")

	env.writeBlock(t, "s1", 0, a)
	env.snapshot(t, "s1", "v1", "s1")
	if n := env.lastCutUploads(t, "s1", "v1"); n != 1 {
		t.Fatalf("the first snapshot uploaded %d chunks; want 1 (chunk 0)", n)
	}
	env.writeBlock(t, "s1", 1, c)                   // chunk 0 again
	env.writeBlock(t, "s1", 2*ladderChunkBlocks, b) // chunk 2
	env.snapshot(t, "s1", "v1", "s2")
	if n := env.lastCutUploads(t, "s1", "v1"); n != 2 {
		t.Fatalf("the incremental snapshot uploaded %d chunks; want 2 (chunks 0 and 2, the ones that changed)", n)
	}

	env.restoreInto(t, "v1", "s1", "v2")
	env.requireBlocks(t, "n-v2", 0, 4, map[uint64][]byte{0: a})
	env.requireBlocks(t, "n-v2", 2*ladderChunkBlocks, 2, nil)
	env.restoreInto(t, "v1", "s2", "v3")
	env.requireBlocks(t, "n-v3", 0, 4, map[uint64][]byte{0: a, 1: c})
	env.requireBlocks(t, "n-v3", 2*ladderChunkBlocks, 2, map[uint64][]byte{2 * ladderChunkBlocks: b})
}

// snapshot takes the named snapshot of vol on its host and checks the answer
// names it.
func (env *handoffEnv) snapshot(t *testing.T, hostID, vol, name string) {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "snapshot", "--volume", vol, "--status-addr", env.hosts[hostID].status, "--name", name)
	var info struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(out), &info); err != nil || info.Name != name {
		t.Fatalf("snapshot %s of %s: %v\n%s", name, vol, err, out)
	}
}

// lastCutUploads is the number of chunks the last cut of vol uploaded, from
// its host's durable status.
func (env *handoffEnv) lastCutUploads(t *testing.T, hostID, vol string) int {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get("http://" + env.hosts[hostID].status + "/status/durable?volume=" + vol)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	n, err := lastCutUploadedChunks(body, vol)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// restoreInto restores snapshot snap of src into the new volume target, as
// the deployment page says: create it, start its blockvolume, start the
// restore driver there; it serves at once.
func (env *handoffEnv) restoreInto(t *testing.T, src, snap, target string) {
	t.Helper()
	b := env.hosts["n-"+target]
	env.h.command(t, "ops", "chunk", "restore", "--source", src, "--snapshot", snap, "--target", target,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	d := env.h.start(t, "blockvolume-"+target, env.h.volume, env.volArgsFor(b)...)
	d.waitLog(t, 60*time.Second, fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at r1@", target))
	poll(t, 60*time.Second, fmt.Sprintf("%s never reported its frontend ready", target), func() bool {
		return volumeFrontendReady(b.status, target)
	})
	env.h.command(t, "ops", "chunk", "restore", "--volume", target, "--status-addr", b.status)
}

// requireBlocks reads n blocks from lba on the host and requires each to be
// want[lba] (zeros where want has none).
func (env *handoffEnv) requireBlocks(t *testing.T, hostID string, lba uint64, n int, want map[uint64][]byte) {
	t.Helper()
	zero := make([]byte, 4096)
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	var bad []string
	for i := uint64(0); i < uint64(n); i++ {
		exp, ok := want[lba+i]
		if !ok {
			exp = zero
		}
		if got := c.read(t, lba+i, 4096); !bytes.Equal(got, exp) {
			bad = append(bad, fmt.Sprint(lba+i))
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%s: blocks %s differ from the snapshot's point", hostID, strings.Join(bad, ","))
	}
}

func volumeFrontendReady(statusAddr, vol string) bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + statusAddr + "/status?volume=" + vol)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return frontendReady(b)
}
