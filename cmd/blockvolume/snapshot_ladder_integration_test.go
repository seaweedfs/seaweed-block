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
//
// The restore that reads a snapshot also exercises the restore's hydration,
// which belongs to rung C. That is sound for rung A only while no snapshot is
// deleted and no collector runs: an A test must never delete a snapshot or
// run GC (those are rung D).
//
// The automatic capture timer is off and the scheduled snapshot is off,
// pinned explicitly, so every cut in a test is one the test made: A2 counts
// the chunks its own snapshot uploaded.

const (
	ladderChunkBlocks = 1024
	ladderLastBlock   = 65535
	ladderUntouched   = 40 * ladderChunkBlocks // a chunk no test writes
)

// startLadderEnv is the procedure harness with restore targets and the
// timer and the schedule pinned off.
func startLadderEnv(t *testing.T, targets ...string) *handoffEnv {
	t.Helper()
	env := startHandoffEnvWith(t, 1, targets)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	src := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, src, "s1")
	return env
}

// TestSnapshotLadderA1OneBlockOneSnapshot: write one block, take a snapshot,
// and read the snapshot back block by block: it holds that block and zeros
// around it, in an untouched chunk and at the volume's last block, as the
// volume did at the snapshot.
func TestSnapshotLadderA1OneBlockOneSnapshot(t *testing.T) {
	env := startLadderEnv(t, "v2")
	block := pattern("ladder-a1-block!")
	env.writeBlock(t, "s1", 5, block)

	env.snapshot(t, "s1", "v1", "a1")
	env.restoreInto(t, "v1", "a1", "v2")
	want := map[uint64][]byte{5: block}
	env.requireBlocks(t, "n-v2", 0, 16, want)
	env.requireEdges(t, "n-v2")
	env.requireBlocks(t, "s1", 0, 16, want) // the source is unchanged
}

// TestSnapshotLadderA2IncrementalSnapshot: write, snapshot, write a few
// blocks in two chunks, snapshot again. Snapshot 2 holds the volume at its
// point, snapshot 1 is unchanged (read after snapshot 2), and the second
// snapshot uploaded only the two chunks that changed (a test-only property:
// no model states it).
func TestSnapshotLadderA2IncrementalSnapshot(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
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
	env.requireEdges(t, "n-v2")
	env.restoreInto(t, "v1", "s2", "v3")
	env.requireBlocks(t, "n-v3", 0, 4, map[uint64][]byte{0: a, 1: c})
	env.requireBlocks(t, "n-v3", 2*ladderChunkBlocks, 2, map[uint64][]byte{2 * ladderChunkBlocks: b})
	env.requireEdges(t, "n-v3")
}

// TestSnapshotLadderA3WritesDuringASnapshot: blocks are written one after
// another while a snapshot runs (between its start and its commit), three
// times, each starting the snapshot after a different number of writes;
// then a last snapshot is taken. Each snapshot taken during the writes is
// one point in time: of those writes it holds a prefix (every write up to
// some point, none after). The last snapshot holds them all. A run where
// every snapshot landed before or after all of its writes proves nothing
// about the middle and fails as inconclusive.
func TestSnapshotLadderA3WritesDuringASnapshot(t *testing.T) {
	const n = 400
	starts := []int{0, 40, 120} // writes issued before each snapshot starts
	env := startLadderEnv(t, "v2", "v3", "v4", "v5")
	blockAt := func(lba uint64) []byte { return pattern(fmt.Sprintf("a3-lba-%08d!", lba)[:16]) }
	inconclusive := true
	all := map[uint64][]byte{}
	for rep, startAfter := range starts {
		first := uint64(100 + rep*1000)
		name := fmt.Sprintf("during-%d", rep+1)
		env.writeDuringSnapshot(t, name, first, n, startAfter, blockAt)
		for i := uint64(0); i < n; i++ {
			all[first+i] = blockAt(first + i)
		}
		target := fmt.Sprintf("v%d", rep+2)
		env.restoreInto(t, "v1", name, target)
		k := env.prefixLength(t, "n-"+target, first, n, blockAt)
		t.Logf("snapshot %s (started after %d writes) holds the first %d of %d writes", name, startAfter, k, n)
		if k > 0 && k < n {
			inconclusive = false
		}
	}
	if inconclusive {
		t.Fatal("inconclusive: every snapshot landed before or after all of its writes")
	}
	env.snapshot(t, "s1", "v1", "after")
	env.restoreInto(t, "v1", "after", "v5")
	for rep := range starts {
		first := uint64(100 + rep*1000)
		env.requireBlocks(t, "n-v5", first, n, all)
	}
}

// TestSnapshotLadderA4ContentReturnsToAnOldValue: one block holds A, then B,
// then A again, with a snapshot after each; all three read back as they
// were.
func TestSnapshotLadderA4ContentReturnsToAnOldValue(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3", "v4")
	a, b := pattern("ladder-a4-blk-A!"), pattern("ladder-a4-blk-B!")
	for i, content := range [][]byte{a, b, a} {
		env.writeBlock(t, "s1", 7, content)
		env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1))
	}
	for i, content := range [][]byte{a, b, a} {
		target := fmt.Sprintf("v%d", i+2)
		env.restoreInto(t, "v1", fmt.Sprintf("s%d", i+1), target)
		env.requireBlocks(t, "n-"+target, 6, 3, map[uint64][]byte{7: content})
		env.requireEdges(t, "n-"+target)
	}
}

// writeDuringSnapshot writes n blocks from first one after another on v1
// and starts snapshot name after startAfter of them, so that the snapshot
// runs while the rest are written. It fails the test if the snapshot fails.
func (env *handoffEnv) writeDuringSnapshot(t *testing.T, name string, first uint64, n, startAfter int, blockAt func(uint64) []byte) {
	t.Helper()
	type result struct {
		out, errOut string
		code        int
	}
	done := make(chan result, 1)
	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	defer c.close()
	for i := 0; i < n; i++ {
		if i == startAfter {
			go func() {
				o, e, code := env.h.run(t, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status, "--name", name)
				done <- result{o, e, code}
			}()
		}
		lba := first + uint64(i)
		c.write(t, lba, blockAt(lba))
	}
	c.flush(t)
	r := <-done
	if r.code != 0 {
		t.Fatalf("snapshot %s during the writes: exit %d\n%s\n%s", name, r.code, r.out, r.errOut)
	}
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

// requireEdges reads, through the host, a block in a chunk no test writes and
// the volume's last block: both must be zero (a manifest that silently
// drops trailing or untouched chunks would otherwise go unseen).
func (env *handoffEnv) requireEdges(t *testing.T, hostID string) {
	t.Helper()
	env.requireBlocks(t, hostID, ladderUntouched, 1, nil)
	env.requireBlocks(t, hostID, ladderLastBlock, 1, nil)
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

// prefixLength reads n blocks from first on the host and returns k such
// that blocks first .. first+k-1 hold blockAt and the rest are zero; any
// other picture (a later write present while an earlier one is missing, a
// torn block) fails the test: the snapshot was not one point in time.
func (env *handoffEnv) prefixLength(t *testing.T, hostID string, first uint64, n int, blockAt func(uint64) []byte) int {
	t.Helper()
	zero := make([]byte, 4096)
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	k, ended := 0, false
	for i := 0; i < n; i++ {
		lba := first + uint64(i)
		got := c.read(t, lba, 4096)
		switch {
		case !ended && bytes.Equal(got, blockAt(lba)):
			k++
		case bytes.Equal(got, zero):
			ended = true
		default:
			t.Fatalf("%s: block %d after the snapshot's prefix of %d holds data: not one point in time", hostID, lba, k)
		}
	}
	return k
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
