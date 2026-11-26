//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
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

// snapshotInfo is what a snapshot's answer and its list entry say about it.
type snapshotInfo struct {
	Name       string `json:"name"`
	HeadGen    uint64 `json:"headGen"`
	CutLSN     uint64 `json:"cutLSN"`
	State      string `json:"state"`
	ChunkCount int    `json:"chunkCount"`
}

// snapshot takes the named snapshot of vol on its host, checks the answer
// names it, and returns the answer.
func (env *handoffEnv) snapshot(t *testing.T, hostID, vol, name string) snapshotInfo {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "snapshot", "--volume", vol, "--status-addr", env.hosts[hostID].status, "--name", name)
	var info snapshotInfo
	if err := json.Unmarshal([]byte(out), &info); err != nil || info.Name != name {
		t.Fatalf("snapshot %s of %s: %v\n%s", name, vol, err, out)
	}
	return info
}

// snapshotEntry is one entry of `sw-block ops chunk snapshots`: a published
// snapshot, its root and the restore targets that depend on it.
type snapshotEntry struct {
	Name       string   `json:"name"`
	Root       string   `json:"root"`
	Dependents []string `json:"dependents"`
}

// listSnapshots lists vol's snapshots (with a name prefix when given) under
// ctx, without failing the test.
func (env *handoffEnv) listSnapshots(ctx context.Context, vol, prefix string) ([]snapshotEntry, error) {
	args := []string{"ops", "chunk", "snapshots", "--master", env.h.masterAddr, "--volume", vol, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath}
	if prefix != "" {
		args = append(args, "--prefix", prefix)
	}
	out, errOut, code, err := runCLI(ctx, env.h, args...)
	if err != nil {
		return nil, err
	}
	var list []snapshotEntry
	if code != 0 || json.Unmarshal([]byte(out), &list) != nil {
		return nil, fmt.Errorf("snapshots of %s: exit %d: %s %s", vol, code, out, errOut)
	}
	return list, nil
}

// snapshots lists vol's snapshots (with a name prefix when given).
func (env *handoffEnv) snapshots(t *testing.T, vol, prefix string) []snapshotEntry {
	t.Helper()
	args := []string{"ops", "chunk", "snapshots", "--master", env.h.masterAddr, "--volume", vol, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath}
	if prefix != "" {
		args = append(args, "--prefix", prefix)
	}
	out, _ := env.h.command(t, args...)
	var list []snapshotEntry
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("snapshots of %s: %v\n%s", vol, err, out)
	}
	return list
}

// restoreAttached creates target from snapshot snap of src and starts its
// blockvolume, but not the restore driver: target serves and stays
// dependent on its source (not independent).
func (env *handoffEnv) restoreAttached(t *testing.T, src, snap, target string) {
	t.Helper()
	b := env.hosts["n-"+target]
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", src, "--snapshot", snap, "--target", target,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	d := env.h.start(t, "blockvolume-"+target, env.h.volume, env.volArgsFor(b)...)
	d.waitLog(t, 60*time.Second, fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at r1@", target))
	poll(t, 60*time.Second, fmt.Sprintf("%s never reported its frontend ready", target), func() bool {
		return volumeFrontendReady(b.status, target)
	})
}

// TestSnapshotLadderB1SnapshotQuery: three snapshots with writes between
// them. The list is exactly the published snapshots, in name order, each
// with its own root; a prefix shows one. A snapshot being taken appears in
// the list only once published, and then stays with the same root; the test
// fails as inconclusive unless at least one listing ran while the snapshot
// was in flight and did not show it. A snapshot of a restore target that is
// not fully local is taken, and a restore from it is refused: it names the
// source's chunks.
func TestSnapshotLadderB1SnapshotQuery(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	var made []snapshotInfo
	for i, w := range []struct {
		lba  uint64
		text string
	}{{0, "ladder-b1-blk-A!"}, {2 * ladderChunkBlocks, "ladder-b1-blk-B!"}, {1, "ladder-b1-blk-C!"}} {
		env.writeBlock(t, "s1", w.lba, pattern(w.text))
		made = append(made, env.snapshot(t, "s1", "v1", fmt.Sprintf("s%d", i+1)))
	}
	list := env.snapshots(t, "v1", "")
	if len(list) != len(made) {
		t.Fatalf("list has %d snapshots; want exactly the %d published: %+v", len(list), len(made), list)
	}
	roots := map[string]bool{}
	for i, got := range list {
		if got.Name != made[i].Name || got.Root == "" || roots[got.Root] {
			t.Fatalf("list entry %d %+v; want %s with a root of its own (list %+v)", i, got, made[i].Name, list)
		}
		roots[got.Root] = true
	}
	if one := env.snapshots(t, "v1", "s2"); len(one) != 1 || one[0].Name != "s2" || one[0].Root != list[1].Root {
		t.Fatalf("show s2: %+v; want %+v", one, list[1])
	}

	// A snapshot being taken: listed only committed, and then it stays. The
	// write before it is acknowledged first, so its cut is past s3's; a
	// second write follows its start, and may land before or after its cut
	// (the test claims nothing about it).
	env.writeBlock(t, "s1", 3, pattern("ladder-b1-blk-D!"))
	type result struct {
		out, errOut string
		code        int
		err         error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	done := make(chan result, 1)
	joined := false
	t.Cleanup(func() { // before the daemons stop: cancel and join the worker
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second): // runCLI's 5 s WaitDelay, with a margin
				t.Error("snapshot s4 worker did not finish within 10 s of its cancellation: its completion is unresolved")
			}
		}
	})
	go func() {
		out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status, "--name", "s4")
		done <- result{out, errOut, code, err}
	}()
	env.writeBlock(t, "s1", 4, pattern("ladder-b1-blk-E!"))
	var observed []snapshotEntry
	var r result
	inFlightUnlisted := 0 // listings begun before s4 answered that did not show it
	// The observation bound: every list probe runs within what is left of it,
	// and a probe that returns after it fails the test.
	deadline := time.Now().Add(160 * time.Second)
	obs, obsCancel := context.WithDeadline(context.Background(), deadline)
	defer obsCancel()
	for running := true; running; {
		select {
		case r = <-done:
			joined, running = true, false
		default:
		}
		if running && time.Now().After(deadline) {
			t.Fatal("snapshot s4 did not return within its bound")
		}
		list, err := env.listSnapshots(obs, "v1", "s4")
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("listing s4 within the observation bound: %v", err)
		}
		var s4 *snapshotEntry
		for _, e := range list {
			e := e
			s4 = &e
		}
		if running && s4 == nil {
			inFlightUnlisted++
		}
		switch {
		case s4 == nil && len(observed) > 0:
			t.Fatal("s4 was listed, then disappeared")
		case s4 != nil && (s4.Root == "" || roots[s4.Root]):
			t.Fatalf("s4 listed without a root of its own: %+v", *s4)
		case s4 != nil:
			observed = append(observed, *s4)
		}
	}
	if r.err != nil || r.code != 0 {
		t.Fatalf("snapshot s4: exit %d %v\n%s\n%s", r.code, r.err, r.out, r.errOut)
	}
	if inFlightUnlisted == 0 {
		t.Fatalf("inconclusive: no listing ran while s4 was in flight and unlisted (%d listings saw it committed)", len(observed))
	}
	t.Logf("s4: %d listings while in flight did not show it; %d showed it published", inFlightUnlisted, len(observed))
	var created snapshotInfo
	if err := json.Unmarshal([]byte(r.out), &created); err != nil || created.Name != "s4" {
		t.Fatalf("snapshot s4 answer: %v\n%s", err, r.out)
	}
	final := env.snapshots(t, "v1", "s4")
	if len(final) != 1 {
		t.Fatalf("s4 not listed after its snapshot returned: %+v", final)
	}
	for _, o := range observed {
		if o.Root != final[0].Root {
			t.Fatalf("s4 listed as %+v while in flight, then as %+v: a listed snapshot changed", o, final[0])
		}
	}

	// A snapshot of a restore target that is not fully local is taken, and
	// names the source's chunks, so a restore from it is refused.
	env.restoreAttached(t, "v1", "s1", "v2")
	if st := kzReadStats(t, env.hosts["n-v2"]); st.AbsentChunks == 0 {
		t.Fatalf("setup: v2 holds every chunk already; want a restore target not fully local")
	}
	env.snapshot(t, "n-v2", "v2", "x")
	if l := env.snapshots(t, "v2", ""); len(l) != 1 || l[0].Name != "x" {
		t.Fatalf("v2's snapshot x is not listed: %+v", l)
	}
	out, errOut, code := env.h.run(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v2", "--snapshot", "x", "--target", "v3",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	if code == 0 || !strings.Contains(out, `"code":"not_admitted"`) || !strings.Contains(out, "names another volume's chunks") {
		t.Fatalf("restore from a restore target's snapshot: exit %d, want not_admitted naming another volume's chunks\n%s\n%s", code, out, errOut)
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
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", src, "--snapshot", snap, "--target", target,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	d := env.h.start(t, "blockvolume-"+target, env.h.volume, env.volArgsFor(b)...)
	d.waitLog(t, 60*time.Second, fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at r1@", target))
	poll(t, 60*time.Second, fmt.Sprintf("%s never reported its frontend ready", target), func() bool {
		return volumeFrontendReady(b.status, target)
	})
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

// hydrate makes every chunk of target local and checks that none is left
// absent. v1 has no background fill: a restore target fetches a chunk from
// its source's snapshot only when the guest touches it, so this reads one
// block of every chunk (which brings the whole chunk local), then waits for
// the chunk status to show no chunk absent. A restored volume keeps its
// references to the source's snapshot; hydration only makes it read locally.
func (env *handoffEnv) hydrate(t *testing.T, target string) {
	t.Helper()
	b := env.hosts["n-"+target]
	c := dialNVMe(t, b.nvme, plannedHandoffNQN)
	// A read fetches its chunk only when the block read is not local: a
	// block the guest wrote is served locally and fills nothing. So read
	// each chunk's last block, which no ladder image writes (ladderImage
	// writes offsets 0, 37, 74, ... in a chunk).
	for lba := uint64(ladderChunkBlocks - 1); lba <= ladderLastBlock; lba += ladderChunkBlocks {
		_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
		c.read(t, lba, 4096)
	}
	c.close()
	const bound = 60 * time.Second
	end := time.Now().Add(bound)
	last := -1
	for time.Now().Before(end) {
		if last = kzReadStats(t, b).AbsentChunks; last == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s: every chunk was read, yet %d are still absent after %v", target, last, bound)
}

// runCLI runs sw-block under ctx and returns its output and exit status. It
// never fails the test, so a worker goroutine may call it; a failure to run
// or a cancelled context comes back as err.
func runCLI(ctx context.Context, h *plannedHandoffHarness, args ...string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, h.cli, args...)
	cmd.Dir = h.dir
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	h.logCommand(args, stdout.String(), stderr.String(), err)
	var exit *exec.ExitError
	switch {
	case err == nil:
		return stdout.String(), stderr.String(), 0, nil
	case errors.As(err, &exit) && ctx.Err() == nil:
		return stdout.String(), stderr.String(), exit.ExitCode(), nil
	}
	return stdout.String(), stderr.String(), -1, fmt.Errorf("sw-block %q: %w", args, err)
}

// ladderImage is a written picture of a volume: blocks in several chunks.
func ladderImage(tag string, chunks []uint64, perChunk int) map[uint64][]byte {
	img := map[uint64][]byte{}
	for _, c := range chunks {
		for i := 0; i < perChunk; i++ {
			lba := c*ladderChunkBlocks + uint64(i)*37
			img[lba] = pattern(fmt.Sprintf("%-8s%08d", tag, lba)[:16])
		}
	}
	return img
}

func (env *handoffEnv) writeImage(t *testing.T, hostID string, img map[uint64][]byte) {
	t.Helper()
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	for lba, b := range img {
		c.write(t, lba, b)
	}
	c.flush(t)
}

// requireImage reads every block of img on the host and the zeros next to
// each written run, and the edge blocks. Every block of later (writes made
// after the snapshot that img pictures) that img does not hold must read
// zero: data written after the snapshot must not leak into its restore.
func (env *handoffEnv) requireImage(t *testing.T, hostID string, img map[uint64][]byte, later ...map[uint64][]byte) {
	t.Helper()
	zero := make([]byte, 4096)
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	for lba, want := range img {
		if got := c.read(t, lba, 4096); !bytes.Equal(got, want) {
			t.Fatalf("%s: block %d differs from the snapshot", hostID, lba)
		}
		if _, written := img[lba+1]; !written && lba+1 <= ladderLastBlock {
			if got := c.read(t, lba+1, 4096); !bytes.Equal(got, zero) {
				t.Fatalf("%s: block %d should be zero", hostID, lba+1)
			}
		}
	}
	for _, l := range later {
		for lba := range l {
			if _, held := img[lba]; held {
				continue // an overwrite: img's value was checked above
			}
			if got := c.read(t, lba, 4096); !bytes.Equal(got, zero) {
				t.Fatalf("%s: block %d was written after the snapshot and should read zero", hostID, lba)
			}
		}
	}
	c.close()
	env.requireEdges(t, hostID)
}

func mergeImages(imgs ...map[uint64][]byte) map[uint64][]byte {
	out := map[uint64][]byte{}
	for _, img := range imgs {
		for k, v := range img {
			out[k] = v
		}
	}
	return out
}

// TestSnapshotLadderC1FullRestore: blocks in five chunks are written and a
// snapshot taken, then some of them overwritten and new chunks written; the
// snapshot's restore into a new volume, once its status says independent,
// equals the snapshot block by block, without the later writes.
func TestSnapshotLadderC1FullRestore(t *testing.T) {
	env := startLadderEnv(t, "v2")
	img := ladderImage("c1", []uint64{0, 1, 7, 20, 63}, 8)
	env.writeImage(t, "s1", img)
	env.snapshot(t, "s1", "v1", "full")
	later := mergeImages(ladderImage("c1later", []uint64{1, 20}, 8), ladderImage("c1new", []uint64{9, 33}, 3))
	env.writeImage(t, "s1", later)
	env.restoreInto(t, "v1", "full", "v2")
	env.hydrate(t, "v2")
	env.requireImage(t, "n-v2", img, later)
}

// TestSnapshotLadderC2IncrementalRestore: three snapshots, each after more
// writes; the restores of the second and the third, each to independence,
// equal those snapshots (the earlier writes and theirs, nothing later).
func TestSnapshotLadderC2IncrementalRestore(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	img1 := ladderImage("c2one", []uint64{0, 5}, 4)
	img2 := ladderImage("c2two", []uint64{5, 9}, 3) // overwrites some of chunk 5
	img3 := ladderImage("c2three", []uint64{0, 30}, 2)
	env.writeImage(t, "s1", img1)
	env.snapshot(t, "s1", "v1", "s1")
	env.writeImage(t, "s1", img2)
	env.snapshot(t, "s1", "v1", "s2")
	env.writeImage(t, "s1", img3)
	env.snapshot(t, "s1", "v1", "s3")

	env.restoreInto(t, "v1", "s2", "v2")
	env.hydrate(t, "v2")
	env.requireImage(t, "n-v2", mergeImages(img1, img2), img3) // nothing written after s2, chunk 30 included
	env.restoreInto(t, "v1", "s3", "v3")
	env.hydrate(t, "v3")
	env.requireImage(t, "n-v3", mergeImages(img1, img2, img3))
}

// TestSnapshotLadderC3RestoreThenWriteThenSnapshotIt: a restored volume,
// once every chunk is local, is written and snapshotted itself, then
// written again. Its snapshot still names the source's chunks for the
// chunks it did not write (filling makes bytes local; it does not take the
// source's chunks over), so a restore from it is refused, naming another
// volume's chunks. The restored volume reads its own image, and the
// source's snapshot, also written past, still restores unchanged.
func TestSnapshotLadderC3RestoreThenWriteThenSnapshotIt(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3", "v4")
	src := ladderImage("c3src", []uint64{0, 3}, 4)
	env.writeImage(t, "s1", src)
	env.snapshot(t, "s1", "v1", "base")
	srcLater := ladderImage("c3srclater", []uint64{3, 15}, 2)
	env.writeImage(t, "s1", srcLater)

	env.restoreInto(t, "v1", "base", "v2")
	env.hydrate(t, "v2")
	own := ladderImage("c3own", []uint64{3, 11}, 3)
	env.writeImage(t, "n-v2", own)
	env.snapshot(t, "n-v2", "v2", "mine")
	ownLater := ladderImage("c3ownlater", []uint64{0, 11, 25}, 2)
	env.writeImage(t, "n-v2", ownLater)

	out, errOut, code := env.h.run(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v2", "--snapshot", "mine", "--target", "v3",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	if code == 0 || !strings.Contains(out, `"code":"not_admitted"`) || !strings.Contains(out, "names another volume's chunks") {
		t.Fatalf("restore from the restored volume's snapshot: exit %d, want not_admitted naming another volume's chunks\n%s\n%s", code, out, errOut)
	}
	env.requireImage(t, "n-v2", mergeImages(src, own, ownLater), srcLater)
	env.restoreInto(t, "v1", "base", "v4") // the source's snapshot is unchanged
	env.hydrate(t, "v4")
	env.requireImage(t, "n-v4", src, srcLater, own, ownLater)
}
