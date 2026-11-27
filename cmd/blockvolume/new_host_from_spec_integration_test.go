//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCellNewHostFromTheClusterSpec: a host is added through the
// configuration path while volumes serve. Nodes are registered only from
// --cluster-spec at blockmaster start (importLifecycleNodeInventory ->
// Nodes.RegisterNode); there is no command that registers one. So: v1 serves
// on s1 beside sibling volumes; a planned move of v1 to s2, not yet in the
// spec, is refused as not registered; s2 is added to the cluster spec and
// blockmaster restarts, well inside the workspace lease; every sibling still
// takes a write and reads it back; the same move then succeeds, s2 starts and
// adopts v1 and reads back v1's block. No sibling write failed at any point,
// and every acknowledged sibling write reads back (the harness read-back).
func TestCellNewHostFromTheClusterSpec(t *testing.T) {
	// The other volumes are what must keep serving, and the test needs two of
	// them: it runs at 3 volumes in every cell (1, 2 and 3 all mean 3 here).
	t.Setenv(testVolumesEnv, "3")
	env := startHandoffEnv(t, 1)
	must, mustU := fatalOn(t)
	h := env.h

	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	block := pattern("new-host-spec-01")
	env.writeBlock(t, "s1", 0, block)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	env.revoke(t, e, g)
	if err := old.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)

	// The new host's endpoints exist from here on (the move names them), but
	// the cluster spec does not list it yet.
	env.hosts["s2"] = bvHost{id: "s2", data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t),
		root: filepath.Join(h.dir, "vol-s2")}

	// Control: blockmaster has not registered s2, so it refuses the move.
	move := env.moveClean(e, "s2", "move-v1-1") // movedEpochTo reads this request id
	out, errOut, code := h.run(t, move...)
	if code == 0 || !strings.Contains(out+errOut, `move target node "s2" is not registered`) {
		t.Fatalf("move to a host not in the cluster spec: exit %d %s %s; want refused by blockmaster, not registered", code, out, errOut)
	}

	// The new host enters the cluster spec; blockmaster restarts with it.
	spec := filepath.Join(h.dir, "cluster-spec.yaml")
	raw, err := os.ReadFile(spec)
	must(err)
	if !bytes.HasPrefix(raw, []byte("nodes:\n")) {
		t.Fatalf("cluster spec %s does not start with its nodes list", spec)
	}
	s2 := env.hosts["s2"]
	added := fmt.Sprintf("nodes:\n  - server_id: %s\n    data_addr: %s\n    ctrl_addr: %s\n", s2.id, s2.data, s2.ctrl)
	must(os.WriteFile(spec, append([]byte(added), raw[len("nodes:\n"):]...), 0o644))
	start := time.Now()
	env.masterProcess.term(t, 30*time.Second)
	env.masterProcess = h.start(t, "blockmaster-with-s2", h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	// The lock line comes before the cluster-spec import: the restart ends
	// when blockmaster answers for v1's record again.
	down, ok := servingWithin(start, 30*time.Second, func() bool {
		_, _, code := h.run(t, env.show...)
		return code == 0
	})
	t.Logf("blockmaster restarted with s2 in the cluster spec and answered in %v", down.Round(time.Millisecond))
	if !ok || down > 10*time.Second {
		t.Fatalf("blockmaster took %v to restart and answer (answered: %v); the siblings' workspace lease is about 20 s", down, ok)
	}

	// Every sibling kept serving: a new write on each reads back.
	_, names := env.siblingWriteFailures()
	if len(names) < 2 {
		t.Fatalf("%d sibling volumes; this test needs other volumes serving", len(names))
	}
	for i, v := range names {
		writeReadSibling(t, env.hosts["n-"+v].nvme, v, siblingVolumeBytes(t)/4096-1-uint64(i), // the writer fills from block 0 up; this one stays unwritten
			pattern(fmt.Sprintf("after-restart-%02d", i)))
	}

	// The same move now succeeds; s2 starts and serves v1.
	moved, _ := h.command(t, move...)
	e2 := mustU(movedEpochTo(moved, e, g, l, "s2"))
	nw := env.start(t, "blockvolume-s2", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("s2 adopted at epoch %d; want %d", got, e2)
	}
	if !bytes.Equal(env.readBlock(t, "s2", 0), block) {
		t.Fatal("the block written on s1 does not read back on the new host s2")
	}
	if !bytes.Equal(env.readBlock(t, "s2", 8), make([]byte, 4096)) {
		t.Fatal("a block never written does not read zero on s2")
	}

	// No sibling write failed while the spec changed and blockmaster restarted.
	failures, _ := env.siblingWriteFailures()
	for v, n := range failures {
		if n != 0 {
			t.Errorf("sibling %s: %d writes failed during the test; want every write acknowledged", v, n)
		}
	}
}

// writeReadSibling writes one block on a sibling and reads it back on the
// same connection, which is closed on every path (a failed write, flush or
// read included).
func writeReadSibling(t *testing.T, addr, volume string, lba uint64, data []byte) {
	t.Helper()
	c := dialNVMe(t, addr, plannedHandoffNQN)
	defer c.close()
	c.write(t, lba, data)
	c.flush(t)
	if !bytes.Equal(c.read(t, lba, 4096), data) {
		t.Fatalf("sibling %s: a write after the restart does not read back", volume)
	}
}

// servingWithin is the time from start until ready first holds (polled), and
// whether it held within the limit.
func servingWithin(start time.Time, limit time.Duration, ready func() bool) (time.Duration, bool) {
	for {
		if ready() {
			return time.Since(start), true
		}
		if time.Since(start) > limit {
			return time.Since(start), false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestServingWithinCountsTheWaitForReadiness: a restart whose log line comes
// at once but which serves only later is measured to the serving, not to the
// log.
func TestServingWithinCountsTheWaitForReadiness(t *testing.T) {
	start := time.Now()
	logged := start // the lock line, at once
	servesAt := start.Add(700 * time.Millisecond)
	down, ok := servingWithin(start, 5*time.Second, func() bool { return !time.Now().Before(servesAt) })
	if !ok || down < 700*time.Millisecond {
		t.Fatalf("measured %v (ok %v); want at least the 700 ms until serving, not the %v until the log", down, ok, logged.Sub(start))
	}
	if _, ok := servingWithin(start, 300*time.Millisecond, func() bool { return false }); ok {
		t.Fatal("a restart that never serves was measured as within the limit")
	}
}
