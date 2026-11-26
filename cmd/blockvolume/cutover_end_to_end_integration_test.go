//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"syscall"
	"testing"
	"time"
)

// TestCutoverCreateFromSnapshot pins the public master create contract: one
// request creates the target identity and its restored image together.
func TestCutoverCreateFromSnapshot(t *testing.T) {
	env := startCutoverLayoutEnv(t, false)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	source := startCutoverVolume(t, env, "restore-source", "s1")
	env.adopt(t, source, "s1")
	env.writeBlock(t, "s1", 5, pattern("restore-seam-A!!"))
	env.snapshot(t, "s1", "v1", "restore-seam")
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "restore-seam", "--target", "v2",
		"--master", env.h.masterAddr)
	target := startCutoverVolume(t, env, "restored-target", "n-v2")
	target.waitLog(t, 60*time.Second, "blockvolume: volume v2 adopted as PRIMARY at r1@")
	poll(t, 60*time.Second, "restored target never ready", func() bool { return volumeFrontendReady(env.hosts["n-v2"].status, "v2") })
	env.requireBlocks(t, "n-v2", 0, ladderLastBlock+1, map[uint64][]byte{5: pattern("restore-seam-A!!")})
}

// TestCutoverWriteSnapshotKillRestartRestore follows the public path through
// real binaries, a stock filer and the NVMe frontend. The guest is quiescent
// at snapshot and kill; process kill is not a power-loss durability claim.
func TestCutoverWriteSnapshotKillRestartRestore(t *testing.T) {
	env := startCutoverLayoutEnv(t, false)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	source := startCutoverVolume(t, env, "source-before-kill", "s1")
	env.adopt(t, source, "s1")
	a, b, c := pattern("cutover-block-A!"), pattern("cutover-block-B!"), pattern("cutover-block-C!")
	before := map[uint64][]byte{5: a, 2*ladderChunkBlocks + 7: b}
	env.writeBlock(t, "s1", 5, a)
	env.writeBlock(t, "s1", 2*ladderChunkBlocks+7, b)
	env.snapshot(t, "s1", "v1", "before-kill")
	// A later synced write must survive restart but must not enter the snapshot.
	env.writeBlock(t, "s1", 4*ladderChunkBlocks+9, c)
	source.kill(t)
	if err := source.proc.Signal(syscall.Signal(0)); err == nil {
		t.Fatal("the killed source process still exists")
	}
	restarted := startCutoverVolume(t, env, "source-after-kill", "s1")
	env.adopt(t, restarted, "s1")
	after := map[uint64][]byte{5: a, 2*ladderChunkBlocks + 7: b, 4*ladderChunkBlocks + 9: c}
	env.requireBlocks(t, "s1", 0, ladderLastBlock+1, after)
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "before-kill", "--target", "v2",
		"--master", env.h.masterAddr)
	target := startCutoverVolume(t, env, "restored-target", "n-v2")
	target.waitLog(t, 60*time.Second, "blockvolume: volume v2 adopted as PRIMARY at r1@")
	poll(t, 60*time.Second, "restored target never ready", func() bool { return volumeFrontendReady(env.hosts["n-v2"].status, "v2") })
	env.requireBlocks(t, "n-v2", 0, ladderLastBlock+1, before)
	env.requireBlocks(t, "s1", 0, ladderLastBlock+1, after)
}

func startCutoverLayoutEnv(t *testing.T, precreateTarget bool) *handoffEnv {
	t.Helper()
	return startHandoffEnvConfigured(t, 1, []string{"v2"}, procedureTLSConfig(), precreateTarget)
}

func startCutoverVolume(t *testing.T, env *handoffEnv, label, host string) *daemon {
	t.Helper()
	return env.h.start(t, label, env.h.volume, env.volArgs(host)...)
}
