//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"context"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/lifecycleclient"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"syscall"
	"testing"
	"time"
)

// TestCutoverWriteSnapshotKillRestartRestore follows the public path through
// real binaries, a stock filer and the NVMe frontend. The guest is quiescent
// at snapshot and kill; process kill is not a power-loss durability claim.
func TestCutoverWriteSnapshotKillRestartRestore(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
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
		"--master", env.h.masterAddr, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	target := startCutoverVolume(t, env, "restored-target", "n-v2")
	target.waitLog(t, 60*time.Second, "blockvolume: volume v2 adopted as PRIMARY at r1@")
	poll(t, 60*time.Second, "restored target never ready", func() bool { return volumeFrontendReady(env.hosts["n-v2"].status, "v2") })
	env.requireBlocks(t, "n-v2", 0, ladderLastBlock+1, before)
	env.requireBlocks(t, "s1", 0, ladderLastBlock+1, after)
}

// Resolve the identity from the real master before launching the public CLI.
// Every host uses its own name's identity; the fixture never mints one.
func startCutoverVolume(t *testing.T, env *handoffEnv, label, host string) *daemon {
	t.Helper()
	name := env.hosts[host].volume
	if name == "" {
		name = "v1"
	}
	conn, err := link.DialMasterFromEnv(env.h.masterAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := lifecycleclient.ResolveVolume(ctx, control.NewLifecycleServiceClient(conn), name)
	if err != nil {
		t.Fatal(err)
	}
	args := append(env.volArgs(host), "--layout-id", id.String())
	return env.h.start(t, label, env.h.volume, args...)
}
