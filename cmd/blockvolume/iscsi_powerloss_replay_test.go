//go:build subprocess

package main_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Power loss with real binaries (V1-SPEC r2.55 gap 3, INV-FE-FLUSH-SYNC-001):
// blockvolume keeps its durable root on a dm-log-writes device. The guest
// writes over iSCSI and issues SYNCHRONIZE CACHE; once the last sync returns,
// the harness marks the log "acknowledged". Later writes and their sync must
// not survive the replay. The
// volume is killed, and the harness replays the log only up to that mark onto
// a fresh device mounted at the same path: the disk as a power cut at the
// mark would have left it, with no page cache. A restarted blockvolume must
// read back every block acknowledged by a sync.
//
// Environment, set by the external replay harness (root, dm_log_writes):
//
//	SWBLOCK_REPLAY_ROOT       mount point of the dm-log-writes filesystem
//	SWBLOCK_REPLAY_CUT        executable: "mark" marks the log, "replay"
//	                          unmounts, replays to the mark and remounts
//	SWBLOCK_TEST_BINS       optional dir with prebuilt blockmaster/blockvolume
//	SWBLOCK_REPLAY_LOGS       optional existing dir for master state and logs
//	SWBLOCK_REPLAY_DROP_SYNC  "1" = negative control: after one SYNCHRONIZE
//	                          CACHE makes the store durable, the measured
//	                          writes get none, so blocks must be lost (the test
//	                          passes and prints REPLAY_control_RED)
//
// The harness is cmd/blockvolume/testdata/powerloss-replay.sh.
func TestPowerLossReplayAckedWritesSurvive(t *testing.T) {
	root, cut := os.Getenv("SWBLOCK_REPLAY_ROOT"), os.Getenv("SWBLOCK_REPLAY_CUT")
	if root == "" && cut == "" {
		t.Skip("requires the isolated dm-log-writes replay harness")
	}
	if root == "" || cut == "" {
		t.Fatalf("SWBLOCK_REPLAY_ROOT=%q and SWBLOCK_REPLAY_CUT=%q must be set together", root, cut)
	}
	dropSync := os.Getenv("SWBLOCK_REPLAY_DROP_SYNC") == "1"
	bins := powerLossBins(t)
	art := t.TempDir() // master state and logs stay off the replayed device
	if d := os.Getenv("SWBLOCK_REPLAY_LOGS"); d != "" {
		art = d // kept for the receipts
	}
	_, masterAddr := startSingleSlotMaster(t, bins, art)

	iqn := "iqn.2026-11.example.powerloss:v1"
	store := filepath.Join(root, "r1-store")
	dataAddr, ctrlAddr := pickAddr(t), pickAddr(t)
	statusAddr, iscsiAddr := pickAddr(t), pickAddr(t)
	opts := volOpts{
		masterAddr:  masterAddr,
		serverID:    "s1",
		replicaID:   "r1",
		dataAddr:    dataAddr,
		ctrlAddr:    ctrlAddr,
		statusAddr:  statusAddr,
		iscsiAddr:   iscsiAddr,
		iscsiIQN:    iqn,
		durableRoot: store,
		logTag:      "powerloss-r1-initial",
	}
	r1 := startG54Volume(t, bins, art, opts)
	waitHealthyReplica(t, statusAddr, "r1", 10*time.Second)
	waitProductIscsiReady(t, iscsiAddr, 5*time.Second)

	const (
		bs      = 4096
		acked   = 160 // LBAs 0..159 are written and synced before the mark
		afterBy = 32  // LBAs 160..191 are written after the mark, never synced
	)
	block := func(lba uint32, gen byte) []byte {
		b := bytes.Repeat([]byte{byte(lba) ^ gen}, bs)
		copy(b, fmt.Sprintf("powerloss lba=%d gen=%d", lba, gen))
		return b
	}
	cli := dialG8Iscsi(t, iscsiAddr, iqn)
	// The store and one block are durable before the measured writes, so a
	// red control shows block loss in an existing store, not a lost store.
	base := block(acked+afterBy, 9)
	cli.write10(t, acked+afterBy, base)
	cli.syncCache10(t)
	want := map[uint32][]byte{}
	for lba := uint32(0); lba < acked; lba++ {
		b := block(lba, 1)
		cli.write10(t, lba, b)
		want[lba] = b
		if !dropSync && (lba+1)%8 == 0 {
			cli.syncCache10(t)
		}
	}
	if !dropSync {
		cli.syncCache10(t)
	}
	runReplayCut(t, cut, "mark")
	for lba := uint32(acked); lba < acked+afterBy; lba++ {
		cli.write10(t, lba, block(lba, 2))
	}
	if !dropSync {
		cli.syncCache10(t) // after the mark: the replay must exclude it
	}
	cli.close(t)

	// Power cut: no shutdown, no page cache. The replay stops at the mark.
	if err := r1.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill blockvolume: %v", err)
	}
	_ = r1.cmd.Wait()
	runReplayCut(t, cut, "replay")

	opts.logTag = "powerloss-r1-after-replay"
	_ = startG54Volume(t, bins, art, opts)
	waitHealthyReplica(t, statusAddr, "r1", 20*time.Second)
	waitProductIscsiReady(t, iscsiAddr, 5*time.Second)

	cli2 := dialG8Iscsi(t, iscsiAddr, iqn)
	defer cli2.close(t)
	if got := cli2.read10(t, acked+afterBy, 1, bs); !bytes.Equal(got, base) {
		t.Fatalf("the block synced before the measured writes is gone: the store did not survive the replay")
	}
	lost := 0
	first := -1
	for lba := uint32(0); lba < acked; lba++ {
		if got := cli2.read10(t, lba, 1, bs); !bytes.Equal(got, want[lba]) {
			if first < 0 {
				first = int(lba)
			}
			lost++
		}
	}
	after := 0
	for lba := uint32(acked); lba < acked+afterBy; lba++ {
		if bytes.Equal(cli2.read10(t, lba, 1, bs), block(lba, 2)) {
			after++
		}
	}
	t.Logf("drop_sync=%v: acked blocks lost %d of %d (first %d); post-mark blocks present %d of %d",
		dropSync, lost, acked, first, after, afterBy)
	if after != 0 {
		t.Fatalf("%d of %d post-mark blocks survived: the replay did not stop at the mark", after, afterBy)
	}
	if dropSync {
		if lost == 0 {
			t.Fatalf("CONTROL_NOT_RED: no block lost without SYNCHRONIZE CACHE; the replay cannot show a missing sync")
		}
		fmt.Printf("REPLAY_control_RED lost=%d of %d first=%d\n", lost, acked, first)
		return
	}
	if lost > 0 {
		t.Fatalf("REPLAY_BYTES_MISMATCH volume: %d of %d acknowledged blocks lost after power-loss replay (first LBA %d)",
			lost, acked, first)
	}
	fmt.Printf("REPLAY_volume_OK acked=%d post_mark_present=%d\n", acked, after)
}

func powerLossBins(t *testing.T) l2bins {
	t.Helper()
	if d := os.Getenv("SWBLOCK_TEST_BINS"); d != "" {
		b := l2bins{master: filepath.Join(d, "blockmaster"), volume: filepath.Join(d, "blockvolume")}
		for _, p := range []string{b.master, b.volume} {
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("prebuilt binary: %v", err)
			}
		}
		return b
	}
	return buildG54Binaries(t)
}

func runReplayCut(t *testing.T, cut string, step ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, cut, step...)
	cmd.WaitDelay = 5 * time.Second // a hung child keeping the pipe open does not outlive the kill
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("replay harness %s: %v\n%s", step, err, out)
	}
	t.Logf("replay harness %s: %s", step, bytes.TrimSpace(out))
}
