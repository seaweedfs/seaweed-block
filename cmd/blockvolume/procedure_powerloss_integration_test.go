//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Power loss on a chunk volume (INV-FE-FLUSH-SYNC-001),
// with real binaries: blockmaster, a real filer (weed, its data off the
// replayed device) and a walstore-chunk blockvolume whose durable root is on
// a dm-log-writes device. Each power cut freezes the holder (SIGSTOP), so
// neither its disk nor the filer moves past the mark, marks the log, kills
// the holder, replays the log only up to the mark onto a fresh device (the
// harness puts a new log-writes layer on it and remounts it at the same
// path), restarts the holder and requires every flush-acknowledged block,
// then a cut that moves HEAD to a new generation and verifies at depth data;
// at the end a snapshot restored into a new volume is the last image. The phases:
//
//  1. image A flushed and cut (committed, WAL trimmed), then a power cut;
//  2. image B over the same LBAs, flushed, one more write without a flush,
//     then a power cut (image A in HEAD must not show through);
//  3. a new image flushed, a cut started, a power cut while it uploads (the
//     delay adapts until chunk objects reached the filer but HEAD did not move).
//
// Environment: SWBLOCK_REPLAY_ROOT and SWBLOCK_REPLAY_CUT as for
// TestPowerLossReplayAckedWritesSurvive; CHUNKSTORE_WEED; optional
// SWBLOCK_TEST_BINS and SWBLOCK_REPLAY_LOGS.
func TestPowerLossReplayChunkVolume(t *testing.T) {
	root, cutTool := os.Getenv("SWBLOCK_REPLAY_ROOT"), os.Getenv("SWBLOCK_REPLAY_CUT")
	if root == "" && cutTool == "" {
		t.Skip("requires the isolated dm-log-writes replay harness")
	}
	if root == "" || cutTool == "" {
		t.Fatalf("SWBLOCK_REPLAY_ROOT=%q and SWBLOCK_REPLAY_CUT=%q must be set together", root, cutTool)
	}
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	if d := os.Getenv("SWBLOCK_REPLAY_LOGS"); d != "" {
		t.Cleanup(func() { copyLogs(t, env.h.dir, d) })
	}
	h := env.hosts["s1"]
	h.root = filepath.Join(root, "vol")
	env.hosts["s1"] = h
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	cut := []string{"ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status}
	verify := []string{"ops", "chunk", "verify", "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath, "--depth", "data", "--timeout", "5m"}
	headGen := func() uint64 {
		out, _ := env.h.command(t, env.chunkShow()...)
		hd, err := parseLayoutHead(out)
		if err != nil {
			t.Fatal(err)
		}
		return hd.Gen
	}

	holder := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, holder, "s1")
	cuts := 0
	// powerCut ends at a restarted holder that serves every acknowledged block
	// of img, and a cut that verifies; with advance, that cut must move HEAD to
	// a new generation (a cut that uploads nothing would leave it). frozenGen,
	// when not 0, is the HEAD generation seen while frozen: it must still hold
	// after the kill, so no HEAD write from before the freeze landed later.
	powerCut := func(phase string, img byte, frozenGen uint64, advance bool) {
		t.Helper()
		if err := holder.proc.Signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("%s: freeze the holder: %v", phase, err)
		}
		runReplayCut(t, cutTool, "mark")
		holder.kill(t)
		if frozenGen != 0 {
			if g := headGen(); g != frozenGen {
				t.Fatalf("%s: HEAD moved from gen %d to %d after the freeze: the cut was not in progress at the mark", phase, frozenGen, g)
			}
		}
		runReplayCut(t, cutTool, "replay")
		cuts++
		holder = env.start(t, fmt.Sprintf("blockvolume-v1-after-cut-%d", cuts), "s1")
		if got := env.adopt(t, holder, "s1"); got != e {
			t.Fatalf("%s: the restarted holder adopted epoch %d; want %d", phase, got, e)
		}
		env.requireCutCrashImage(t, "s1", img)
		g0 := headGen()
		env.h.command(t, cut...)
		if g := headGen(); advance && g <= g0 {
			t.Fatalf("%s: the cut after the replay left HEAD at gen %d (was %d): the replayed writes were not published", phase, g, g0)
		}
		if out, errOut, rc := env.h.run(t, verify...); rc != 0 {
			t.Fatalf("%s: HEAD does not verify after the replay (exit %d):\n%s\n%s", phase, rc, out, errOut)
		}
		t.Logf("%s: power cut %d replayed; image %c read back; the next cut committed and verified", phase, cuts, img)
	}

	// Phase 1: a committed cut before the power cut.
	env.writeCutCrashImage(t, 'A')
	cutStart := time.Now()
	env.h.command(t, cut...)
	cutTook := time.Since(cutStart)
	powerCut("phase 1 (after a committed cut)", 'A', 0, false)

	// Phase 2: overwrite with B; then a write the guest never flushed.
	env.writeCutCrashImage(t, 'B')
	extra := uint64(cutCrashChunks * ladderChunkBlocks)
	before := func() []byte {
		c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
		defer c.close()
		return c.read(t, extra, 4096)
	}()
	func() {
		c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
		defer c.close()
		c.write(t, extra, cutCrashBlock('x', extra))
	}()
	powerCut("phase 2 (between a write and its flush)", 'B', 0, true)
	func() {
		c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
		defer c.close()
		got := c.read(t, extra, 4096)
		if !bytes.Equal(got, before) && !bytes.Equal(got, cutCrashBlock('x', extra)) {
			t.Fatalf("phase 2: the unflushed block reads neither its old nor its new content")
		}
	}()

	// Phase 3: a power cut while a cut uploads.
	delay := cutTook / 2
	img, landed := byte('B'), false
	for attempt := 1; attempt <= cutCrashAttempts && !landed; attempt++ {
		img++
		env.writeCutCrashImage(t, img)
		gen, entries := headGen(), f1FilerEntries(t, env)
		done := make(chan int, 1)
		go func() {
			_, _, code := env.h.run(t, cut...)
			done <- code
		}()
		time.Sleep(delay)
		if err := holder.proc.Signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("phase 3: freeze the holder: %v", err)
		}
		time.Sleep(time.Second) // a HEAD write already on the wire lands before the reading
		uploaded, after := f1FilerEntries(t, env)-entries, headGen()
		switch {
		case after != gen:
			t.Logf("phase 3 attempt %d: the cut committed before %s (HEAD gen %d -> %d); shorter", attempt, delay, gen, after)
			delay /= 2
		case uploaded <= 0:
			t.Logf("phase 3 attempt %d: nothing uploaded by %s; longer", attempt, delay)
			delay *= 2
		default:
			landed = true
			t.Logf("phase 3 attempt %d: frozen at %s mid-upload: %d new filer entries, HEAD still gen %d", attempt, delay, uploaded, gen)
		}
		if landed {
			// Still frozen mid-upload: the power cut starts from here (its
			// SIGSTOP is a no-op on a stopped process).
			powerCut(fmt.Sprintf("phase 3 attempt %d (cut in progress)", attempt), img, gen, true)
			<-done
			break
		}
		if err := holder.proc.Signal(syscall.SIGCONT); err != nil {
			t.Fatalf("phase 3: resume: %v", err)
		}
		<-done
	}
	if !landed {
		t.Fatalf("no power cut landed while a cut uploaded, in %d attempts", cutCrashAttempts)
	}
	// What the filer holds is the image: a snapshot restored into a new volume.
	env.snapshot(t, "s1", "v1", "after-power-cuts")
	env.restoreInto(t, "v1", "after-power-cuts", "v2")
	env.requireCutCrashImage(t, "n-v2", img)
	fmt.Printf("REPLAY_chunk_OK power_cuts=%d last_image=%c\n", cuts, img)
}

func copyLogs(t *testing.T, from, to string) {
	logs, _ := filepath.Glob(filepath.Join(from, "*.log"))
	for _, l := range logs {
		if b, err := os.ReadFile(l); err == nil {
			_ = os.WriteFile(filepath.Join(to, filepath.Base(l)), b, 0o644)
		}
	}
}
