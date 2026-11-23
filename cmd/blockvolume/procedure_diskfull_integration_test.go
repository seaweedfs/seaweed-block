//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Local disk full (V1-SPEC r2.55 round 2), with real binaries: blockmaster, a
// real filer (weed, its data elsewhere) and walstore-chunk blockvolumes whose
// durable roots and --host-limit-dir are on small filesystems the harness
// mounts (cmd/blockvolume/testdata/diskfull.sh). A filler file takes the free
// space; deleting it frees it.
//
//  1. The WAL's disk fills under guest I/O: every guest command answers within
//     a bound (an error is allowed, a hang is not); after the space is freed
//     and the holder restarts, every flush-acknowledged block reads back and
//     the next cut moves HEAD and verifies.
//  2. A new volume's durable root is full: blockvolume exits within a bound and
//     says why; after the space is freed it starts and serves.
//  3. The --host-limit-dir disk is full: a cut answers within a bound and guest
//     I/O keeps working meanwhile; after the space is freed a cut commits.
//
// Environment: SWBLOCK_DISKFULL_ROOT and SWBLOCK_DISKFULL_LIMIT (two mounted
// filesystems, nothing else on them); CHUNKSTORE_WEED; optional
// SWBLOCK_TEST_BINS and SWBLOCK_REPLAY_LOGS (logs are copied there).
const diskFullBound = 30 * time.Second

func TestProcedureLocalDiskFull(t *testing.T) {
	root, limit := os.Getenv("SWBLOCK_DISKFULL_ROOT"), os.Getenv("SWBLOCK_DISKFULL_LIMIT")
	if root == "" && limit == "" {
		t.Skip("requires the disk-full harness (two small mounted filesystems)")
	}
	if root == "" || limit == "" {
		t.Fatalf("SWBLOCK_DISKFULL_ROOT=%q and SWBLOCK_DISKFULL_LIMIT=%q must be set together", root, limit)
	}
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	if d := os.Getenv("SWBLOCK_REPLAY_LOGS"); d != "" {
		t.Cleanup(func() { copyLogs(t, env.h.dir, d) })
	}
	for id, dir := range map[string]string{"s1": "vol", "n-v2": "vol-v2"} {
		h := env.hosts[id]
		h.root = filepath.Join(root, dir)
		env.hosts[id] = h
	}
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	cut := []string{"ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status}
	verify := []string{"ops", "chunk", "verify", "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath, "--depth", "data", "--timeout", "5m"}
	headGen := func() uint64 {
		out, _ := env.h.command(t, env.chunkShow()...)
		hd, err := parseHead(out)
		if err != nil {
			t.Fatal(err)
		}
		return hd.Gen
	}
	cutAdvances := func(what string) {
		t.Helper()
		g0 := headGen()
		env.h.command(t, cut...)
		if g := headGen(); g <= g0 {
			t.Fatalf("%s: the cut left HEAD at gen %d (was %d)", what, g, g0)
		}
		if out, errOut, rc := env.h.run(t, verify...); rc != 0 {
			t.Fatalf("%s: HEAD does not verify (exit %d):\n%s\n%s", what, rc, out, errOut)
		}
	}

	holder := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeCutCrashImage(t, 'A')
	cutAdvances("before the disk fills")

	// 1. The WAL's disk fills under guest I/O.
	fill := fillDisk(t, root, 2<<20)
	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	want := map[uint64][]byte{}
	var pending []uint64
	var firstErr error
	const maxWrites = 8192 // 32 MiB, far more than the 2 MiB left
	for i := 0; i < maxWrites && firstErr == nil; i++ {
		lba := uint64(i)
		b := cutCrashBlock('W', lba)
		if err := diskFullOp(c, func() error { return c.tryWrite(lba, b) }); err != nil {
			firstErr = fmt.Errorf("write %d: %w", i, err)
			break
		}
		pending = append(pending, lba)
		if (i+1)%8 == 0 {
			if err := diskFullOp(c, c.tryFlush); err != nil {
				firstErr = fmt.Errorf("flush after write %d: %w", i, err)
				break
			}
			for _, l := range pending {
				want[l] = cutCrashBlock('W', l)
			}
			pending = pending[:0]
		}
	}
	if errors.Is(firstErr, errDiskFullHang) {
		t.Fatalf("case 1: a guest command did not answer within %s on a full disk: %v", diskFullBound, firstErr)
	}
	// INV-FE-ERROR-RETRY-CLASS-001: ENOSPC is a permanent cause, so NVMe
	// Generic/Internal Error with DNR (0x800c); a WAL pressure timeout is the
	// documented retryable Namespace Not Ready, DNR=0 (0x0104). Anything else
	// (a generic transport error, another code) is not the documented answer.
	if firstErr != nil && !strings.Contains(firstErr.Error(), "status 0x800c") && !strings.Contains(firstErr.Error(), "status 0x0104") {
		t.Fatalf("case 1: the guest saw %v, not a documented completion for a full disk", firstErr)
	}
	t.Logf("case 1: %d flush-acknowledged writes, %d unflushed; first error: %v; frontend ready: %v",
		len(want), len(pending), firstErr, env.frontendReadyAt("s1"))
	c.close()
	fill.free(t)
	holder.kill(t)
	holder = env.start(t, "blockvolume-v1-after-full-wal", "s1")
	if got := env.adopt(t, holder, "s1"); got != e {
		t.Fatalf("case 1: the restarted holder adopted epoch %d; want %d", got, e)
	}
	func() {
		c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
		defer c.close()
		for ch := 0; ch < cutCrashChunks; ch++ {
			for i := 0; i < cutCrashPer; i++ {
				lba := uint64(ch*ladderChunkBlocks + i)
				got := c.read(t, lba, 4096)
				if w, ok := want[lba]; ok {
					if !bytes.Equal(got, w) {
						t.Fatalf("case 1: flush-acknowledged LBA %d lost after the disk filled", lba)
					}
				} else if !bytes.Equal(got, cutCrashBlock('A', lba)) && !bytes.Equal(got, cutCrashBlock('W', lba)) {
					t.Fatalf("case 1: LBA %d reads neither image A nor its unflushed write", lba)
				}
			}
		}
	}()
	cutAdvances("case 1, after the space is freed")

	// 2. A new volume's durable root is full.
	fill = fillDisk(t, root, 0)
	v2 := env.start(t, "blockvolume-v2-full", "n-v2")
	err := v2.wait(diskFullBound)
	if err == nil || strings.Contains(err.Error(), "still running") {
		t.Fatalf("case 2: blockvolume on a full durable root: %v (want an exit with a reason within %s)", err, diskFullBound)
	}
	if !v2.has("no space left on device") {
		t.Fatalf("case 2: blockvolume exited (%v) without naming the full disk; log %s", err, v2.log)
	}
	fill.free(t)
	v2 = env.start(t, "blockvolume-v2", "n-v2")
	v2.waitLog(t, 60*time.Second, "blockvolume: volume v2 adopted as PRIMARY at r1@")
	t.Logf("case 2: exited on the full root (%v), then started and adopted v2", err)

	// 3. The --host-limit-dir disk is full.
	holder.term(t, 30*time.Second)
	slots := filepath.Join(limit, "slots")
	if err := os.Mkdir(slots, 0o700); err != nil {
		t.Fatal(err)
	}
	fill = fillDisk(t, limit, 0)
	env.volExtra = append(env.volExtra, "--host-limit-dir", slots)
	holder = env.start(t, "blockvolume-v1-full-limit-dir", "s1")
	if got := env.adopt(t, holder, "s1"); got != e {
		t.Fatalf("case 3: the holder adopted epoch %d; want %d", got, e)
	}
	env.writeCutCrashImage(t, 'B')
	type cutResult struct {
		out string
		rc  int
	}
	done := make(chan cutResult, 1)
	g0 := headGen()
	go func() {
		out, errOut, rc := env.h.run(t, cut...)
		done <- cutResult{out + errOut, rc}
	}()
	func() {
		c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
		defer c.close()
		lba := uint64(cutCrashChunks * ladderChunkBlocks)
		if err := diskFullOp(c, func() error { return c.tryWrite(lba, cutCrashBlock('L', lba)) }); err != nil {
			t.Fatalf("case 3: guest write with the host-limit dir full: %v", err)
		}
		if err := diskFullOp(c, c.tryFlush); err != nil {
			t.Fatalf("case 3: guest flush with the host-limit dir full: %v", err)
		}
	}()
	var r cutResult
	select {
	case r = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatalf("case 3: a cut with the host-limit dir full did not answer within 2m")
	}
	// The docs name no outcome for a full --host-limit-dir (the launch check,
	// INV-HOST-LAUNCH-CHECK-001, only reads it). A definite answer either way
	// is accepted: a refusal naming the full disk, or a commit that moved HEAD.
	switch g := headGen(); {
	case r.rc != 0 && !strings.Contains(r.out, "no space left on device"):
		t.Fatalf("case 3: the cut failed (exit %d) without naming the full disk:\n%s", r.rc, r.out)
	case r.rc == 0 && g <= g0:
		t.Fatalf("case 3: the cut exited 0 but HEAD stayed at gen %d", g)
	}
	t.Logf("case 3: cut with the host-limit dir full exited %d; guest write+flush succeeded meanwhile", r.rc)
	fill.free(t)
	cutAdvances("case 3, after the space is freed")
	env.requireCutCrashImage(t, "s1", 'B')
	fmt.Println("DISKFULL_OK")
}

var errDiskFullHang = errors.New("no answer within the bound")

// diskFullOp runs op with a deadline on the I/O queue; a timeout is a hang.
func diskFullOp(c *nvmeInitiator, op func() error) error {
	_ = c.io.SetDeadline(time.Now().Add(diskFullBound))
	err := op()
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("%w: %v", errDiskFullHang, err)
	}
	return err
}

type diskFiller struct{ path string }

// fillDisk writes a filler file on dir's filesystem until it is full, then
// shrinks it to leave about keep bytes free.
func fillDisk(t *testing.T, dir string, keep int64) diskFiller {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("filler-%d", time.Now().UnixNano()))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1<<20)
	var n int64
	for {
		w, err := f.Write(buf)
		n += int64(w)
		if err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("filler: %v", err)
			}
			break
		}
	}
	if keep > 0 {
		if err := f.Truncate(max(n-keep, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil && !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}
	var st syscall.Statfs_t
	_ = syscall.Statfs(dir, &st)
	t.Logf("filled %s: filler %d MiB, %d KiB free", dir, n>>20, int64(st.Bavail)*int64(st.Bsize)>>10)
	return diskFiller{path: path}
}

func (d diskFiller) free(t *testing.T) {
	t.Helper()
	if err := os.Remove(d.path); err != nil {
		t.Fatal(err)
	}
}
