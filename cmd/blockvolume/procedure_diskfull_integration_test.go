//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Local disk full, with real binaries: blockmaster, a
// real filer (weed, its data elsewhere) and walstore-chunk blockvolumes whose
// durable roots and --host-limit-dir are on small filesystems the harness
// mounts (cmd/blockvolume/testdata/diskfull.sh). A filler file takes the free
// space; deleting it frees it.
//
//  1. The WAL's disk fills under guest I/O: every guest command answers within
//     the bound; the first error is the retryable 0x0104 that 1.0 documents
//     (a Linux guest then fails the write with EREMOTEIO after about 75 s;
//     the permanent class is #484, after GO); image A stays readable; once
//     the space is freed, writes succeed without a restart.
//     1b. The holder restarts with the disk still full: it serves reads or
//     exits naming the full disk. After the space is freed and a restart,
//     every flush-acknowledged block reads back and the next cut moves HEAD
//     and verifies.
//  2. A new volume's durable root is full: blockvolume refuses or exits and
//     says why; after the space is freed it starts over what is left and
//     serves.
//  3. The --host-limit-dir disk is full: a cut is refused naming the full
//     disk (background work fails closed, as ruled for #485) and detach
//     --plain, the urgent seal, still seals (#486); guest I/O works meanwhile.
//
// Each filed defect is a known-finding row (diskFullFinding): while it
// reproduces the row logs KNOWN FINDING #n and passes; once fixed the row
// fails with FIXED #n until the issue is removed from knownDiskFullFindings,
// which makes it a strict assertion. A defect not in the list fails the test.
//
// Environment: SWBLOCK_DISKFULL_ROOT and SWBLOCK_DISKFULL_LIMIT (two mounted
// filesystems, nothing else on them); CHUNKSTORE_WEED; optional
// SWBLOCK_TEST_BINS and SWBLOCK_REPLAY_LOGS (logs are copied there).
//
// diskFullBound stays below the Linux NVMe host default I/O timeout (30 s)
// with margin: an answer at 29 s would still make a real guest reset the
// controller.
const diskFullBound = 20 * time.Second

func TestProcedureLocalDiskFull(t *testing.T) {
	root, limit := os.Getenv("SWBLOCK_DISKFULL_ROOT"), os.Getenv("SWBLOCK_DISKFULL_LIMIT")
	if root == "" && limit == "" {
		t.Skip("requires the disk-full harness (two small mounted filesystems)")
	}
	if root == "" || limit == "" {
		t.Fatalf("SWBLOCK_DISKFULL_ROOT=%q and SWBLOCK_DISKFULL_LIMIT=%q must be set together", root, limit)
	}
	for _, m := range []string{root, limit} {
		requireSmallDedicatedMount(t, m)
	}
	env := startHandoffEnvVolumes(t, 1, []string{"v2"})
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
		hd, err := parseLayoutHead(out)
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

	// 1. The WAL's disk fills under guest I/O. LBAs 0.. are written in order
	// (chunks 0-31 at most); image A in chunks 40-47 is never overwritten here.
	fill := fillDisk(t, root, 2<<20)
	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	want := map[uint64][]byte{}
	var pending []uint64
	next := 0
	writeUntilError := func(n int) error {
		for end := next + n; next < end; next++ {
			lba := uint64(next)
			b := cutCrashBlock('W', lba)
			if err := diskFullOp(c, func() error { return c.tryWrite(lba, b) }); err != nil {
				return fmt.Errorf("write %d: %w", next, err)
			}
			pending = append(pending, lba)
			if (next+1)%8 == 0 {
				if err := diskFullOp(c, c.tryFlush); err != nil {
					return fmt.Errorf("flush after write %d: %w", next, err)
				}
				for _, l := range pending {
					want[l] = cutCrashBlock('W', l)
				}
				pending = pending[:0]
			}
		}
		return nil
	}
	// 128 MiB: more than the 2 MiB left and more than the preallocated WAL, so
	// the WAL fills too when the flusher cannot write extents.
	firstErr := writeUntilError(32768)
	if errors.Is(firstErr, errDiskFullHang) {
		t.Fatalf("case 1: a guest command did not answer within %s on a full disk: %v", diskFullBound, firstErr)
	}
	// INV-FE-ERROR-RETRY-CLASS-001: ENOSPC is a permanent cause, so NVMe
	// Generic/Internal Error with DNR (0x800c). Namespace Not Ready, DNR=0
	// (0x0104) is right only for WAL pressure a cut relieves; with ENOSPC
	// behind it a guest would retry for ever: a wrong class, reported.
	switch {
	case firstErr == nil:
		t.Fatalf("case 1: no write failed in %d writes with 2 MiB free: the disk never filled, so this case tested nothing", next)
	case strings.Contains(firstErr.Error(), "status 0x800c"):
		diskFullFinding(t, 484, false, "case 1: the guest saw the permanent 0x800c for ENOSPC")
	case strings.Contains(firstErr.Error(), "status 0x0104"):
		if holder.has("no space left on device") {
			diskFullFinding(t, 484, true, "case 1: the guest saw retryable 0x0104 (%v) while the holder logs ENOSPC: a permanent cause surfaced as retryable", firstErr)
		} else {
			diskFullFinding(t, 484, false, "case 1: 0x0104 with no ENOSPC behind it")
		}
	default:
		t.Fatalf("case 1: the guest saw %v, not a documented completion for a full disk", firstErr)
	}
	t.Logf("case 1: %d flush-acknowledged writes, %d unflushed; first error: %v; frontend ready: %v",
		len(want), len(pending), firstErr, env.frontendReadyAt("s1"))
	// A full disk never makes existing data unreadable.
	readImageA := func(what string, c *nvmeInitiator) {
		t.Helper()
		for ch := 40; ch < cutCrashChunks; ch++ {
			for i := 0; i < cutCrashPer; i += 16 {
				lba := uint64(ch*ladderChunkBlocks + i)
				var got []byte
				err := diskFullOp(c, func() (err error) { got, err = c.tryRead(lba, 4096); return err })
				if err != nil || !bytes.Equal(got, cutCrashBlock('A', lba)) {
					t.Fatalf("%s: image A at LBA %d is not readable on a full disk: %v", what, lba, err)
				}
			}
		}
	}
	readImageA("case 1, after the first write error", c)
	// Operator guide: once space is freed, new writes succeed without a
	// restart (on Linux the writes made while full failed with EREMOTEIO).
	fill.free(t)
	if err := writeUntilError(16); err != nil {
		t.Fatalf("case 1: after freeing the space, without a restart, writes still fail: %v", err)
	}
	c.close()

	// 1b. Restart while the disk is still full: serve (reads at least) or exit
	// within the bound naming the full disk; never damage acknowledged data.
	fill = fillDisk(t, root, 0)
	holder.kill(t)
	requireDiskFull(t, root, "case 1b, at the restart")
	holder = env.start(t, "blockvolume-v1-restart-full", "s1")
	served := false
	for end := time.Now().Add(60 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		if holder.has("blockvolume: volume v1 adopted as PRIMARY at r1@") {
			served = true
			break
		}
		exited := false
		select {
		case <-holder.done:
			exited = true
		default:
		}
		if exited {
			if !holder.has("no space left on device") {
				t.Fatalf("case 1b: the holder exited on a full disk without naming it (%v); log %s", holder.err, holder.log)
			}
			break
		}
		if time.Now().After(end) {
			t.Fatalf("case 1b: the holder neither served nor exited within 60s on a full disk")
		}
	}
	if served {
		env.h.waitPrimaryAt(t, env.hosts["s1"].status, e)
		c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
		readImageA("case 1b, restarted on a full disk", c)
		c.close()
		t.Logf("case 1b: restarted on a full disk and serves reads")
	} else {
		t.Logf("case 1b: restart on a full disk exited naming the full disk")
	}
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
				_ = c.io.SetDeadline(time.Now().Add(diskFullBound))
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
		for lba, w := range want {
			_ = c.io.SetDeadline(time.Now().Add(diskFullBound))
			if !bytes.Equal(c.read(t, lba, 4096), w) {
				t.Fatalf("case 1: flush-acknowledged LBA %d lost after the disk filled", lba)
			}
		}
	}()
	cutAdvances("case 1, after the space is freed")

	// 2. A new volume's durable root is full. v1 stops first: its flusher
	// would take any space freed on the shared disk.
	holder.term(t, 30*time.Second)
	fill = fillDisk(t, root, 0)
	// Either answer names the full disk within the bound: an exit, or the
	// process stays up with its durable recovery faulted (status stays
	// available, readiness blocked) and never serves.
	v2 := env.start(t, "blockvolume-v2-full", "n-v2")
	outcome := ""
	for end := time.Now().Add(60 * time.Second); outcome == ""; time.Sleep(500 * time.Millisecond) {
		select {
		case <-v2.done:
			outcome = fmt.Sprintf("exited (%v)", v2.err)
		default:
		}
		switch {
		case outcome != "":
		case v2.has("durable recovery faulted"):
			outcome = "stayed up with durable recovery faulted"
		case v2.has("adopted as PRIMARY"):
			t.Fatalf("case 2: v2 adopted as primary on a full durable root")
		case time.Now().After(end):
			t.Fatalf("case 2: blockvolume on a full durable root neither exited nor faulted within 60s; log %s", v2.log)
		}
	}
	// The headroom check refuses the create first (free space below the
	// volume size + WAL); without it the create fails with ENOSPC.
	if !v2.has("no space left on device") && !v2.has("bytes free; volume v2 needs") {
		t.Fatalf("case 2: blockvolume %s without naming the full disk; log %s", outcome, v2.log)
	}
	v2.kill(t)
	fill.free(t)
	// What the failed create left behind must not stop the next start.
	v2 = env.start(t, "blockvolume-v2", "n-v2")
	v2.waitLog(t, 60*time.Second, "blockvolume: volume v2 adopted as PRIMARY at r1@")
	t.Logf("case 2: on the full root blockvolume %s naming ENOSPC; after freeing, it started over the partial create and adopted v2", outcome)

	// 3. The --host-limit-dir disk is full.
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
	go func() { // off the test goroutine: no t.Fatal here
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		out, errOut, rc, err := runCLI(ctx, env.h, cut...)
		if err != nil {
			rc, errOut = -1, errOut+err.Error()
		}
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
	// Ruled (issue 485): background work, timer and explicit cuts, fails
	// closed when the limiter cannot run, and says why; HEAD stays.
	if g := headGen(); r.rc == 0 || g != g0 || !strings.Contains(r.out, "no space left on device") {
		t.Fatalf("case 3: with the host-limit dir full a cut must be refused naming the full disk; exit %d, HEAD gen %d (was %d):\n%s", r.rc, g, g0, r.out)
	}
	t.Logf("case 3: cut with the host-limit dir full exited %d; guest write+flush succeeded meanwhile", r.rc)
	// The seal of a planned hand-over must not depend on the limit directory.
	out, errOut, rc := env.h.run(t, "ops", "chunk", "detach", "--plain", "--volume", "v1", "--status-addr", env.hosts["s1"].status, "--timeout", "120s")
	sealed, sealErr := sealedGen(out, errOut, e)
	diskFullFinding(t, 486, rc != 0 || sealErr != nil || sealed <= g0,
		"case 3: detach --plain with the host-limit dir full exited %d, sealed gen %d (HEAD was %d, %v):\n%s\n%s", rc, sealed, g0, sealErr, out, errOut)
	if g := headGen(); rc == 0 && g != sealed {
		t.Fatalf("case 3: detach --plain reported gen %d but HEAD is at gen %d", sealed, g)
	}
	if o, eo, vrc := env.h.run(t, verify...); rc == 0 && vrc != 0 {
		t.Fatalf("case 3: the sealed HEAD does not verify (exit %d):\n%s\n%s", vrc, o, eo)
	}
	fill.free(t)
	t.Logf("slowest guest command answer: %s (bound %s)", diskFullSlowest, diskFullBound)
	if !t.Failed() {
		fmt.Println("DISKFULL_OK")
	}
}

// knownDiskFullFindings are the filed defects this test still expects.
var knownDiskFullFindings = map[int]bool{484: true} // 486 fixed by #487

// diskFullFinding is one defect row: bad reports whether issue's defect
// reproduced in this run.
func diskFullFinding(t *testing.T, issue int, bad bool, format string, args ...any) {
	t.Helper()
	fail, line := diskFullVerdict(knownDiskFullFindings, issue, bad, fmt.Sprintf(format, args...))
	switch {
	case fail:
		t.Error(line)
	case line != "":
		t.Log(line)
	}
}

var (
	errDiskFullHang = errors.New("no answer within the bound")
	diskFullSlowest time.Duration
)

// diskFullOp runs op with a deadline on the I/O queue; a timeout is a hang.
func diskFullOp(c *nvmeInitiator, op func() error) error {
	start := time.Now()
	_ = c.io.SetDeadline(start.Add(diskFullBound))
	err := op()
	if d := time.Since(start); d > diskFullSlowest {
		diskFullSlowest = d
	}
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
	var n int64
	// 1 MiB writes, then 4 KiB ones, so not even a small file still fits.
	for _, size := range []int{1 << 20, 4096} {
		buf := make([]byte, size)
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
	}
	if err := f.Sync(); err != nil && !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("filler sync: %v", err)
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
	if free := int64(st.Bavail) * int64(st.Bsize); free > keep+(1<<20) {
		t.Fatalf("filled %s but %d bytes are still free (want about %d)", dir, free, keep)
	}
	return diskFiller{path: path}
}

func (d diskFiller) free(t *testing.T) {
	t.Helper()
	if err := os.Remove(d.path); err != nil {
		t.Fatal(err)
	}
}

// requireDiskFull fails unless dir's file system has (almost) no free space.
func requireDiskFull(t *testing.T, dir, what string) {
	t.Helper()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	if free := int64(st.Bavail) * int64(st.Bsize); free > 64<<10 {
		t.Fatalf("%s: the disk is not full (%d bytes free)", what, free)
	}
}

// requireSmallDedicatedMount guards the fill: dir must be the root of its own
// small file system, so the filler can only fill the harness's mount.
func requireSmallDedicatedMount(t *testing.T, dir string) {
	t.Helper()
	var fs syscall.Statfs_t
	var self, parent syscall.Stat_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Stat(dir, &self); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Stat(filepath.Dir(dir), &parent); err != nil {
		t.Fatal(err)
	}
	if size := int64(fs.Blocks) * int64(fs.Bsize); self.Dev == parent.Dev || size > 2<<30 {
		t.Fatalf("%s must be a dedicated mount of at most 2 GiB (own device %v, size %d)", dir, self.Dev != parent.Dev, size)
	}
}
