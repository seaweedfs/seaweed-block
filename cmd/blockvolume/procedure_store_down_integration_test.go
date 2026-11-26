//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// The SeaweedFS side down during a cut, then back,
// with real binaries: image B is written and a cut starts; once
// its uploads have begun, the filer (or the volume server) is killed. For a
// minute the guest keeps writing and flushing over NVMe/TCP while the test
// records how the cut answers and how much copy-on-write side data the
// stalled cut keeps on the local root. Then the store comes back.
//
// The store goes down inside the cut by construction: a filer front holds
// the reply to the cut's first applied chunk-object create, the store is
// stopped while that reply is held, then the reply is released. The cut
// cannot commit while one of its uploads is unanswered. With
// E1_TEST_SKIP_BOUNDARY=1 the front holds nothing and the test fails as
// inconclusive (the control).
//
// Asserted, in every variant: the store goes down mid-cut (uploads begun,
// HEAD unmoved); the guest keeps writing with no error and every command
// answers within storeDownBound; the cut the outage hit fails; after the
// store returns a later cut moves HEAD and verifies at depth data; no COW
// side file is left; a snapshot restored into a new volume is the last
// flushed image (each pass writes new bytes). Recorded for the operator
// guide: the slowest answer, when and how the stalled cut answered, the peak
// COW side bytes.
//
// Runs 4 variants of about 2-3 minutes: go test -timeout 30m.
const storeDownBound = 20 * time.Second

func TestProcedureStoreDownDuringACut(t *testing.T) {
	for _, down := range []string{"filer", "volume"} {
		t.Run(down, func(t *testing.T) { storeDownDuringACut(t, down, 0, false) })
	}
	// 4 MiB: the reserve's half holds no 4 MiB chunk, so stage() uploads the
	// whole dirty set before the cut point and opens no copy-on-write window;
	// with the store down that upload fails. No local space, guest undisturbed.
	t.Run("volume-cow-reserve-exceeded", func(t *testing.T) { storeDownDuringACut(t, "volume", 4<<20, false) })
	// 400 MiB: the 48 captured 4 MiB chunks fit in half of it, so the cut
	// opens its copy-on-write window; asserted to stay within reserve/2 and
	// to close when the cut fails.
	t.Run("volume-cow-window-400MiB", func(t *testing.T) { storeDownDuringACut(t, "volume", 400<<20, true) })
}

func storeDownDuringACut(t *testing.T, down string, cowBytes int64, cowWindow bool) {
	if testListenHost() != "127.0.0.1" {
		t.Fatal("the causal filer front requires loopback")
	}
	var boundary *snapshotFilerBoundary
	cluster := startWorkspaceWeedWith(t, t.TempDir(), w4WeedOpts{VolumeSizeLimitMB: 256, VolumeMax: 32,
		FilerBind: "127.0.0.2", FilerFront: func(c *w4WeedCluster) { boundary = newSnapshotFilerBoundary(t, c) }})
	env := startHandoffEnvOnCluster(t, 1, []string{"v2"}, procedureTLSConfig(), false, cluster)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	if cowBytes > 0 {
		// Not under a temporary directory: the host-limit launch check refuses one.
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		dir, err := os.MkdirTemp(home, ".swblock-test-host-limit-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		env.volExtra = append(env.volExtra, "--host-limit-dir", dir, "--host-limit-cow-bytes", fmt.Sprint(cowBytes))
	}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	id, err := layout.ParseVolumeID(env.layoutID("v1"))
	if err != nil {
		t.Fatal(err)
	}
	boundary.id = id
	boundary.dir = path.Join(env.chunkPath, layout.VolumeDir("v1", id))
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
	sideBytes := func() int64 {
		files, _ := filepath.Glob(filepath.Join(env.hosts["s1"].root, "*.cut-cow-*"))
		var n int64
		for _, f := range files {
			if st, err := os.Stat(f); err == nil {
				n += st.Size()
			}
		}
		return n
	}

	env.writeCutCrashImage(t, 'A')
	env.h.command(t, cut...)
	env.writeCutCrashImage(t, 'B')
	want := map[uint64][]byte{}
	for ch := 0; ch < cutCrashChunks; ch++ {
		for i := 0; i < cutCrashPer; i++ {
			lba := uint64(ch*ladderChunkBlocks + i)
			want[lba] = cutCrashBlock('B', lba)
		}
	}

	// A cut starts; the store goes down while the reply to its first applied
	// chunk-object create is held, so before the cut can commit.
	gen := headGen()
	reached := boundary.arm("OBJECT", "")
	type cutAnswer struct {
		rc  int
		out string
		at  time.Duration
	}
	answered := make(chan cutAnswer, 1)
	cutStart := time.Now()
	cutCtx, cutCancel := context.WithTimeout(context.Background(), 8*time.Minute)
	cutDone := make(chan struct{})
	// Off the test goroutine: runCLI never fails the test, and cleanup waits
	// for it so nothing outlives the test.
	go func() {
		defer close(cutDone)
		out, errOut, rc, err := runCLI(cutCtx, env.h, cut...)
		if err != nil {
			rc, errOut = -1, errOut+err.Error()
		}
		answered <- cutAnswer{rc, out + errOut, time.Since(cutStart)}
	}()
	t.Cleanup(func() { boundary.unblock(); cutCancel(); <-cutDone })
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: inconclusive: the cut applied no chunk-object create within 30s, so the store would not go down mid-cut", down)
	}
	if g := headGen(); g != gen {
		t.Fatalf("%s: the cut committed (HEAD gen %d -> %d) while one of its uploads was unanswered", down, gen, g)
	}
	env.cluster.stopProc(t, down)
	boundary.unblock()
	downAt := time.Now()
	t.Logf("%s killed %s after the cut started", down, downAt.Sub(cutStart).Round(time.Millisecond))

	// A minute of guest writes and flushes while the store is down.
	c := dialNVMe(t, env.hosts["s1"].nvme, plannedHandoffNQN)
	var pending []uint64
	var errs int
	var slowest time.Duration
	var cutWhileDown *cutAnswer
	peakSide := sideBytes()
	op := func(f func() error) error {
		start := time.Now()
		_ = c.io.SetDeadline(start.Add(storeDownBound))
		err := f()
		if d := time.Since(start); d > slowest {
			slowest = d
		}
		var ne interface{ Timeout() bool }
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("%s down: a guest command did not answer within %s: %v", down, storeDownBound, err)
		}
		return err
	}
	written := 0
	pendingBytes := map[uint64][]byte{}
	for n := 0; time.Since(downAt) < time.Minute; n++ {
		ch, i := (n/cutCrashPer)%cutCrashChunks, n%cutCrashPer
		lba := uint64(ch*ladderChunkBlocks + i)
		// Each pass over the image writes new bytes ('C', 'D', ...), so a lost
		// tail of rewrites is visible at the read-back.
		b := cutCrashBlock(byte('C'+n/(cutCrashChunks*cutCrashPer)), lba)
		if err := op(func() error { return c.tryWrite(lba, b) }); err != nil {
			errs++
			continue
		}
		written++
		pending = append(pending, lba)
		pendingBytes[lba] = b
		if len(pending) == 8 {
			if err := op(c.tryFlush); err != nil {
				errs++
			} else {
				for _, l := range pending {
					want[l] = pendingBytes[l]
				}
			}
			pending = pending[:0]
			clear(pendingBytes)
		}
		if s := sideBytes(); s > peakSide {
			peakSide = s
		}
		if cutWhileDown == nil {
			select {
			case a := <-answered:
				cutWhileDown = &a
			default:
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The last partial batch is flushed too, so every write is acknowledged
	// and the read-back can name each block's bytes.
	if len(pending) > 0 {
		if err := op(c.tryFlush); err != nil {
			errs++
		} else {
			for _, l := range pending {
				want[l] = pendingBytes[l]
			}
		}
	}
	c.close()
	t.Logf("%s down 60s (COW reserve %d): guest writes %d, errors %d, slowest answer %s, frontend ready %v, peak COW side bytes %d",
		down, cowBytes, written, errs, slowest.Round(time.Millisecond), env.frontendReadyAt("s1"), peakSide)
	// Guest I/O is not delayed by the outage, in every variant.
	if written == 0 || errs != 0 || slowest >= storeDownBound {
		t.Fatalf("%s down: the guest was disturbed: %d writes, %d errors, slowest answer %s", down, written, errs, slowest)
	}
	if cutWhileDown != nil {
		t.Logf("%s down: the cut answered while it was down, after %s, exit %d: %s", down, cutWhileDown.at.Round(time.Millisecond), cutWhileDown.rc, firstLine(cutWhileDown.out))
	} else {
		t.Logf("%s down: the cut had not answered after %s", down, time.Since(cutStart).Round(time.Second))
	}
	if cowBytes > 0 && !cowWindow {
		// A dirty set larger than half the reserve is staged before the cut
		// point: the cut fails in its upload, with no copy-on-write window
		// and no local space.
		if peakSide != 0 || (cutWhileDown != nil && strings.Contains(cutWhileDown.out, "capture copy")) {
			t.Fatalf("%s down: a staged cut used local copy-on-write space (peak %d) or opened a window: %+v", down, peakSide, cutWhileDown)
		}
	}
	if cowWindow {
		// The reserve bounds the copy-on-write window: stage() opens it only
		// when the captured chunks fit in half the reserve.
		switch {
		case cutWhileDown == nil || !strings.Contains(cutWhileDown.out, "capture copy"):
			t.Fatalf("%s down: the cut did not fail inside a copy-on-write window (no 'capture copy'): %+v", down, cutWhileDown)
		case peakSide == 0 || peakSide > cowBytes/2:
			t.Fatalf("%s down: peak COW side bytes %d, want > 0 and <= half the %d reserve", down, peakSide, cowBytes)
		case sideBytes() != 0:
			t.Fatalf("%s down: %d COW side bytes left after the cut failed: the window did not close", down, sideBytes())
		case written == 0:
			t.Fatalf("%s down: the guest wrote nothing while the window was open", down)
		}
		t.Logf("%s down: COW window measured: peak %d <= reserve/2 %d, closed on the failure", down, peakSide, cowBytes/2)
	}

	// The store comes back.
	env.cluster.restartProc(t, down)
	backAt := time.Now()
	if down == "filer" {
		w4WaitTCP(t, env.cluster.filerGRPC, 30*time.Second)
	}
	stalled := cutWhileDown
	if stalled == nil {
		select {
		case a := <-answered:
			stalled = &a
			t.Logf("%s back: the stalled cut answered %s after the restart, exit %d: %s", down, time.Since(backAt).Round(time.Millisecond), a.rc, firstLine(a.out))
		case <-time.After(5 * time.Minute):
			t.Fatalf("%s back: the cut started before the outage never answered", down)
		}
	}
	// The cut the outage hit must have failed; HEAD is read again after its
	// answer, so only a later cut can satisfy the check below.
	if stalled.rc == 0 {
		t.Fatalf("%s: the cut the outage hit reported success: %s", down, firstLine(stalled.out))
	}
	gen = headGen()
	var g uint64
	for end := time.Now().Add(2 * time.Minute); ; time.Sleep(2 * time.Second) {
		_, _, rc := env.h.run(t, cut...)
		if g = headGen(); rc == 0 && g > gen {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("%s back: no cut committed within 2m (HEAD gen %d, was %d)", down, g, gen)
		}
	}
	t.Logf("%s back: a cut committed %s after the restart (HEAD gen %d -> %d)", down, time.Since(backAt).Round(time.Millisecond), gen, g)
	if out, errOut, rc := env.h.run(t, verify...); rc != 0 {
		t.Fatalf("%s back: HEAD does not verify (exit %d):\n%s\n%s", down, rc, out, errOut)
	}
	if s := sideBytes(); s != 0 {
		t.Fatalf("%s back: %d COW side bytes left on the local root after a committed cut", down, s)
	}
	env.snapshot(t, "s1", "v1", "after-"+down+"-down")
	env.restoreInto(t, "v1", "after-"+down+"-down", "v2")
	for _, host := range []string{"s1", "n-v2"} {
		c := dialNVMe(t, env.hosts[host].nvme, plannedHandoffNQN)
		for lba, w := range want {
			if got := c.read(t, lba, 4096); !bytes.Equal(got, w) {
				c.close()
				t.Fatalf("%s back: %s LBA %d is not the last flushed write", down, host, lba)
			}
		}
		c.close()
	}
	fmt.Printf("STORE_DOWN_OK %s\n", down)
}

func firstLine(s string) string {
	if i := bytes.IndexByte([]byte(s), '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
