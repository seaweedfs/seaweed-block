//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The SeaweedFS side down during a cut, then back (V1-SPEC r2.61 addendum,
// round 2), with real binaries: image B is written and a cut starts; once
// its uploads have begun, the filer (or the volume server) is killed. For a
// minute the guest keeps writing and flushing over NVMe/TCP while the test
// records how the cut answers and how much copy-on-write side data the
// stalled cut keeps on the local root. Then the store comes back.
//
// Asserted: every guest command answers within storeDownBound and a guest
// write that was flushed is never lost; the cut answers (an error is
// allowed) once the store is back at the latest; the next cut moves HEAD and
// verifies at depth data; no COW side file is left; a snapshot restored into
// a new volume is the last flushed image. Recorded for the operator guide:
// guest errors and the slowest answer while the store is down, when and how
// the cut answered, the peak COW side bytes.
const storeDownBound = 20 * time.Second

func TestProcedureStoreDownDuringACut(t *testing.T) {
	for _, down := range []string{"filer", "volume"} {
		t.Run(down, func(t *testing.T) { storeDownDuringACut(t, down) })
	}
}

func storeDownDuringACut(t *testing.T, down string) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
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

	// A cut starts; the store goes down once its uploads have begun.
	gen, entries := headGen(), f1FilerEntries(t, env)
	type cutAnswer struct {
		rc  int
		out string
		at  time.Duration
	}
	answered := make(chan cutAnswer, 1)
	cutStart := time.Now()
	go func() {
		out, errOut, rc := env.h.run(t, cut...)
		answered <- cutAnswer{rc, out + errOut, time.Since(cutStart)}
	}()
	for end := time.Now().Add(10 * time.Second); f1FilerEntries(t, env) <= entries && time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
	}
	env.cluster.stopProc(t, down)
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
	for n := 0; time.Since(downAt) < time.Minute; n++ {
		ch, i := (n/cutCrashPer)%cutCrashChunks, n%cutCrashPer
		lba := uint64(ch*ladderChunkBlocks + i)
		b := cutCrashBlock('C', lba)
		if err := op(func() error { return c.tryWrite(lba, b) }); err != nil {
			errs++
			continue
		}
		written++
		pending = append(pending, lba)
		if len(pending) == 8 {
			if err := op(c.tryFlush); err != nil {
				errs++
			} else {
				for _, l := range pending {
					want[l] = cutCrashBlock('C', l)
				}
			}
			pending = pending[:0]
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
	c.close()
	t.Logf("%s down 60s: guest writes %d, errors %d, slowest answer %s, frontend ready %v, peak COW side bytes %d",
		down, written, errs, slowest.Round(time.Millisecond), env.frontendReadyAt("s1"), peakSide)
	if cutWhileDown != nil {
		t.Logf("%s down: the cut answered while it was down, after %s, exit %d: %s", down, cutWhileDown.at.Round(time.Millisecond), cutWhileDown.rc, firstLine(cutWhileDown.out))
	} else {
		t.Logf("%s down: the cut had not answered after %s", down, time.Since(cutStart).Round(time.Second))
	}

	// The store comes back.
	env.cluster.restartProc(t, down)
	backAt := time.Now()
	if down == "filer" {
		w4WaitTCP(t, env.cluster.filerGRPC, 30*time.Second)
	}
	if cutWhileDown == nil {
		select {
		case a := <-answered:
			t.Logf("%s back: the stalled cut answered %s after the restart, exit %d: %s", down, time.Since(backAt).Round(time.Millisecond), a.rc, firstLine(a.out))
		case <-time.After(5 * time.Minute):
			t.Fatalf("%s back: the cut started before the outage never answered", down)
		}
	}
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
