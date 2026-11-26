//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The procedure gate: every operator procedure of docs/volume-lifecycle.md
// runs as written, with real binaries, on blockvolume processes with distinct
// server ids (s1, and s2 where a procedure moves the volume) sharing one
// blockmaster and one SeaweedFS. The two start-time tests and the same-host
// restarts use s1 only. The planned return to the same host is
// TestStandalonePlannedHandoffToTheSameHost.

// TestProcedurePlannedHandoffToAnotherHost: the planned hand-over from s1 to
// s2. The new holder starts with no local copy, claims the sealed HEAD at the
// new epoch and serves the written block.
func TestProcedurePlannedHandoffToAnotherHost(t *testing.T) {
	env := startHandoffEnv(t, 2)
	must, mustU := fatalOn(t)
	h := env.h

	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	block := pattern("to-another-host!")
	env.writeBlock(t, "s1", 0, block)

	// Step 1: read the holder; the lease must be granted.
	out, _ := h.command(t, env.show...)
	must(checkShowGranted(out, e))
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	env.revoke(t, e, g)
	if err := old.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)

	moved, _ := h.command(t, env.moveClean(e, "s2", "move-v1-1")...)
	e2 := mustU(movedEpochTo(moved, e, g, l, "s2"))
	nw := env.start(t, "blockvolume-s2", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("s2 adopted at epoch %d; want %d", got, e2)
	}
	out, _ = h.command(t, env.show...)
	must(wantLines(out, fmt.Sprintf("holder: r1@%d (logical replica)", e2),
		fmt.Sprintf("move: request=move-v1-1 kind=clean issued=true s1/r1@%d -> s2/r1@%d HEAD generation %d remote LSN %d", e, e2, g, l)))
	out, _ = h.command(t, env.chunkShow()...)
	must(checkLayoutHeadReopenedBy(out, e2, g, l, "s2"))
	if !bytes.Equal(env.readBlock(t, "s2", 0), block) {
		t.Fatal("the block written on s1 does not read back on s2")
	}
	// Control: a block never written reads zero, so the comparison above can
	// fail.
	if !bytes.Equal(env.readBlock(t, "s2", 8), make([]byte, 4096)) {
		t.Fatal("a block never written does not read zero on s2")
	}
}

// TestProcedureSameHostRestartInsideTheLease: the page's restart without a
// hand-over, `systemctl restart`: the holder gets SIGTERM, exits, and is
// started again on its host within the unit's RestartSec (5 s), inside the
// 30 s lease. It keeps holder and epoch and serves again at once from its
// local WAL.
func TestProcedureSameHostRestartInsideTheLease(t *testing.T) {
	env := startHandoffEnv(t, 1)
	first := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, first, "s1")
	block := pattern("restart-in-lease")
	env.writeBlock(t, "s1", 0, block)
	first.term(t, 30*time.Second)
	env.restartServesAtOnce(t, "blockvolume-s1-again", e, block)
}

// TestProcedureSameHostCrashRestartInsideTheLease: the page's "the holder
// comes back within the lease" after a crash: the process is killed and
// started again within the unit's RestartSec (5 s). The same outcome as a
// restart.
func TestProcedureSameHostCrashRestartInsideTheLease(t *testing.T) {
	env := startHandoffEnv(t, 1)
	first := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, first, "s1")
	block := pattern("crash-in-lease!!")
	env.writeBlock(t, "s1", 0, block)
	first.kill(t)
	env.restartServesAtOnce(t, "blockvolume-s1-again", e, block)
}

// restartServesAtOnce starts s1 again within 5 s of its exit and requires it
// to serve within 15 s ("at once"), as s1/r1 at the same epoch, with the
// written block.
func (env *handoffEnv) restartServesAtOnce(t *testing.T, name string, e uint64, block []byte) {
	t.Helper()
	must, _ := fatalOn(t)
	started := time.Now()
	again := env.start(t, name, "s1")
	if got := env.adopt(t, again, "s1"); got != e {
		t.Fatalf("the restarted holder adopted epoch %d; want the same epoch %d", got, e)
	}
	if took := time.Since(started); took > 15*time.Second {
		t.Fatalf("the restarted holder served after %s; the page says at once", took.Round(time.Second))
	}
	out, _ := env.h.command(t, env.show...)
	must(checkShowGranted(out, e))
	env.requireHeadHolder(t, e, "s1")
	if !bytes.Equal(env.readBlock(t, "s1", 0), block) {
		t.Fatal("the block written before the restart does not read back")
	}
}

// TestProcedureReturnAfterALapsedLease: the holder's process dies and stays
// down past its lease. Started again 65 s after it stopped (the page's
// bound with default flags), it is granted again at the same epoch, reclaims
// HEAD and serves its local WAL.
func TestProcedureReturnAfterALapsedLease(t *testing.T) {
	env := startHandoffEnv(t, 2)
	must, _ := fatalOn(t)

	first := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, first, "s1")
	block := pattern("after-lapsed-ttl")
	env.writeBlock(t, "s1", 0, block)
	first.kill(t)
	stopped := time.Now()

	time.Sleep(time.Until(stopped.Add(65 * time.Second)))
	started := time.Now()
	back := env.start(t, "blockvolume-s1-back", "s1")
	if got := env.adopt(t, back, "s1"); got != e {
		t.Fatalf("the returning holder adopted epoch %d; want the same epoch %d", got, e)
	}
	if took := time.Since(started); took > 30*time.Second {
		t.Fatalf("the returning holder served after %s, past its 30 s authority wait", took.Round(time.Second))
	}
	if back.has("assignment unavailable, not serving") {
		t.Fatal("started 65 s after the stop, the holder still logged the not-serving line")
	}
	out, _ := env.h.command(t, env.show...)
	must(checkShowGranted(out, e))
	env.requireHeadHolder(t, e, "s1")
	if !bytes.Equal(env.readBlock(t, "s1", 0), block) {
		t.Fatal("the block written before the lapse does not read back")
	}
}

// TestProcedureForcedMoveWithTheOldHostReturning: the holder on s1 dies and
// never comes back in time. A forced move to s2, admitted after the barrier,
// restores the last HEAD: a block cut before the crash is kept, one written
// after the cut is lost, as the lost range says. The old s1 process then
// returns with its old local root and must not serve; s2 keeps serving.
func TestProcedureForcedMoveWithTheOldHostReturning(t *testing.T) {
	env := startHandoffEnv(t, 2)
	must, mustU := fatalOn(t)
	h := env.h

	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	kept, lost := pattern("cut-before-crash"), pattern("after-the-cut!!!")
	env.writeBlock(t, "s1", 0, kept)
	h.command(t, "ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status)
	env.writeBlock(t, "s1", 8, lost)
	old.kill(t)

	out := env.forceMove(t, e, "s2", "force-v1-1")
	e2, keptLSN := mustU(forcedEpoch(out, e, "s1", "s2")), mustU(forcedKeptLSN(out))
	must(wantLines(out, "next: start volume v1 on s2 (this command moved authority only)"))
	if !strings.Contains(out, fmt.Sprintf("writes with LSN <= %d are preserved", keptLSN)) {
		t.Fatalf("the forced move printed no lost range at LSN %d:\n%s", keptLSN, out)
	}
	ranges, _ := h.command(t, "ops", "workspace", "lost-range", "--master", h.masterAddr, "--volume", "v1")
	if !strings.Contains(ranges, "range 1 of 1: request force-v1-1, r1@") {
		t.Fatalf("lost-range does not show the forced move:\n%s", ranges)
	}

	nw := env.start(t, "blockvolume-s2", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("s2 adopted at epoch %d; want %d", got, e2)
	}
	if !bytes.Equal(env.readBlock(t, "s2", 0), kept) {
		t.Fatal("the block cut before the crash is not on s2")
	}
	if got := env.readBlock(t, "s2", 8); !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("the block written after the last cut reads back on s2; the page says it is lost")
	}

	// The old host returns with its old local root. blockmaster refuses its
	// lease with a typed MOVED (the volume moved to s2 at a newer epoch), so
	// it exits 78, which the example unit does not restart
	// (RestartPreventExitStatus=78); an operator's start again ends the same
	// way. Each time it must not take s2's line as its own (a holder is server
	// and replica), report its frontend ready or listen for I/O; it logs
	// `exiting status=78 reason=moved`.
	back := env.oldHostRefuses(t, "blockvolume-s1-back", env.hosts["s1"], e2, 78)
	time.Sleep(5 * time.Second)
	again := env.oldHostRefuses(t, "blockvolume-s1-restarted", env.hosts["s1"], e2, 78)
	// Control: on a fresh local root there is no stale root; the master's
	// MOVED refusal alone ends it, also with status 78.
	fresh := env.hosts["s1"]
	fresh.root = fresh.root + "-fresh"
	freshRoot := env.oldHostRefuses(t, "blockvolume-s1-fresh-root", fresh, e2, 78)
	// A line carries its owner and that owner's endpoints: every line
	// delivered at the forced epoch carries s2's own endpoints, never another
	// host's. A returning source can instead be refused MOVED before the
	// assignment stream delivers anything (volume-lifecycle.md, do-not list).
	if bad := wrongOwnerEndpoints(e2, env.hosts["s2"], nw, back, again, freshRoot); len(bad) > 0 {
		t.Errorf("epoch-%d owner evidence invalid (want data=%s ctrl=%s in every delivery, or a moved refusal before delivery):\n%s",
			e2, env.hosts["s2"].data, env.hosts["s2"].ctrl, strings.Join(bad, "\n"))
	}
	env.h.waitPrimaryAt(t, env.hosts["s2"].status, e2)
	if !bytes.Equal(env.readBlock(t, "s2", 0), kept) {
		t.Fatal("s2 no longer serves the kept block after the old host returned")
	}
}

// TestProcedureRevokeWithoutACKThenForcedMove: the volume is sealed, its
// process is gone before the revoke (so no process ever sends the ACK), and
// the lease is revoked with the sealed generation. The clean move is refused;
// only a forced move to another node remains, and since HEAD is sealed,
// nothing acknowledged is lost. Killing before the revoke makes the missing
// ACK certain; the page's row "Revoked, but no ACK" has the same outcome.
func TestProcedureRevokeWithoutACKThenForcedMove(t *testing.T) {
	env := startHandoffEnv(t, 2)
	must, mustU := fatalOn(t)
	h := env.h

	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	block := pattern("sealed-no-ack!!!")
	env.writeBlock(t, "s1", 0, block)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	old.kill(t)
	env.revoke(t, e, g)
	o, eo, code := h.run(t, env.moveClean(e, "s2", "move-v1-1")...)
	must(checkCleanMoveRefused(o, eo, code))

	out := env.forceMove(t, e, "s2", "force-v1-1")
	e2 := mustU(forcedEpoch(out, e, "s1", "s2"))
	if got := mustU(forcedKeptLSN(out)); got != l {
		t.Fatalf("the forced move kept LSN %d; want the sealed HEAD's %d", got, l)
	}
	nw := env.start(t, "blockvolume-s2", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("s2 adopted at epoch %d; want %d", got, e2)
	}
	if !bytes.Equal(env.readBlock(t, "s2", 0), block) {
		t.Fatal("the sealed block does not read back on s2")
	}
}

// TestProcedureStartInsideTheBarrierWaitIsReGranted: the holder's process
// dies and is started again 40 s later: its lease has ended, the re-grant
// barrier (at least 50 s and at most 65 s after the stop, default flags) has
// not passed, and it passes within the process's 30 s authority wait. It must not
// serve while its renewals are still refused for the barrier; once
// re-granted it serves its local WAL at the same epoch.
func TestProcedureStartInsideTheBarrierWaitIsReGranted(t *testing.T) {
	env := startHandoffEnv(t, 1)
	first := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, first, "s1")
	block := pattern("inside-the-wait!")
	env.writeBlock(t, "s1", 0, block)
	first.kill(t)
	time.Sleep(40 * time.Second)

	back := env.start(t, "blockvolume-s1-back", "s1")
	var ready []time.Time
	end := time.Now().Add(90 * time.Second)
	for time.Now().Before(end) {
		if env.servingNow("s1") {
			ready = append(ready, time.Now())
			if len(ready) >= 5 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(ready) == 0 {
		t.Fatal("the returning holder never served within 90 s")
	}
	// Started before the barrier, its renewals must have been refused for it
	// at least once; log times have 1 s resolution.
	last := lastLogTime(back, "clock-skew margin has not elapsed")
	if last.IsZero() {
		t.Fatal("started 40 s after the stop, the returning holder logged no renewal refused for the barrier")
	}
	if ready[0].Before(last.Add(time.Second)) {
		t.Errorf("the returning holder served at %s, while its renewal was still refused for the barrier at %s: it served without a lease",
			ready[0].Format("15:04:05.000"), last.Format("15:04:05"))
	}
	if !back.has("workspace lease: re-granted after ") {
		t.Error("the returning holder served without logging its re-grant")
	}
	if got := env.adopt(t, back, "s1"); got != e {
		t.Fatalf("the returning holder adopted epoch %d; want the same epoch %d", got, e)
	}
	if !bytes.Equal(env.readBlock(t, "s1", 0), block) {
		t.Fatal("the block written before the stop does not read back")
	}
}

// lastLogTime is the time of the last log line containing s (second
// resolution, the log's local time), or zero.
func lastLogTime(d *daemon, s string) time.Time {
	data, _ := os.ReadFile(d.log)
	var last time.Time
	for _, l := range strings.Split(string(data), "\n") {
		if strings.Contains(l, s) && len(l) >= 19 {
			if ts, err := time.ParseInLocation("2006/01/02 15:04:05", l[:19], time.Local); err == nil {
				last = ts
			}
		}
	}
	return last
}

// exitCode is a child's exit status (-1 when it did not exit with one).
func exitCode(err error) int {
	var exit *exec.ExitError
	if err == nil {
		return 0
	}
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	return -1
}

// oldHostRefuses starts the old holder b after the volume moved to epoch e2
// on another host, and returns it. While it runs it must not adopt the new
// line, report its frontend ready or listen; it must log a refusal and exit
// with wantExit.
func (env *handoffEnv) oldHostRefuses(t *testing.T, name string, b bvHost, e2 uint64, wantExit int) *daemon {
	t.Helper()
	d := env.h.start(t, name, env.h.volume, env.volArgsFor(b)...)
	var failures []string
	end := time.Now().Add(60 * time.Second)
	for alive := true; alive && time.Now().Before(end); {
		if l := d.line(fmt.Sprintf("adopted as PRIMARY at r1@%d", e2)); l != "" {
			failures = append(failures, "it adopted the new holder's line: "+l)
		}
		if env.frontendReadyAt("s1") {
			failures = append(failures, "its status reports FrontendPrimaryReady")
		}
		if listening(b.nvme) {
			failures = append(failures, "its NVMe/TCP target listens")
		}
		select {
		case <-d.done:
			alive = false
		case <-time.After(500 * time.Millisecond):
		}
	}
	if err := d.wait(5 * time.Second); exitCode(err) != wantExit {
		failures = append(failures, fmt.Sprintf("it did not exit with status %d: %v", wantExit, err))
	}
	if wantExit == 78 && !d.has("exiting status=78 reason=moved") {
		failures = append(failures, "it did not log `exiting status=78 reason=moved`")
	}
	if wantExit != 78 && !d.has("refusing to attach volume=v1") && !d.has("workspace lease: stale authority") && !d.has("foreign assignment") {
		failures = append(failures, "it logged no refusal (stale root, stale authority, or another server's assignment)")
	}
	if len(failures) > 0 {
		t.Errorf("%s after the forced move: %s", name, strings.Join(failures, "; "))
	}
	return d
}

// servingNow reports whether the host serves: its frontend is ready or its
// NVMe/TCP target accepts connections.
func (env *handoffEnv) servingNow(id string) bool {
	return env.frontendReadyAt(id) || listening(env.hosts[id].nvme)
}

// requireHeadHolder checks that HEAD is active, held by host/r1 at epoch e.
func (env *handoffEnv) requireHeadHolder(t *testing.T, e uint64, host string) {
	t.Helper()
	out, _ := env.h.command(t, env.chunkShow()...)
	head, err := parseLayoutHead(out)
	if err != nil {
		t.Fatal(err)
	}
	if head.State != "" || head.ClaimID == "" || head.Epoch != e || head.Holder != host+"/r1" {
		t.Fatalf("HEAD %+v; want live at epoch %d held by %s/r1", head, e, host)
	}
}

func (env *handoffEnv) start(t *testing.T, name, id string) *daemon {
	t.Helper()
	return env.h.start(t, name, env.h.volume, env.volArgs(id)...)
}

// adopt waits for the process to adopt v1 as primary and its host to report
// it healthy, and returns the epoch.
func (env *handoffEnv) adopt(t *testing.T, d *daemon, id string) uint64 {
	t.Helper()
	_, mustU := fatalOn(t)
	e := mustU(adoptedEpoch(d.waitLog(t, 60*time.Second, "blockvolume: volume v1 adopted as PRIMARY at r1@")))
	env.h.waitPrimaryAt(t, env.hosts[id].status, e)
	return e
}

// frontendReadyAt reports whether the host's status shows v1's frontend
// ready to serve.
func (env *handoffEnv) frontendReadyAt(id string) bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + env.hosts[id].status + "/status?volume=v1")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return frontendReady(b)
}

// listening reports whether something accepts TCP connections at addr.
func listening(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func (env *handoffEnv) writeBlock(t *testing.T, id string, lba uint64, data []byte) {
	t.Helper()
	c := dialNVMe(t, env.hosts[id].nvme, plannedHandoffNQN)
	defer c.close()
	c.write(t, lba, data)
	c.flush(t)
}

func (env *handoffEnv) readBlock(t *testing.T, id string, lba uint64) []byte {
	t.Helper()
	c := dialNVMe(t, env.hosts[id].nvme, plannedHandoffNQN)
	defer c.close()
	return c.read(t, lba, 4096)
}

func (env *handoffEnv) seal(t *testing.T, id string, e uint64) uint64 {
	t.Helper()
	_, mustU := fatalOn(t)
	out, errOut := env.h.command(t, "ops", "chunk", "detach", "--plain", "--volume", "v1", "--status-addr", env.hosts[id].status, "--timeout", "120s")
	return mustU(sealedGen(out, errOut, e))
}

func (env *handoffEnv) chunkShow() []string {
	return []string{"ops", "chunk", "show", "--master", env.h.masterAddr, "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath}
}

func (env *handoffEnv) sealedLSN(t *testing.T, e, g uint64) uint64 {
	t.Helper()
	_, mustU := fatalOn(t)
	out, _ := env.h.command(t, env.chunkShow()...)
	return mustU(sealedLayoutHeadPosition(out, e, g))
}

func (env *handoffEnv) revoke(t *testing.T, e, g uint64) {
	t.Helper()
	must, _ := fatalOn(t)
	out, _ := env.h.command(t, "ops", "workspace", "revoke", "--master", env.h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--sealed-gen", fmt.Sprint(g))
	must(checkRevoke(out, e, g))
}

func (env *handoffEnv) waitAck(t *testing.T, e, g uint64) {
	t.Helper()
	poll(t, 30*time.Second, fmt.Sprintf("workspace show never reported the ACK of r1@%d at generation %d", e, g), func() bool {
		out, _ := env.h.command(t, env.show...)
		return showsAck(out, e, g)
	})
}

func (env *handoffEnv) moveClean(e uint64, target, request string) []string {
	b := env.hosts[target]
	return []string{"ops", "workspace", "move", "--clean", "--master", env.h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--target-host", b.id, "--target-data", b.data, "--target-ctrl", b.ctrl, "--request-id", request}
}

// forceMove runs the page's forced move: --dry-run until it says admitted
// (the barrier: at most 65 s after the holder stopped, with defaults, plus a
// margin), then the move itself, whose stdout it returns.
func (env *handoffEnv) forceMove(t *testing.T, e uint64, target, request string) string {
	t.Helper()
	b := env.hosts[target]
	args := []string{"ops", "workspace", "force-move", "--master", env.h.masterAddr, "--volume", "v1",
		"--target-host", b.id, "--target-data", b.data, "--target-ctrl", b.ctrl,
		"--request-id", request, "--confirm-forced", fmt.Sprintf("v1@%d", e)}
	first, _ := env.h.command(t, append(args, "--dry-run")...)
	if !previewAdmitted(first) && !strings.Contains(first, "admissible at ") {
		t.Fatalf("a refused preview does not say when the move is admissible:\n%s", first)
	}
	poll(t, 90*time.Second, "the forced move was never admitted", func() bool {
		out, _ := env.h.command(t, append(args, "--dry-run")...)
		return previewAdmitted(out)
	})
	out, _ := env.h.command(t, args...)
	return out
}

// wrongOwnerEndpoints returns every "assignment stream delivery" log line at epoch e (in any of ds) whose data or control
// endpoint is not owner's, plus an entry for each recipient whose log cannot be
// read or shows neither an epoch-e delivery nor the documented MOVED refusal.
func wrongOwnerEndpoints(e uint64, owner bvHost, ds ...*daemon) []string {
	var bad []string
	tag := fmt.Sprintf(" epoch=%d ev=", e)
	for _, d := range ds {
		raw, err := os.ReadFile(d.log)
		if err != nil {
			bad = append(bad, d.name+": log unreadable: "+err.Error())
			continue
		}
		seen := 0
		for _, l := range strings.Split(string(raw), "\n") {
			if !strings.Contains(l, "assignment stream delivery") || !strings.Contains(l, tag) {
				continue
			}
			seen++
			if !strings.Contains(l, fmt.Sprintf("data=%q", owner.data)) || !strings.Contains(l, fmt.Sprintf("ctrl=%q", owner.ctrl)) {
				bad = append(bad, d.name+": "+strings.TrimSpace(l))
			}
		}
		if seen == 0 && !strings.Contains(string(raw), "blockvolume: exiting status=78 reason=moved") {
			// Mere absence proves nothing. The master's typed MOVED refusal
			// ends an old source's stream before any fact can be delivered.
			bad = append(bad, d.name+": no epoch delivery logged")
		}
	}
	return bad
}
