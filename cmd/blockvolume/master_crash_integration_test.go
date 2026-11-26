//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"errors"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This kills the master after the move answer, before the target starts.
// It tests an issued move's recovery, not a kill inside a persistence syscall.
func TestProcedureMasterKilledAfterMoveBeforeTargetStarts(t *testing.T) {
	env := startHandoffEnv(t, 3)
	must, mustU := fatalOn(t)
	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	block := pattern("master-move-test")
	env.writeBlock(t, "s1", 0, block)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	env.revoke(t, e, g)
	must(old.wait(30 * time.Second))
	env.waitAck(t, e, g)

	args := env.moveClean(e, "s2", "move-v1-1")
	out, _ := env.h.command(t, args...)
	e2 := mustU(movedEpochTo(out, e, g, l, "s2"))
	restartKilledMaster(t, env)
	// The same request must recover the issued epoch, not issue another one.
	out, _ = env.h.command(t, args...)
	if replay := mustU(movedEpochTo(out, e, g, l, "s2")); replay != e2 {
		t.Fatalf("replayed move issued epoch %d; original was %d", replay, e2)
	}
	for _, conflict := range []struct{ target, request string }{
		{"s2", "different-request"}, {"s3", "different-target"},
	} {
		out, stderr, code := env.h.run(t, env.moveClean(e, conflict.target, conflict.request)...)
		if code != 1 || out != "" || strings.TrimSpace(stderr) != "workspace move: clean move: workspace move: missing or changed authority/lease" {
			t.Fatalf("competing move to %s: exit=%d stdout=%q stderr=%q; want the definite-refusal exit and changed-authority answer", conflict.target, code, out, stderr)
		}
	}
	out, _ = env.h.command(t, env.chunkShow()...)
	if got := mustU(sealedLayoutHeadPosition(out, e, g)); got != l {
		t.Fatalf("sealed HEAD LSN %d; want %d", got, l)
	}
	nw := env.start(t, "blockvolume-s2", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("target adopted epoch %d; want recovered epoch %d", got, e2)
	}
	// Planned hand-over step 8 observes the completed layout claim in HEAD:
	// its identity advanced while the sealed image and position were kept.
	out, _ = env.h.command(t, env.chunkShow()...)
	must(checkLayoutHeadReopenedBy(out, e2, g, l, "s2"))
	if !bytes.Equal(env.readBlock(t, "s2", 0), block) {
		t.Fatal("acknowledged source block changed after master crash and move recovery")
	}
	if !bytes.Equal(env.readBlock(t, "s2", 8), make([]byte, 4096)) {
		t.Fatal("unwritten target block is not zero")
	}
	// A returning source must not adopt or serve the issued target's line.
	returning := env.start(t, "blockvolume-s1-returning", "s1")
	poll(t, 45*time.Second, "returning old holder did not exit", func() bool {
		if returning.has("adopted as PRIMARY") || env.servingNow("s1") {
			t.Fatal("returning old holder adopted or served after the issued move")
		}
		select {
		case <-returning.done:
			return true
		default:
			return false
		}
	})
	code := exitCode(returning.err)
	if (code != 75 && code != 78) || !returning.has("blockvolume: exiting status=") {
		t.Fatalf("returning old holder exit=%v without the documented retry/no-retry exit line", returning.err)
	}

}

func restartKilledMaster(t *testing.T, env *handoffEnv) {
	t.Helper()
	env.masterProcess.kill(t)
	var exit *exec.ExitError
	if !errors.As(env.masterProcess.err, &exit) {
		t.Fatalf("master exit %v; want SIGKILL", env.masterProcess.err)
	}
	if ws, ok := exit.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("master status %v; want SIGKILL", exit.Sys())
	}
	t.Log("blockmaster confirmed dead by SIGKILL before restart")
	env.masterProcess = env.h.start(t, "blockmaster-restarted", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
}
