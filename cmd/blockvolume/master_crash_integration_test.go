//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// This kills the master after the move answer, before the target starts.
// It tests an issued move's recovery, not a kill inside a persistence syscall.
func TestProcedureMasterKilledAfterMoveBeforeTargetStarts(t *testing.T) {
	env := startHandoffEnv(t, 2)
	must, mustU := fatalOn(t)
	old := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, old, "s1")
	block := pattern("master-crash-move")
	env.writeBlock(t, "s1", 0, block)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	env.revoke(t, e, g)
	must(old.wait(30 * time.Second))
	env.waitAck(t, e, g)

	args := env.moveClean(e, "s2", "crash-after-move")
	out, _ := env.h.command(t, args...)
	e2 := mustU(movedEpochTo(out, e, g, l, "s2"))
	restartKilledMaster(t, env)
	// The same request must recover the issued epoch, not issue another one.
	out, _ = env.h.command(t, args...)
	if replay := mustU(movedEpochTo(out, e, g, l, "s2")); replay != e2 {
		t.Fatalf("replayed move issued epoch %d; original was %d", replay, e2)
	}
	out, _ = env.h.command(t, env.chunkShow()...)
	if got := mustU(sealedHeadLSN(out, e, g)); got != l {
		t.Fatalf("sealed HEAD LSN %d; want %d", got, l)
	}
	nw := env.start(t, "blockvolume-s2", "s2")
	must(checkClaimDoneBy(nw.waitLog(t, 60*time.Second, "durable: HEAD claim for volume=v1 done in"), e2, "s2"))
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("target adopted epoch %d; want recovered epoch %d", got, e2)
	}
	env.requireHeadHolder(t, e2, "s2")
	if !bytes.Equal(env.readBlock(t, "s2", 0), block) {
		t.Fatal("acknowledged source block changed after master crash and move recovery")
	}
	if !bytes.Equal(env.readBlock(t, "s2", 8), make([]byte, 4096)) {
		t.Fatal("unwritten target block is not zero")
	}
}

func restartKilledMaster(t *testing.T, env *handoffEnv) {
	t.Helper()
	env.master.kill(t)
	var exit *exec.ExitError
	if !errors.As(env.master.err, &exit) {
		t.Fatalf("master exit %v; want SIGKILL", env.master.err)
	}
	if ws, ok := exit.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("master status %v; want SIGKILL", exit.Sys())
	}
	t.Log("blockmaster confirmed dead by SIGKILL before restart")
	env.master = env.h.start(t, "blockmaster-restarted", env.h.master, env.masterArgs...)
	env.master.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
}
