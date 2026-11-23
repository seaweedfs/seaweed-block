//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"fmt"
	"syscall"
	"testing"
	"time"
)

// P13.6 / INV-WS-CLEAN-MOVE-001 / INV-WS-REVOKE-ACK-001:
// Revoking survives blockmaster SIGKILL; after restart, the same holder
// may finish its valid drain and ACK before a clean move reads the data.
func TestProcedureMasterKilledWhileRevoking(t *testing.T) {
	env := startHandoffEnv(t, 2)
	holder := env.start(t, "holder-before-master-kill", "s1")
	e := env.adopt(t, holder, "s1")
	want := bytes.Repeat([]byte{0x6d}, 4096)
	env.writeBlock(t, "s1", 7, want)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	// Freeze only the owned holder so it cannot race an ACK past the
	// observed Revoking/no-ACK boundary. This is not a graceful master stop.
	if err := holder.proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	env.revoke(t, e, g)
	requireRevoking := func() {
		t.Helper()
		out, _ := env.h.command(t, env.show...)
		if !hasLine(out, fmt.Sprintf("epoch: %d", e)) || !hasLine(out, "lease state: revoking") ||
			!hasLine(out, "recorded ACK: none") {
			t.Fatalf("want the same Revoking line and no ACK:\n%s", out)
		}
	}
	requireRevoking()
	env.masterProcess.kill(t)
	if env.masterProcess.err == nil {
		t.Fatal("master exited successfully instead of being killed")
	}
	restarted := env.h.start(t, "blockmaster-after-revoke-kill", env.h.master, env.masterArgs...)
	restarted.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	requireRevoking()
	if got := env.sealedLSN(t, e, g); got != l {
		t.Fatalf("master restart changed the sealed LSN %d -> %d", l, got)
	}
	if err := holder.proc.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("holder failed to complete the restored revocation: %v", err)
	}
	env.waitAck(t, e, g)
	out, _ := env.h.command(t, env.moveClean(e, "s2", "move-v1-1")...)
	_, mustU := fatalOn(t)
	e2 := mustU(movedEpochTo(out, e, g, l, "s2"))
	target := env.start(t, "holder-after-restored-revocation", "s2")
	if got := env.adopt(t, target, "s2"); got != e2 {
		t.Fatalf("move target adopted epoch %d; want %d", got, e2)
	}
	env.requireHeadHolder(t, e2, "s2")
	if got := env.readBlock(t, "s2", 7); !bytes.Equal(got, want) {
		t.Fatal("clean move after master SIGKILL lost the acknowledged block")
	}
}
