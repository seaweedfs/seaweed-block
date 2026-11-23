//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// P13.6 / INV-WS-CLEAN-MOVE-001 / INV-WS-REVOKE-ACK-001:
// Revoking survives blockmaster SIGKILL; after restart, the same holder
// may finish its valid drain and ACK before a clean move reads the data.
func TestProcedureMasterKilledWhileRevoking(t *testing.T) {
	masterKilledWhileRevoking(t, true)
}

// The live holder must also finish after a master crash without a test-
// induced pause consuming its lease. Lease expiry here is a product finding.
func TestProcedureMasterKilledWhileRevokingWithoutHolderPause(t *testing.T) {
	masterKilledWhileRevoking(t, false)
}

func masterKilledWhileRevoking(t *testing.T, freeze bool) {
	t.Helper()
	env := startHandoffEnv(t, 2)
	holder := env.start(t, "holder-before-master-kill", "s1")
	e := env.adopt(t, holder, "s1")
	want := bytes.Repeat([]byte{0x6d}, 4096)
	env.writeBlock(t, "s1", 7, want)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	// Freeze only the owned holder so it cannot race an ACK past the
	// observed Revoking/no-ACK boundary. This is not a graceful master stop.
	conn, err := grpc.NewClient(env.h.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ws := control.NewWorkspaceServiceClient(conn)
	identity := &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: "r1", ServerId: "s1", Epoch: e}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	lease, err := ws.RenewWorkspaceLease(ctx, identity)
	cancel()
	if err != nil || lease.GetValidForNs() <= 0 {
		t.Fatalf("pre-crash lease: %v %v", lease, err)
	}
	validity := time.Duration(lease.GetValidForNs())
	frozenAt := time.Now()
	if freeze {
		if err := holder.proc.Signal(syscall.SIGSTOP); err != nil {
			t.Fatal(err)
		}
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
	if freeze {
		requireRevoking()
	}
	env.masterProcess.kill(t)
	var exited *exec.ExitError
	if !errors.As(env.masterProcess.err, &exited) {
		t.Fatalf("master did not have a signal exit: %v", env.masterProcess.err)
	}
	status, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("master exit %v; want SIGKILL", exited)
	}
	restarted := env.h.start(t, "blockmaster-after-revoke-kill", env.h.master, env.masterArgs...)
	restarted.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	if freeze {
		requireRevoking()
		out, errs, code := env.h.run(t, env.moveClean(e, "s2", "before-ack")...)
		if err := checkCleanMoveRefused(out, errs, code); err != nil {
			t.Fatal(err)
		}
		b := env.hosts["s2"]
		out, errs, code = env.h.run(t, "ops", "workspace", "force-move", "--master", env.h.masterAddr,
			"--volume", "v1", "--target-host", b.id, "--target-data", b.data, "--target-ctrl", b.ctrl,
			"--request-id", "before-expiry", "--confirm-forced", fmt.Sprintf("v1@%d", e), "--dry-run")
		if code != 0 || !strings.Contains(out, "forced move not admitted:") || !strings.Contains(out, "admissible at ") {
			t.Fatalf("forced preview must affirmatively refuse unexpired lease: code=%d out=%s err=%s", code, out, errs)
		}
		requireRevoking()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		renewed, renewErr := ws.RenewWorkspaceLease(ctx, identity)
		cancel()
		t.Logf("post-reload renewal: reply=%v error=%v", renewed, renewErr)
		if renewErr != nil || !renewed.GetRevoking() || renewed.GetValidForNs() <= 0 {
			t.Fatalf("PRODUCT: reloaded master cannot answer the valid Revoking holder: %v %v", renewed, renewErr)
		}
	}
	if got := env.sealedLSN(t, e, g); got != l {
		t.Fatalf("master restart changed the sealed LSN %d -> %d", l, got)
	}
	if freeze {
		elapsed := time.Since(frozenAt)
		t.Logf("frozen duration=%s, sampled host lease validity=%s, required below one third", elapsed, validity)
		if elapsed >= validity/3 {
			t.Fatal("INCONCLUSIVE: test pause consumed too much lease validity")
		}
		if err := holder.proc.Signal(syscall.SIGCONT); err != nil {
			t.Fatal(err)
		}
	}
	if err := holder.wait(30 * time.Second); err != nil {
		out, _, code := env.h.run(t, env.show...)
		masterLog, _ := os.ReadFile(restarted.log)
		holderLog, _ := os.ReadFile(holder.log)
		t.Fatalf("PRODUCT: holder failed restored revocation (paused=%t, exit=%d): %v; master journal view code=%d: %s; master log=%s; holder log=%s", freeze, exitCode(err), err, code, out, masterLog, holderLog)
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
