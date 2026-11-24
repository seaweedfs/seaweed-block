//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Phase 0 procedure tests on the TLS binaries (the gate's T6-T13). Each runs
// real blockmaster, blockvolume and sw-block processes with mutual TLS.

func startTLSEnv(t *testing.T, n int, targets []string, notAfter map[string]time.Time) *handoffEnv {
	t.Helper()
	if notAfter == nil {
		notAfter = map[string]time.Time{}
	}
	return startHandoffEnvTLS(t, n, targets, notAfter)
}

func (env *handoffEnv) dialAs(t *testing.T, name string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(env.h.masterAddr, env.masterDialOption(t, name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func wantCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("%s: %v; want %s", what, err, want)
	}
}

// T8: a node certificate acting for another server's volume (s2's identity
// renewing, ACKing, heartbeating and subscribing as s1) is refused at each
// call, and s1's lease and service are unaffected.
func TestProcedureTLSWrongNodeCertificateIsRefused(t *testing.T) {
	env := startTLSEnv(t, 2, nil, nil)
	d := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, d, "s1")
	block := pattern("wrong-node-cert!")
	env.writeBlock(t, "s1", 0, block)

	conn := env.dialAs(t, "s2")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id := &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: "r1", Epoch: e, ServerId: "s1"}
	ws := control.NewWorkspaceServiceClient(conn)
	_, err := ws.RenewWorkspaceLease(ctx, id)
	wantCode(t, "s2's certificate renewing s1's lease", err, codes.PermissionDenied)
	_, err = ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 1})
	wantCode(t, "s2's certificate ACKing for s1", err, codes.PermissionDenied)
	_, err = control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "s1"})
	wantCode(t, "s2's certificate heartbeating as s1", err, codes.PermissionDenied)
	stream, err := control.NewAssignmentServiceClient(conn).SubscribeAssignments(ctx, &control.SubscribeRequest{VolumeId: "v1", ReplicaId: "r1", ServerId: "s1"})
	if err == nil {
		_, err = stream.Recv()
	}
	wantCode(t, "s2's certificate subscribing as s1", err, codes.PermissionDenied)

	// s1 keeps its lease past one TTL and still serves the block.
	time.Sleep(35 * time.Second)
	if d.has("workspace lease: fenced") || !env.frontendReadyAt("s1") {
		t.Fatal("s1 lost its lease or its frontend after the refused calls")
	}
	if !bytes.Equal(env.readBlock(t, "s1", 0), block) {
		t.Fatal("s1 no longer reads its block")
	}
}

// T11: a role file corrupted and reloaded (SIGHUP) under load keeps the
// previous table: no fence, no change.
func TestProcedureTLSCorruptRoleFileAtReloadChangesNothing(t *testing.T) {
	env := startTLSEnv(t, 1, nil, nil)
	d := env.start(t, "blockvolume-s1", "s1")
	env.adopt(t, d, "s1")
	good, err := os.ReadFile(env.tls.roles)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(env.tls.roles, []byte("sha256:not-a-fingerprint node s1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := env.masterProcess.proc.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	env.masterProcess.waitLog(t, 10*time.Second, "NOT reloaded, the previous table stays")
	deadline := time.Now().Add(40 * time.Second) // more than one lease TTL under writes
	for i := uint64(0); time.Now().Before(deadline); i++ {
		b := bytes.Repeat([]byte{byte(i)}, 4096)
		env.writeBlock(t, "s1", i%64, b)
		if !bytes.Equal(env.readBlock(t, "s1", i%64), b) {
			t.Fatalf("write %d did not read back after the corrupt reload", i)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if d.has("workspace lease: fenced") || !env.frontendReadyAt("s1") {
		t.Fatal("the holder fenced after a corrupt role file was refused")
	}
	if err := os.WriteFile(env.tls.roles, good, 0o600); err != nil {
		t.Fatal(err)
	}
}

// T12: blockmaster restarted with one holder removed from the role file:
// that holder fences (its renewals are refused until its lease ends); the
// other holder keeps serving.
func TestProcedureTLSMasterRestartWithAHolderRemoved(t *testing.T) {
	env := startTLSEnv(t, 1, []string{"v2"}, nil)
	d1 := env.start(t, "blockvolume-s1", "s1")
	env.adopt(t, d1, "s1")
	d2 := env.h.start(t, "blockvolume-n-v2", env.h.volume, env.volArgsFor(env.hosts["n-v2"])...)
	d2.waitLog(t, 60*time.Second, "blockvolume: volume v2 adopted as PRIMARY at r1@")

	env.tls.removeRole(t, "s1")
	env.masterProcess.kill(t)
	env.masterProcess = env.h.start(t, "blockmaster-restarted", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")

	// s1 cannot renew any more: it fences within its lease and exits.
	if err := d1.wait(90 * time.Second); err == nil {
		t.Fatal("s1 exited 0; want the fence exit")
	}
	if !d1.has("workspace lease: fenced volume=v1") {
		t.Fatalf("s1 exited without a lease fence line (log %s)", d1.log)
	}
	// n-v2 keeps renewing and serving.
	time.Sleep(5 * time.Second)
	select {
	case <-d2.done:
		t.Fatalf("n-v2 exited although its line is still in the role file: %v", d2.err)
	default:
	}
	if d2.has("workspace lease: fenced") {
		t.Fatal("n-v2 fenced although its line is still in the role file")
	}
}

// T13: sw-block without a client certificate against the TLS master is
// refused at the handshake; it exits "not authorized" (5) and names the cure.
// (A forced move by an operator is refused at the master: T7 and
// TestForcedMoveNeedsAnAdmin.)
func TestProcedureTLSPlaintextClientIsRefusedWithTheCure(t *testing.T) {
	env := startTLSEnv(t, 1, nil, nil)
	admin := env.h.cliEnv
	env.h.cliEnv = []string{"SW_BLOCK_TLS_CERT=", "SW_BLOCK_TLS_KEY=", "SW_BLOCK_TLS_CA="}
	_, stderr, code := env.h.run(t, env.show...)
	if code != 5 || !strings.Contains(stderr, "SW_BLOCK_TLS_CERT") || !strings.Contains(stderr, "mutual TLS") {
		t.Fatalf("plaintext sw-block against the TLS master: exit %d, stderr %q; want 5 naming SW_BLOCK_TLS_CERT and mutual TLS", code, stderr)
	}
	env.h.cliEnv = admin
	if _, _, code := env.h.run(t, env.show...); code != 0 {
		t.Fatalf("admin show: exit %d", code)
	}
}

// fencedHolderThenForcedMove is T6/T7's flow: s1 serves, publishes block 0
// (seal), writes block 8 after it (unpublished), then lose() takes its
// certificate away. s1 fences and exits; when operatorFirst, an operator's
// forced move is refused as not authorized; the admin's forced move is
// admitted, s2 serves block 0, block 8 is the recorded lost range.
func fencedHolderThenForcedMove(t *testing.T, env *handoffEnv, lose func(), operatorFirst bool) {
	t.Helper()
	must, mustU := fatalOn(t)
	d := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, d, "s1")
	kept := pattern("published-block!")
	env.writeBlock(t, "s1", 0, kept)
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	env.writeBlock(t, "s1", 8, pattern("after-the-seal!!"))

	lose()
	if err := d.wait(150 * time.Second); err == nil {
		t.Fatal("s1 exited 0 after losing its certificate; want the fence exit")
	}
	if !d.has("workspace lease: fenced volume=v1") {
		t.Fatalf("s1 exited without a lease fence line (log %s)", d.log)
	}

	if operatorFirst {
		admin := env.h.cliEnv
		env.h.cliEnv = env.tls.cliEnv("operator")
		b := env.hosts["s2"]
		args := []string{"ops", "workspace", "force-move", "--master", env.h.masterAddr, "--volume", "v1",
			"--target-host", b.id, "--target-data", b.data, "--target-ctrl", b.ctrl,
			"--request-id", "force-by-operator", "--confirm-forced", fmt.Sprintf("v1@%d", e)}
		poll(t, 120*time.Second, "the forced move preview never admitted (operator)", func() bool {
			out, _, _ := env.h.run(t, append(args, "--dry-run")...)
			return previewAdmitted(out)
		})
		_, stderr, code := env.h.run(t, args...)
		if code != 5 || !strings.Contains(stderr, "admin") {
			t.Fatalf("operator forced move: exit %d, stderr %q; want 5 (not authorized) naming admin", code, stderr)
		}
		env.h.cliEnv = admin
	}

	out := env.forceMove(t, e, "s2", "force-v1-tls")
	e2 := mustU(forcedEpoch(out, e, "s1", "s2"))
	if got := mustU(forcedKeptLSN(out)); got != l {
		t.Fatalf("the forced move kept LSN %d; want the sealed HEAD's %d", got, l)
	}
	ranges, _ := env.h.command(t, "ops", "workspace", "lost-range", "--master", env.h.masterAddr, "--volume", "v1")
	if !strings.Contains(ranges, "range 1 of 1: request force-v1-tls, r1@") {
		t.Fatalf("lost-range does not show the forced move:\n%s", ranges)
	}
	nw := env.start(t, "blockvolume-s2", "s2")
	if got := env.adopt(t, nw, "s2"); got != e2 {
		t.Fatalf("s2 adopted at epoch %d; want %d", got, e2)
	}
	if !bytes.Equal(env.readBlock(t, "s2", 0), kept) {
		t.Fatal("the published block does not read back on s2")
	}
	must(func() error {
		if got := env.readBlock(t, "s2", 8); !bytes.Equal(got, make([]byte, 4096)) {
			return fmt.Errorf("the block written after the seal reads back on s2; it is in the lost range")
		}
		return nil
	}())
}

// T6: the holder's certificate expires on the master clock while it serves
// (NotAfter set close in the test CA; no clock is stepped).
func TestProcedureTLSExpiredHolderCertificate(t *testing.T) {
	env := startTLSEnv(t, 2, nil, map[string]time.Time{"s1": time.Now().Add(75 * time.Second)})
	fencedHolderThenForcedMove(t, env, func() {}, false) // the expiry comes by itself
}

// T7: the holder's line is removed from the role file and blockmaster
// reloads it (SIGHUP) while the holder serves.
func TestProcedureTLSRevokedHolderCertificate(t *testing.T) {
	env := startTLSEnv(t, 2, nil, nil)
	fencedHolderThenForcedMove(t, env, func() {
		env.tls.removeRole(t, "s1")
		if err := env.masterProcess.proc.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		env.masterProcess.waitLog(t, 10*time.Second, "tls/roles reloaded")
	}, true)
}
