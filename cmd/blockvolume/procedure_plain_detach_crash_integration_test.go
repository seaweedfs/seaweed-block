//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// P12.6 / INV-WS-SEAL-REPLAY-001: a SIGKILL during plain-seal upload
// must not strand the hand-over. The kill is witnessed by a new immutable
// key and an unchanged active HEAD, not by a delay or a successful CLI exit.
func TestProcedureKillDuringPlainDetachThenCompleteHandoff(t *testing.T) {
	env := startHandoffEnv(t, 2)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off", "--chunk-upload-concurrency", "1"}
	holder := env.start(t, "holder-before-detach-kill", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeCutCrashImage(t, 'A')
	env.h.command(t, "ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status)
	out, _ := env.h.command(t, env.chunkShow()...)
	before, err := parseLayoutHead(out)
	if err != nil || before.State != "" || before.ClaimID == "" || before.Epoch != e {
		t.Fatalf("baseline HEAD: %+v: %v", before, err)
	}
	env.writeCutCrashImage(t, 'B')
	keys := plainDetachStoredKeys(t, env)
	cli := env.h.start(t, "plain-detach-interrupted", env.h.cli, "ops", "chunk", "detach", "--plain",
		"--volume", "v1", "--status-addr", env.hosts["s1"].status, "--timeout", "120s")
	uploadDeadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case <-cli.done:
			t.Fatal("detach finished before the mid-upload kill witness")
		default:
		}
		if plainDetachStoredKeys(t, env) > keys {
			break
		}
		if time.Now().After(uploadDeadline) {
			t.Fatal("plain detach stored no new immutable chunk")
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Stop only this child before reading the remote proof, so the uploader
	// cannot cross its CAS while the test verifies where the kill will land.
	if err := holder.proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	out, _ = env.h.command(t, env.chunkShow()...)
	atKill, err := parseLayoutHead(out)
	if err != nil || atKill.Gen != before.Gen || atKill.State != "" || atKill.ClaimID != before.ClaimID || atKill.Position != before.Position {
		t.Fatalf("missed mid-upload window: before=%+v stopped=%+v: %v", before, atKill, err)
	}
	t.Logf("kill witness: immutable keys %d -> %d, HEAD gen=%d LSN=%d remains active", keys, plainDetachStoredKeys(t, env), atKill.Gen, atKill.Position)
	select {
	case <-cli.done:
		t.Fatal("detach completed or aborted before the verified kill window")
	default:
	}
	holder.kill(t)
	var exited *exec.ExitError
	if !errors.As(holder.err, &exited) {
		t.Fatalf("holder did not have a signal exit: %v", holder.err)
	}
	status, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("holder exit %v; want SIGKILL", exited)
	}
	if err := cli.wait(15 * time.Second); err == nil {
		t.Fatal("interrupted detach unexpectedly returned success")
	}
	select {
	case <-cli.done:
	default:
		t.Fatal("interrupted detach command did not join")
	}
	// No graceful Close ran. Reopen the same root and prove every Flush-
	// acknowledged block survives before retrying the public plain detach.
	holder = env.start(t, "holder-after-detach-kill", "s1")
	if got := env.adopt(t, holder, "s1"); got != e {
		t.Fatalf("reopened epoch %d; want %d", got, e)
	}
	env.requireCutCrashImage(t, "s1", 'B')
	g := env.seal(t, "s1", e)
	l := env.sealedLSN(t, e, g)
	if l <= before.Position {
		t.Fatalf("retry did not publish the acknowledged image: remote LSN %d <= baseline %d", l, before.Position)
	}
	if again := env.seal(t, "s1", e); again != g {
		t.Fatalf("plain seal replay changed generation %d -> %d", g, again)
	}
	env.revoke(t, e, g)
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("revoked restarted holder did not exit cleanly: %v", err)
	}
	env.waitAck(t, e, g)
	out, _ = env.h.command(t, env.moveClean(e, "s2", "move-v1-1")...)
	_, mustU := fatalOn(t)
	e2 := mustU(movedEpochTo(out, e, g, l, "s2"))
	next := env.start(t, "holder-after-clean-move", "s2")
	if got := env.adopt(t, next, "s2"); got != e2 {
		t.Fatalf("clean move adopted epoch %d; want %d", got, e2)
	}
	// A fresh root on another host cannot satisfy this from the source WAL.
	env.requireCutCrashImage(t, "s2", 'B')
	verified, _ := env.h.command(t, "ops", "chunk", "verify", "--volume", "v1",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath,
		"--depth", "data", "--timeout", "60s", "--bytes-per-sec", "0")
	var result layout.VerifyResult
	if err := json.Unmarshal([]byte(verified), &result); err != nil || !result.Complete ||
		len(result.Findings) != 0 || result.Chunks == 0 {
		t.Fatalf("post-move data verify incomplete or corrupt: %s: %v", verified, err)
	}
}

// This is the real filer's public listing of immutable key bindings, not
// directory/intent growth. Every non-200, malformed or truncated listing
// fails instead of supplying a positive crash-stage witness.
func plainDetachStoredKeys(t *testing.T, env *handoffEnv) int {
	t.Helper()
	c := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "http://"+env.cluster.filerHTTP+env.chunkPath+"/v1/keys/?limit=1000", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var listing struct {
		Entries               []json.RawMessage
		ShouldDisplayLoadMore bool
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatal(fmt.Errorf("immutable key listing status %d", resp.StatusCode))
	}
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil || listing.ShouldDisplayLoadMore {
		t.Fatalf("immutable key listing incomplete: %v, more=%t", err, listing.ShouldDisplayLoadMore)
	}
	return len(listing.Entries)
}
