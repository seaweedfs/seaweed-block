//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"syscall"
	"testing"
	"time"
)

// P12.6 / INV-WS-SEAL-REPLAY-001: a SIGKILL during plain-seal upload
// must not strand the hand-over. The kill is witnessed by a new immutable
// key and an unchanged active HEAD, not by a delay or a successful CLI exit.
func TestProcedureKillDuringPlainDetachThenCompleteHandoff(t *testing.T) {
	env := startHandoffEnv(t, 1)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off", "--chunk-upload-concurrency", "1"}
	holder := env.start(t, "holder-before-detach-kill", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeCutCrashImage(t, 'A')
	env.h.command(t, "ops", "chunk", "cut", "--volume", "v1", "--status-addr", env.hosts["s1"].status)
	out, _ := env.h.command(t, env.chunkShow()...)
	before, err := parseHead(out)
	if err != nil || before.State != "active" || before.Epoch != e {
		t.Fatalf("baseline HEAD: %+v: %v", before, err)
	}
	env.writeCutCrashImage(t, 'B')
	keys := plainDetachStoredKeys(t, env)
	cli := env.h.start(t, "plain-detach-interrupted", env.h.cli, "ops", "chunk", "detach", "--plain",
		"--volume", "v1", "--status-addr", env.hosts["s1"].status, "--timeout", "120s")
	poll(t, 60*time.Second, "plain detach stored no new immutable chunk", func() bool {
		select {
		case <-cli.done:
			t.Fatal("detach finished before the mid-upload kill witness")
		default:
		}
		return plainDetachStoredKeys(t, env) > keys
	})
	// Stop only this child before reading the remote proof, so the uploader
	// cannot cross its CAS while the test verifies where the kill will land.
	if err := holder.proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	out, _ = env.h.command(t, env.chunkShow()...)
	atKill, err := parseHead(out)
	if err != nil || atKill.Gen != before.Gen || atKill.State != "active" || atKill.RemoteLSN != before.RemoteLSN {
		t.Fatalf("missed mid-upload window: before=%+v stopped=%+v: %v", before, atKill, err)
	}
	holder.kill(t)
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
	if again := env.seal(t, "s1", e); again != g {
		t.Fatalf("plain seal replay changed generation %d -> %d", g, again)
	}
	env.revoke(t, e, g)
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("revoked restarted holder did not exit cleanly: %v", err)
	}
	env.waitAck(t, e, g)
	out, _ = env.h.command(t, env.moveClean(e, "s1", "move-v1-1")...)
	_, mustU := fatalOn(t)
	e2 := mustU(movedEpoch(out, e, g, l))
	next := env.start(t, "holder-after-clean-move", "s1")
	if got := env.adopt(t, next, "s1"); got != e2 {
		t.Fatalf("clean move adopted epoch %d; want %d", got, e2)
	}
	env.requireCutCrashImage(t, "s1", 'B')
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
