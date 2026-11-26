//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The volume-lifecycle page's "Delete a volume", end to end with the real
// binaries: seal, revoke, ACK, wait for the horizon, release dependents, delete
// the volume; and the page's recovery when blockvolume was stopped by a
// signal after the seal (the ACK was skipped).

// volumeDelete runs the page's step 4 and returns the answer and the exit
// status.
func (env *handoffEnv) volumeDelete(t *testing.T, request string) (string, string, int) {
	t.Helper()
	return env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v1", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath,
		"--master", env.h.masterAddr, "--request-id", request)
}

// deletePastPrintedHorizon follows volume-lifecycle steps 2-4. The first
// refusal proves ACK alone is insufficient; the timestamp only schedules
// retries. The CLI verdict decides success. All retries keep the same id and
// the first T+5s deadline, even if another lease appears (which fails the test).
func (env *handoffEnv) deletePastPrintedHorizon(t *testing.T, volume, request string, requireLive bool) (string, string, int) {
	t.Helper()
	call := func(ctx context.Context) (string, string, int) {
		out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "volume-delete", "--volume", volume,
			"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath,
			"--master", env.h.masterAddr, "--request-id", request)
		if err != nil {
			t.Fatalf("volume-delete %s: %v", volume, err)
		}
		return out, errOut, code
	}
	first, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	out, errOut, code := call(first)
	until, err := deleteLeaseHorizon(out, code)
	if err != nil {
		if requireLive {
			t.Fatalf("first delete must refuse the live lease: %v; %s %s", err, out, errOut)
		}
		return out, errOut, code
	}
	if wait := time.Until(until); wait < 0 || wait > 90*time.Second {
		t.Fatalf("first refusal horizon must be upcoming and within 90s, got %s: %s", until, out)
	}
	ctx, cancel := context.WithDeadline(context.Background(), until.Add(5*time.Second))
	defer cancel()
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("delete did not resolve by original horizon + 5s: %s %s", out, errOut)
		case <-timer.C:
		}
		out, errOut, code = call(ctx)
		next, err := deleteLeaseHorizon(out, code)
		if err != nil {
			return out, errOut, code
		}
		if !next.Equal(until) {
			t.Fatalf("lease horizon changed from %s to %s; refusing to widen deadline", until, next)
		}
		timer.Reset(100 * time.Millisecond)
	}
}

// deleteLeaseHorizon accepts only the documented typed CLI refusal, never
// a matching stderr fragment or a successful exit with an error-shaped body.
func deleteLeaseHorizon(out string, code int) (time.Time, error) {
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if code != 1 {
		return time.Time{}, fmt.Errorf("exit %d is not refusal", code)
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil {
		return time.Time{}, err
	}
	if body.Code != "not_admitted" {
		return time.Time{}, fmt.Errorf("code %q is not not_admitted", body.Code)
	}
	_, rest, ok := strings.Cut(body.Message, "live until ")
	if !ok {
		return time.Time{}, fmt.Errorf("missing lease horizon")
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return time.Time{}, fmt.Errorf("empty lease horizon")
	}
	return time.Parse(time.RFC3339Nano, strings.TrimSuffix(fields[0], ":"))
}

// requireDeleteCompleted checks step 4's documented state:"deleted" result
// (volume-lifecycle.md, "Delete a volume", step 4). Retries use the same
// request id and must complete again; the CLI reports no replayed flag.
func requireDeleteCompleted(t *testing.T, out, errOut string, code int) {
	t.Helper()
	if code != 0 || !handoffDeleteCompleted(out) {
		t.Fatalf("volume-delete: exit %d %s %s; want a volume directory and state deleted", code, out, errOut)
	}
}

// headShown reads head.State and independently counts stored pool objects.
// volume-lifecycle.md step 4 specifies that show has no chunkCount field;
// its object-listing alternative preserves the checks that nothing was
// removed on refusal and no chunks remain after completion.
func (env *handoffEnv) headShown(t *testing.T) (string, int) {
	t.Helper()
	var head struct {
		State string `json:"state"`
	}
	out := env.headOf(t, "v1")
	if err := json.Unmarshal([]byte(out), &head); err != nil {
		t.Fatalf("chunk show: %v %s", err, out)
	}
	return head.State, env.poolObjects(t, "v1")
}

// requireDeletedForGood checks the page's deleted HEAD and empty pool,
// then repeats the delete with the same request id and checks both again.
func (env *handoffEnv) requireDeletedForGood(t *testing.T, request string) {
	t.Helper()
	if state, n := env.headShown(t); state != "deleted" || n != 0 {
		t.Fatalf("after the delete: HEAD state %q, pool objects %d; want deleted, 0", state, n)
	}
	out, errOut, code := env.volumeDelete(t, request)
	requireDeleteCompleted(t, out, errOut, code)
	if state, n := env.headShown(t); state != "deleted" || n != 0 {
		t.Fatalf("after the repeated delete: HEAD state %q, pool objects %d; want deleted, 0", state, n)
	}
}

// TestProcedureDeleteAVolume: ACK does not bypass the lease horizon; a
// restored-volume dependent blocks deletion, while ordinary snapshots do not.
// Refusals preserve HEAD/pool; completion empties the pool and replays by id.
func TestProcedureDeleteAVolume(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	holder := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, pattern("delete-a-volume!"))
	env.snapshot(t, "s1", "v1", "s1")
	env.snapshot(t, "s1", "v1", "plain")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1",
		"--snapshot", "s1", "--target", "v2", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)

	g := env.seal(t, "s1", e)
	env.revoke(t, e, g)
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)

	state0, n0 := env.headShown(t)
	out, errOut, code := env.deletePastPrintedHorizon(t, "v1", "del-v1", true)
	var refusal struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(out), &refusal); err != nil || code != 1 || refusal.Code != "snapshot_has_dependents" || !strings.Contains(refusal.Message, `snapshot "s1"`) {
		t.Fatalf("volume-delete with a dependent: exit %d %s %s; want typed snapshot_has_dependents naming s1", code, out, errOut)
	}
	if state, n := env.headShown(t); state != "detached" || state != state0 || n != n0 || n == 0 {
		t.Fatalf("after the refused delete HEAD is %q with %d chunks; want it as before (%q, %d chunks): nothing deleted", state, n, state0, n0)
	}
	// v2 never started; delete it through the public API (and its own horizon,
	// if the master granted a line). This releases s1's dependent.
	out, errOut, code = env.deletePastPrintedHorizon(t, "v2", "del-v2", false)
	requireDeleteCompleted(t, out, errOut, code)
	if state, n := env.headOf(t, "v2"), env.poolObjects(t, "v2"); !strings.Contains(state, `"state":"deleted"`) || n != 0 {
		t.Fatalf("restored target not deleted: HEAD %s pool %d", state, n)
	}
	// Both snapshots still exist, now without dependents: plain snapshots
	// must be removed by the volume delete itself, not by this fixture.
	snapshots, _ := env.h.command(t, "ops", "chunk", "snapshots", "--master", env.h.masterAddr,
		"--volume", "v1", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	var names []struct {
		Name       string   `json:"name"`
		Dependents []string `json:"dependents"`
	}
	if err := json.Unmarshal([]byte(snapshots), &names); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, snap := range names {
		if len(snap.Dependents) != 0 {
			t.Fatalf("dependent not released: %s", snapshots)
		}
		seen[snap.Name] = true
	}
	if !seen["s1"] || !seen["plain"] {
		t.Fatalf("plain snapshots disappeared before volume-delete: %s", snapshots)
	}
	out, errOut, code = env.volumeDelete(t, "del-v1")
	requireDeleteCompleted(t, out, errOut, code)
	env.requireDeletedForGood(t, "del-v1")
}

// TestProcedureDeleteAfterASignalStop: the page's recovery. The volume is
// sealed and blockvolume is then stopped with SIGTERM, so the ACK is
// skipped: volume-delete is refused while the lease is live and nothing
// is deleted. blockvolume is started again with
// the same flags and local root; it is revoked and sends the ACK (exit 0 by
// itself); volume-delete then completes, HEAD is deleted with no chunks, and
// a rerun with the same request id completes again.
func TestProcedureDeleteAfterASignalStop(t *testing.T) {
	env := startHandoffEnv(t, 1)
	holder := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, pattern("delete-after-sig"))

	g := env.seal(t, "s1", e)
	holder.term(t, 30*time.Second)
	state0, n0 := env.headShown(t)
	out, errOut, code := env.volumeDelete(t, "del-v1")
	if _, err := deleteLeaseHorizon(out, code); err != nil {
		t.Fatalf("volume-delete after a signal stop: exit %d %s %s; want typed live-lease refusal: %v", code, out, errOut, err)
	}
	if state, n := env.headShown(t); state != "detached" || state != state0 || n != n0 || n == 0 {
		t.Fatalf("after the refused delete HEAD is %q with %d chunks; want it as before (%q, %d chunks): nothing deleted", state, n, state0, n0)
	}

	again := env.start(t, "blockvolume-s1-again", "s1")
	poll(t, 60*time.Second, "the restarted holder never listened on its status address", func() bool {
		return listening(env.hosts["s1"].status)
	})
	env.revoke(t, e, g)
	if err := again.wait(60 * time.Second); err != nil {
		t.Fatalf("the restarted s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)
	out, errOut, code = env.deletePastPrintedHorizon(t, "v1", "del-v1", true)
	requireDeleteCompleted(t, out, errOut, code)
	env.requireDeletedForGood(t, "del-v1")
}
