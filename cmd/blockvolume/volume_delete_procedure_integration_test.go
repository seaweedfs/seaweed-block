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
// binaries: seal, revoke and wait for the ACK, delete the snapshots, delete
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
// refusal proves the lease is still live; the timestamp only schedules
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

// The CLI error body, rather than a substring in combined output, is the
// authority for retry timing. Every refusal class stays distinguishable.
func TestDeleteLeaseHorizonRequiresTypedRefusal(t *testing.T) {
	const valid = `{"code":"not_admitted","message":"workspace delete: live until 2026-11-26T13:00:00.125Z: lease expiry plus clock-skew margin has not elapsed"}`
	for _, tc := range []struct {
		name, out string
		code      int
		ok        bool
	}{
		{"typed horizon", valid, 1, true},
		{"successful exit cannot refuse", valid, 0, false},
		{"usage failure", valid, 2, false},
		{"plain stderr is not evidence", "not_admitted: live until 2026-11-26T13:00:00.125Z", 1, false},
		{"dependent refusal", `{"code":"snapshot_has_dependents","message":"snapshot s1"}`, 1, false},
		{"uncertain lease", `{"code":"not_admitted","message":"restart required"}`, 1, false},
		{"missing time", `{"code":"not_admitted","message":"live until "}`, 1, false},
		{"invalid time", `{"code":"not_admitted","message":"live until tomorrow: refused"}`, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			until, err := deleteLeaseHorizon(tc.out, tc.code)
			if (err == nil) != tc.ok {
				t.Fatalf("horizon %s error %v, want valid=%v", until, err, tc.ok)
			}
			if tc.ok && until.Format(time.RFC3339Nano) != "2026-11-26T13:00:00.125Z" {
				t.Fatalf("wrong horizon %s", until)
			}
		})
	}
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

// TestProcedureDeleteAVolume: the page's steps. A volume with a snapshot is
// sealed, revoked (blockvolume sends the ACK and exits 0 by itself) and its
// ACK seen; volume-delete is refused while the snapshot exists, with nothing
// deleted; the snapshot is deleted; volume-delete then answers deleted,
// `chunk show` shows HEAD deleted and the pool is empty, and a rerun with
// the same request id completes again.
func TestProcedureDeleteAVolume(t *testing.T) {
	env := startHandoffEnv(t, 1)
	holder := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, pattern("delete-a-volume!"))
	env.snapshot(t, "s1", "v1", "s1")

	g := env.seal(t, "s1", e)
	env.revoke(t, e, g)
	if err := holder.wait(30 * time.Second); err != nil {
		t.Fatalf("s1 after the revocation: %v; want a clean exit by itself", err)
	}
	env.waitAck(t, e, g)

	state0, n0 := env.headShown(t)
	out, errOut, code := env.volumeDelete(t, "del-v1")
	if code == 0 || !strings.Contains(out+errOut, "has snapshots (s1)") {
		t.Fatalf("volume-delete with a snapshot: exit %d %s %s; want refused, naming s1", code, out, errOut)
	}
	if state, n := env.headShown(t); state != "detached" || state != state0 || n != n0 || n == 0 {
		t.Fatalf("after the refused delete HEAD is %q with %d chunks; want it as before (%q, %d chunks): nothing deleted", state, n, state0, n0)
	}
	env.deleteSnapshot(t, "v1", "s1")
	out, errOut, code = env.volumeDelete(t, "del-v1")
	requireDeleteCompleted(t, out, errOut, code)
	env.requireDeletedForGood(t, "del-v1")
}

// TestProcedureDeleteAfterASignalStop: a sealed holder stopped by SIGTERM
// stays down. The first delete refuses the live lease and changes nothing;
// the same request completes after its printed horizon without a restart or
// ACK. A retry completes again, and a late start cannot claim or serve.
func TestProcedureDeleteAfterASignalStop(t *testing.T) {
	env := startHandoffEnv(t, 1)
	holder := env.start(t, "blockvolume-s1", "s1")
	e := env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, pattern("delete-after-sig"))
	env.seal(t, "s1", e)
	holder.term(t, 30*time.Second)
	stopped := func() {
		t.Helper()
		select {
		case <-holder.done:
		default:
			t.Fatal("signal-stopped holder is still running")
		}
		if listening(env.hosts["s1"].status) {
			t.Fatal("signal-stopped holder listened again before deletion")
		}
	}
	stopped()
	state0, n0 := env.headShown(t)
	out, errOut, code := env.volumeDelete(t, "del-v1")
	if _, err := deleteLeaseHorizon(out, code); err != nil {
		t.Fatalf("volume-delete after signal stop: exit %d %s %s; want typed live-until refusal: %v", code, out, errOut, err)
	}
	if state, n := env.headShown(t); state != "detached" || state != state0 || n != n0 || n == 0 {
		t.Fatalf("refused delete changed HEAD/pool: %q %d; before %q %d", state, n, state0, n0)
	}
	stopped()
	out, errOut, code = env.deletePastPrintedHorizon(t, "v1", "del-v1", true)
	requireDeleteCompleted(t, out, errOut, code)
	stopped()
	env.requireDeletedForGood(t, "del-v1")
	late := env.start(t, "blockvolume-s1-late", "s1")
	err := late.wait(60 * time.Second)
	served := late.has("blockvolume: backend admits I/O: starting the frontends") || volumeFrontendReady(env.hosts["s1"].status, "v1")
	keepFullLog(t, late)
	if err == nil || strings.Contains(fmt.Sprint(err), "still running") || served {
		t.Fatalf("late start after deletion must exit without serving: exit=%v served=%v", err, served)
	}
	if state, n := env.headShown(t); state != "deleted" || n != 0 {
		t.Fatalf("late start changed deleted HEAD/pool: %q %d", state, n)
	}
}
