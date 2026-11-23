//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
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

// requireDeleteCompleted checks step 4's answer: admitted, committed, HEAD
// marked, completed.
func requireDeleteCompleted(t *testing.T, out, errOut string, code int) {
	t.Helper()
	var res struct {
		Admitted, Committed, HeadMarked, Completed bool
	}
	if code != 0 || json.Unmarshal([]byte(out), &res) != nil || !res.Admitted || !res.Committed || !res.HeadMarked || !res.Completed {
		t.Fatalf("volume-delete: exit %d %s %s; want admitted, committed, headMarked, completed", code, out, errOut)
	}
}

// headShown returns HEAD's state and chunk count from `ops chunk show`.
func (env *handoffEnv) headShown(t *testing.T) (string, int) {
	t.Helper()
	out, _ := env.h.command(t, env.chunkShow()...)
	var body struct {
		Head *struct {
			State      string `json:"state"`
			ChunkCount int    `json:"chunkCount"`
		} `json:"head"`
	}
	if err := json.Unmarshal([]byte(out), &body); err != nil || body.Head == nil {
		t.Fatalf("chunk show: %v %s", err, out)
	}
	return body.Head.State, body.Head.ChunkCount
}

// requireDeletedForGood checks the page's end state ("state": "deleted",
// "chunkCount": 0) and that a rerun with the same request id completes
// again.
func (env *handoffEnv) requireDeletedForGood(t *testing.T, request string) {
	t.Helper()
	if state, n := env.headShown(t); state != "deleted" || n != 0 {
		t.Fatalf("chunk show after the delete: state %q, chunkCount %d; want deleted, 0", state, n)
	}
	out, errOut, code := env.volumeDelete(t, request)
	requireDeleteCompleted(t, out, errOut, code)
}

// TestProcedureDeleteAVolume: the page's steps. A volume with a snapshot is
// sealed, revoked (blockvolume sends the ACK and exits 0 by itself) and its
// ACK seen; volume-delete is refused while the snapshot exists, with nothing
// deleted; the snapshot is deleted; volume-delete then answers admitted,
// committed, headMarked, completed, `chunk show` shows HEAD deleted with no
// chunks, and a rerun with the same request id completes again.
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

	out, errOut, code := env.volumeDelete(t, "del-v1")
	if code == 0 || !strings.Contains(out+errOut, "has snapshots (s1)") {
		t.Fatalf("volume-delete with a snapshot: exit %d %s %s; want refused, naming s1", code, out, errOut)
	}
	if state, _ := env.headShown(t); state != "detached" {
		t.Fatalf("after the refused delete HEAD is %q; want detached (nothing deleted)", state)
	}
	env.deleteSnapshot(t, "v1", "s1")
	out, errOut, code = env.volumeDelete(t, "del-v1")
	requireDeleteCompleted(t, out, errOut, code)
	env.requireDeletedForGood(t, "del-v1")
}

// TestProcedureDeleteAfterASignalStop: the page's recovery. The volume is
// sealed and blockvolume is then stopped with SIGTERM, so the ACK is
// skipped: volume-delete is refused (live authority requires a clean
// revocation ACK) and nothing is deleted. blockvolume is started again with
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
	out, errOut, code := env.volumeDelete(t, "del-v1")
	if code == 0 || !strings.Contains(out+errOut, "live authority requires clean revocation ACK") {
		t.Fatalf("volume-delete after a signal stop: exit %d %s %s; want refused for the missing ACK", code, out, errOut)
	}
	if state, _ := env.headShown(t); state != "detached" {
		t.Fatalf("after the refused delete HEAD is %q; want detached (nothing deleted)", state)
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
	out, errOut, code = env.volumeDelete(t, "del-v1")
	requireDeleteCompleted(t, out, errOut, code)
	env.requireDeletedForGood(t, "del-v1")
}
