//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// 1.0.1 (V1-SPEC r2.65 item 4), RED on 1.0 by design: a volume whose holder
// never started (a restore target created but never attached, as a target
// refused for local headroom is) is deletable: NeverStarted -> Deleted is a
// lease-automaton transition, not a flag. The operator runs volume-delete;
// it completes, the target's catalog entry and HEAD are gone, and the source
// volume and its snapshot are untouched.
//
// On 1.0 volume-delete is refused: it wants a clean revocation ACK that a
// never-started holder cannot send (the operator guide's known limit).
func TestNeverStartedHolderIsDeletable(t *testing.T) {
	env := startHandoffEnvWith(t, 1, []string{"v2"})
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('N', 0))
	env.snapshot(t, "s1", "v1", "s1")

	// The target exists in the catalog; its blockvolume never starts.
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)

	out, errOut, code := env.h.run(t, "ops", "chunk", "volume-delete", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC,
		"--chunk-path", env.chunkPath, "--master", env.h.masterAddr, "--request-id", "del-v2-never-started")
	var del struct{ Completed bool }
	if code != 0 || json.Unmarshal([]byte(out), &del) != nil || !del.Completed {
		t.Fatalf("1.0.1: volume-delete of a never-started target must complete; exit %d:\n%s\n%s", code, out, errOut)
	}
	shown, _, _ := env.h.run(t, "ops", "chunk", "show", "--volume", "v2", "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	if strings.Contains(shown, `"gen"`) {
		t.Fatalf("1.0.1: the deleted target still has a HEAD:\n%s", shown)
	}
	// The source and its snapshot are untouched.
	if got := env.readBlock(t, "s1", 0); string(got) != string(cutCrashBlock('N', 0)) {
		t.Fatalf("the source's block changed")
	}
	if snaps := env.snapshots(t, "v1", ""); len(snaps) != 1 {
		t.Fatalf("the source has %d snapshots after the target's delete; want 1", len(snaps))
	}
}
