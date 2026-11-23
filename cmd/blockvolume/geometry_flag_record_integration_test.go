//go:build subprocess && chunkstore_integration

package main_test

import (
	"os"
	"strings"
	"testing"
	"time"
)

// 1.0.1 red: blockvolume compares its geometry flags with the volume record
// BEFORE it creates a local store, and refuses a mismatch as a permanent
// error. A restore target started with half the record's --durable-blocks
// must exit 78 (no restart under the shipped unit) well inside the 30 s
// recovery watchdog, name both block counts, leave no file under
// --durable-root, and never claim HEAD. On 1.0 it creates the store at the
// wrong size, retries geometry_mismatch for 30 s and exits 75 (restart), and
// a right-flag start on the same root then fails on the leftover store.
func TestWrongGeometryFlagIsRefusedBeforeTheStoreIsCreated(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	b := env.hosts["n-v2"]
	args := append([]string{}, env.volArgsFor(b)...)
	for i := range args {
		if args[i] == "--durable-blocks" {
			args[i+1] = "32768" // the record says 65536
		}
	}
	start := time.Now()
	d := env.h.start(t, "blockvolume-v2-wrong-flag", env.h.volume, args...)
	err := d.wait(90 * time.Second)
	took := time.Since(start)
	refusal := d.line("65536")
	left := listTree(b.root)
	head := env.headOf(t, "v2")
	t.Logf("wrong-flag start: exit %v after %v; refusal line %q; HEAD %s; files under --durable-root:\n%s", err, took.Round(time.Millisecond), refusal, head, left)

	var problems []string
	if code := exitCode(err); code != 78 {
		problems = append(problems, "exit is not 78 (a permanent refusal)")
	}
	if took >= 20*time.Second {
		problems = append(problems, "the refusal waited for the recovery watchdog")
	}
	if refusal == "" || !strings.Contains(refusal, "32768") || !strings.Contains(refusal, "--durable-blocks") {
		problems = append(problems, "no log line names --durable-blocks 32768 against the record's 65536")
	}
	if entries, _ := os.ReadDir(b.root); len(entries) != 0 {
		problems = append(problems, "a local store was created under --durable-root")
	}
	if !strings.Contains(head, `"epoch":0,"holder":""`) {
		problems = append(problems, "HEAD was claimed")
	}
	if len(problems) > 0 {
		t.Fatalf("1.0.1: %s", strings.Join(problems, "; "))
	}
}
