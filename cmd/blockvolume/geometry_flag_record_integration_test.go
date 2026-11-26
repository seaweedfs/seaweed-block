//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"os"
	"path"
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
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
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

// INV-VOL-GEOMETRY-FLAGS-RECORD-001, the corrupt record: in create mode, a
// volume record that is there but does not decode is refused for good (exit
// 78) before any local store exists, never taken as "absent" and never
// retried. The filer stays up and answers the startup checks; v2's VOLUME
// entry is made undecodable. A wrong and a right --durable-blocks alike exit
// 78 and leave nothing under --durable-root. With the entry restored the wrong
// flag is refused (78, mismatch) and the right flag serves. (A filer that
// cannot deliver the record is the retryable case, 75: the unit test.)
func TestCorruptRecordIsRefusedBeforeTheStoreIsCreated(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	b := env.hosts["n-v2"]
	right := append([]string{}, env.volArgsFor(b)...)
	wrong := append([]string{}, right...)
	for i := range wrong {
		if wrong[i] == "--durable-blocks" {
			wrong[i+1] = "32768"
		}
	}
	ctx := context.Background()
	cl := env.cluster.client(t, "")
	dir := path.Join(env.chunkPath, "v2")
	entry, err := cl.LookupEntry(ctx, dir, "VOLUME")
	if err != nil {
		t.Fatal(err)
	}
	record := append([]byte(nil), entry.Content...)
	entry.Content = []byte("{")
	if err := cl.UpdateEntry(ctx, dir, entry, nil); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		args []string
	}{{"wrong", wrong}, {"right", right}} {
		d := env.h.start(t, "blockvolume-v2-"+c.name+"-corrupt", env.h.volume, c.args...)
		err := d.wait(60 * time.Second)
		line := d.line("is unreadable (corrupt:")
		entries, _ := os.ReadDir(b.root)
		t.Logf("%s flag over a corrupt record: exit %v; %q; files under --durable-root %d", c.name, err, line, len(entries))
		if exitCode(err) != 78 || !strings.Contains(line, "no local store was created") || len(entries) != 0 {
			t.Fatalf("1.0.1: a %s-flag first start over a corrupt record must exit 78 before creating anything; exit %v, line %q, %d files", c.name, err, line, len(entries))
		}
	}
	entry.Content = record
	if err := cl.UpdateEntry(ctx, dir, entry, nil); err != nil {
		t.Fatal(err)
	}
	d := env.h.start(t, "blockvolume-v2-wrong-readable", env.h.volume, wrong...)
	if err := d.wait(60 * time.Second); exitCode(err) != 78 {
		t.Fatalf("with the record readable again, the wrong flag must be refused (78); exit %v", err)
	}
	served := env.startTarget(t, "v2", "blockvolume-v2-right-readable")
	served.kill(t)
}

// Guard: a first start with the filer stopped ends at the startup filer
// check (exit 1) before the record check and before any store exists.
func TestFirstStartWithTheFilerDownCreatesNoStore(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	b := env.hosts["n-v2"]
	env.cluster.stopProc(t, "filer")
	d := env.h.start(t, "blockvolume-v2-filer-down", env.h.volume, env.volArgsFor(b)...)
	err := d.wait(120 * time.Second)
	env.cluster.restartProc(t, "filer")
	w4WaitTCP(t, env.cluster.filerGRPC, 30*time.Second)
	entries, _ := os.ReadDir(b.root)
	t.Logf("filer down: exit %v; %q; files under --durable-root %d", err, d.line("needs exactly one filer"), len(entries))
	if exitCode(err) != 1 || d.line("needs exactly one filer") == "" || len(entries) != 0 {
		t.Fatalf("a first start with the filer down must end at the startup filer check without a store; exit %v, %d files", err, len(entries))
	}
}
