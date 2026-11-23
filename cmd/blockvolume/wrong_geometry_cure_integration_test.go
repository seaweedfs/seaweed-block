//go:build subprocess && chunkstore_integration

package main_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The operations-guide cure for a blockvolume started with the wrong
// --durable-blocks, run as written, in one sitting: the process is stopped
// first; then the guard: `sw-block ops chunk show` must print the HEAD with
// epoch 0, no holder and state active (the volume never served, so its local
// files hold no acknowledged write); then the four named files are removed
// from --durable-root and a start with the right flags serves. Afterwards the
// same command shows a claimed HEAD (epoch >= 1, a holder): the guard now
// forbids removing those files.
func TestWrongGeometryCureAsWritten(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.strandTarget(t, "v2") // stopped: it exited 75 (under systemd: systemctl stop)
	b := env.hosts["n-v2"]
	guard := env.headOf(t, "v2")
	t.Logf("guard: sw-block ops chunk show --volume v2: %s", guard)
	if !strings.Contains(guard, `"epoch":0,"holder":""`) || !strings.Contains(guard, `"state":"active"`) {
		t.Fatalf("the guard does not show a never-claimed HEAD: %s", guard)
	}
	for _, name := range []string{"v2.bin", "v2.bin.chunkfresh", "v2.bin.chunkmap", "v2.replica-floor"} {
		if err := os.Remove(filepath.Join(b.root, name)); err != nil {
			t.Fatalf("remove %s: %v", name, err)
		}
	}
	t.Logf("left under --durable-root after the removal:\n%s", listTree(b.root))
	d := env.startTarget(t, "v2", "blockvolume-v2-cured")
	defer d.kill(t)
	after := env.headOf(t, "v2")
	t.Logf("after the cure: %s", after)
	if strings.Contains(after, `"epoch":0`) || strings.Contains(after, `"holder":""`) {
		t.Fatalf("a served volume's HEAD must show a claim (the guard then forbids removal): %s", after)
	}
}

// startSourceWithSnapshot starts v1 on s1 with one written block and its
// snapshot s1; the targets are restore targets of it, none started.
func startSourceWithSnapshot(t *testing.T, targets ...string) *handoffEnv {
	env := startHandoffEnvWith(t, 1, targets)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	env.adopt(t, holder, "s1")
	env.writeBlock(t, "s1", 0, cutCrashBlock('A', 0))
	env.snapshot(t, "s1", "v1", "s1")
	return env
}

// strandTarget restores v from v1's snapshot s1 and starts its blockvolume
// with half the cluster spec's --durable-blocks (a wrong flag): the master
// assigns it, it creates its local store at the wrong size, never becomes
// promotion-ready and exits 75; its HEAD was never claimed (epoch 0, no
// holder).
func (env *handoffEnv) strandTarget(t *testing.T, v string) {
	t.Helper()
	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", v,
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	args := append([]string{}, env.volArgsFor(env.hosts["n-"+v])...)
	for i := range args {
		if args[i] == "--durable-blocks" {
			args[i+1] = "32768"
		}
	}
	d := env.h.start(t, "blockvolume-"+v+"-wrong-geometry", env.h.volume, args...)
	err := d.wait(90 * time.Second)
	head := env.headOf(t, v)
	if exitCode(err) != 75 || !d.has("adopted as PRIMARY") || !strings.Contains(head, `"epoch":0`) || !strings.Contains(head, `"holder":""`) {
		t.Fatalf("setup: %s is not assigned-never-claimed: exit %v, adopted %v, HEAD %s", v, err, d.has("adopted as PRIMARY"), head)
	}
}

// headOf is the first line of `sw-block ops chunk show --volume v`.
func (env *handoffEnv) headOf(t *testing.T, v string) string {
	out, _, _ := env.h.run(t, "ops", "chunk", "show", "--volume", v, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	return firstLine(out)
}

func listTree(root string) string {
	var sb strings.Builder
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			fmt.Fprintf(&sb, "  %s %d\n", strings.TrimPrefix(p, root), info.Size())
		}
		return nil
	})
	return sb.String()
}
