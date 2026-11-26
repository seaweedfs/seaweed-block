//go:build subprocess && chunkstore_integration

package main_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The operations-guide cure for a local store of the wrong size left by a
// 1.0 start with the wrong --durable-blocks (from 1.0.1 such a start is
// refused before any store exists), run as written, in one sitting. The
// setup plants that store: testdata/onezero-wrong-geometry-store.txt holds,
// as readable text, the four files a 3402e0dd (1.0 RC) run left under v2's
// --durable-root
// (65536-block record, --durable-blocks 32768). A start with the right flags
// still fails geometry_mismatch at attach and never serves. The process is
// stopped; then the guard: `sw-block ops chunk show` must print the HEAD with
// epoch 0, no holder and state active (the volume never served, so its local
// files hold no acknowledged write); then the four named files are removed
// from --durable-root and a start with the right flags serves. Afterwards the
// same command shows a claimed HEAD (epoch >= 1, a holder): the guard now
// forbids removing those files.
func TestWrongGeometryCureAsWritten(t *testing.T) {
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	plantStore(t, filepath.Join("testdata", "onezero-wrong-geometry-store.txt"), env.hosts["n-v2"].root)
	// The planted files are byte for byte the captured ones.
	for name, want := range map[string]string{
		"v2.bin":            "e9941d515c8b295dc860df1f3c89a4b36c3cc7d372334aa14527da9928b3de7e",
		"v2.bin.chunkfresh": "02db0d2659c9d48bc15f81a388594fc0e3cf4c780fdc27ea21e0671afc37de19",
		"v2.bin.chunkmap":   "ac6dba36a1b8bb106aaf9f10dda148818b5cf0b545cdf7fe92fa5c8ce52df4fc",
		"v2.replica-floor":  "40ee91af4b55571765a602664a45913101254ccd16974ed3d01fbb8c7efa60e3",
	} {
		data, err := os.ReadFile(filepath.Join(env.hosts["n-v2"].root, name))
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != want {
			t.Fatalf("planted %s: sha256 %s, captured %s", name, got, want)
		}
	}
	stuck := env.h.start(t, "blockvolume-v2-right-flags-on-the-1.0-store", env.h.volume, env.volArgsFor(env.hosts["n-v2"])...)
	err := stuck.wait(90 * time.Second) // stopped (under systemd: systemctl stop)
	t.Logf("right flags on the 1.0 store: exit %v; %q; %q", err, stuck.line("geometry_mismatch"), stuck.line("exiting status="))
	// The attach check refuses it (the store has its own geometry) and the
	// process exits 75 after its retries; the pre-store flag check (exit
	// 78) does not apply to an existing store. Over its whole life it never
	// started a frontend (no *-listening or ublk-ready phase line).
	mismatch := stuck.line("chunk attach check")
	served := stuck.has(`"phase":"nvme-listening"`) || stuck.has(`"phase":"iscsi-listening"`) || stuck.has(`"phase":"ublk-ready"`)
	if exitCode(err) != 75 || !strings.Contains(mismatch, "geometry_mismatch") || !strings.Contains(mismatch, "VOLUME geometry") || served {
		t.Fatalf("setup: the right flags on the 1.0 store must fail the attach check with geometry_mismatch, exit 75, and never serve; exit %v, attach line %q, served %v", err, mismatch, served)
	}
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

// headOf is the first line of `sw-block ops chunk show --volume v`.
func (env *handoffEnv) headOf(t *testing.T, v string) string {
	t.Helper()
	out, _ := env.h.command(t, "ops", "chunk", "show", "--master", env.h.masterAddr, "--volume", v, "--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	head, err := handoffHeadJSON(out)
	if err != nil {
		t.Fatalf("show HEAD for %s: %v\n%s", v, err, out)
	}
	return head
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

// plantStore writes the files a text fixture describes into root: each
// "file <name> <size> <octal mode>" line starts a file of that size (all
// zero, sparse where the file system allows), and each "<offset> <hex>" line
// after it sets those bytes. Lines starting with # are comments.
func plantStore(t *testing.T, fixture, root string) {
	t.Helper()
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	var out *os.File
	closeOut := func() {
		if out != nil {
			if err := out.Close(); err != nil {
				t.Fatal(err)
			}
			out = nil
		}
	}
	defer closeOut()
	for n, line := range strings.Split(strings.ReplaceAll(string(data), "\r", ""), "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) == 0 || strings.HasPrefix(f[0], "#"):
		case f[0] == "file" && len(f) == 4:
			closeOut()
			size, err1 := strconv.ParseInt(f[2], 10, 64)
			mode, err2 := strconv.ParseUint(f[3], 8, 32)
			if err1 != nil || err2 != nil || strings.ContainsAny(f[1], "/\\") {
				t.Fatalf("%s:%d: bad file line %q", fixture, n+1, line)
			}
			if out, err = os.OpenFile(filepath.Join(root, f[1]), os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(mode)); err != nil {
				t.Fatal(err)
			}
			if err := out.Truncate(size); err != nil {
				t.Fatal(err)
			}
		case out != nil && len(f) == 2:
			off, err1 := strconv.ParseInt(f[0], 10, 64)
			b, err2 := hex.DecodeString(f[1])
			if err1 != nil || err2 != nil {
				t.Fatalf("%s:%d: bad bytes line %q", fixture, n+1, line)
			}
			if _, err := out.WriteAt(b, off); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("%s:%d: unexpected line %q", fixture, n+1, line)
		}
	}
}
