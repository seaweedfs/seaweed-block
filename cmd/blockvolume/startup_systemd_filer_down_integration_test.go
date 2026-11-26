//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The operator guide's procedure for a first start during a long filer
// outage, run as written under real systemd (root): the blockvolume unit,
// with the shipped restart policy and a short start limit, starts while the
// filer is down; each start fails at the startup filer check (exit 1, no
// store) until systemd stops retrying (its start limit). The filer comes
// back; `systemctl
// reset-failed <unit>` and `systemctl start <unit>` then serve the volume.
//
// A runtime unit file (/run/systemd/system) stands for the installed unit:
// a transient systemd-run unit would be removed by reset-failed.
func TestSystemdFirstStartDuringAFilerOutageThenResetFailedServes(t *testing.T) {
	if os.Getenv("GAP1_SYSTEMD_AUTHORIZED") != "1" {
		t.Fatal("explicit real-systemd qualification authorization required; no skip counts as proof")
	}
	if os.Geteuid() != 0 {
		t.Fatal("systemd qualification needs root")
	}
	env := startSourceWithSnapshot(t, "v2")
	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "s1", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	b := env.hosts["n-v2"]

	shipped := gap1ShippedPolicy(t)
	unit := fmt.Sprintf("swb-c04-filer-outage-%d.service", os.Getpid())
	unitPath := filepath.Join("/run/systemd/system", unit)
	// Under systemd blockvolume refuses the test-only lock directory: the
	// unit uses the real one, as an installed unit does.
	quoted := []string{fmt.Sprintf("%q", env.h.volume)}
	args := env.volArgsFor(b)
	for i := 0; i < len(args); i++ {
		if args[i] == "--volume-lock-dir-test-only" {
			i++
			continue
		}
		quoted = append(quoted, fmt.Sprintf("%q", args[i]))
	}
	// Create the lock directory and its parent only if missing, and remove
	// only what this test created (child first); nothing pre-existing is
	// touched.
	const lockDir = "/run/sw-block/locks"
	var created []string
	for _, dir := range []string{filepath.Dir(lockDir), lockDir} {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			created = append(created, dir)
		}
	}
	t.Cleanup(func() {
		for i := len(created) - 1; i >= 0; i-- {
			_ = os.Remove(created[i])
		}
	})
	spec := strings.Join([]string{
		"[Unit]",
		"Description=sw-block test unit: first start during a filer outage",
		"StartLimitIntervalSec=600",
		"StartLimitBurst=2",
		"[Service]",
		"Type=exec",
		"ExecStart=" + strings.Join(quoted, " "),
		"Restart=on-failure",
		"RestartSec=" + shipped["RestartSec"],
		"RestartPreventExitStatus=" + shipped["RestartPreventExitStatus"],
		"TimeoutStopSec=" + shipped["TimeoutStopSec"],
		"",
	}, "\n")
	systemctl := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
		return string(out), err
	}
	t.Cleanup(func() {
		gap1CleanupUnit(t, unit)
		_ = os.Remove(unitPath)
		_, _ = systemctl("daemon-reload")
	})
	if err := os.WriteFile(unitPath, []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := systemctl("daemon-reload"); err != nil {
		t.Fatalf("daemon-reload: %v %s", err, out)
	}

	env.cluster.stopProc(t, "filer")
	if out, err := systemctl("start", "--no-block", unit); err != nil {
		t.Fatalf("start %s: %v %s", unit, err, out)
	}
	// systemd gives up when the start limit is hit: the unit stays failed
	// after StartLimitBurst restarts, and the journal says "Start request
	// repeated too quickly" (systemd 249 keeps Result=exit-code).
	var state map[string]string
	poll(t, 8*time.Minute, unit+" never stopped retrying with the filer down", func() bool {
		s, err := gap1ReadUnit(unit)
		state = s
		return err == nil && s["ActiveState"] == "failed" && s["NRestarts"] == "2"
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	journal, _ := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager").CombinedOutput()
	cancel()
	entries, _ := os.ReadDir(b.root)
	t.Logf("filer down: %s gave up: Result %s, NRestarts %s, ExecMainStatus %s, ActiveState %s; files under --durable-root %d",
		unit, state["Result"], state["NRestarts"], state["ExecMainStatus"], state["ActiveState"], len(entries))
	if !strings.Contains(string(journal), "Start request repeated too quickly") || state["ExecMainStatus"] != "1" {
		t.Fatalf("the unit did not stop on its start limit after failing at the startup filer check (status %s):\n%s", state["ExecMainStatus"], journal)
	}
	if len(entries) != 0 {
		t.Fatalf("the failed starts created %d files under --durable-root", len(entries))
	}

	env.cluster.restartProc(t, "filer")
	w4WaitTCP(t, env.cluster.filerGRPC, 30*time.Second)
	// The guide, as written: reset-failed, then start.
	if out, err := systemctl("reset-failed", unit); err != nil {
		t.Fatalf("systemctl reset-failed %s: %v %s", unit, err, out)
	}
	if out, err := systemctl("start", unit); err != nil {
		t.Fatalf("systemctl start %s: %v %s", unit, err, out)
	}
	poll(t, 2*time.Minute, "v2 never served after reset-failed and start", func() bool {
		return volumeFrontendReady(b.status, "v2")
	})
	head := env.headOf(t, "v2")
	if strings.Contains(head, `"holder":""`) {
		t.Fatalf("v2 serves but its HEAD is not claimed: %s", head)
	}
	t.Logf("after reset-failed + start: v2 serves; HEAD %s", head)
}
