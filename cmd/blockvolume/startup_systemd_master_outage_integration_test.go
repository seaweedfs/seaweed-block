//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A long blockmaster outage under real systemd (root), with the shipped
// unit's restart policy AND its shipped start limit: a serving volume's
// blockvolume fences, exits 75 and is restarted by the unit; each start
// waits for a grant and exits 75 again. The test keeps the master down until
// systemd stops retrying (the start limit) or 12 minutes pass, records when
// and after how many restarts, then brings the master back and records
// whether the volume serves by itself, and, if the unit failed, how long
// `systemctl reset-failed` + `systemctl start` take to serve it.
//
// A runtime unit file (/run/systemd/system) stands for the installed unit.
func TestSystemdMasterOutagePastTheStartLimit(t *testing.T) {
	if os.Getenv("GAP1_SYSTEMD_AUTHORIZED") != "1" {
		t.Fatal("explicit real-systemd qualification authorization required; no skip counts as proof")
	}
	if os.Geteuid() != 0 {
		t.Fatal("systemd qualification needs root")
	}
	shipped := gap1ShippedPolicy(t)
	interval, burst := shippedStartLimit(t)

	env := startHandoffEnv(t, 1)
	b := env.hosts["s1"]
	unit := fmt.Sprintf("swb-c04-master-outage-%d.service", os.Getpid())
	writeRuntimeUnit(t, unit, env.h.volume, env.volArgsFor(b), []string{
		"StartLimitIntervalSec=" + interval,
		"StartLimitBurst=" + burst,
	}, []string{
		"Restart=on-failure",
		"RestartSec=" + shipped["RestartSec"],
		"RestartPreventExitStatus=" + shipped["RestartPreventExitStatus"],
		"TimeoutStopSec=" + shipped["TimeoutStopSec"],
	})
	if out, err := runSystemctl("start", unit); err != nil {
		t.Fatalf("start %s: %v %s", unit, err, out)
	}
	poll(t, 2*time.Minute, "v1 never served under the unit", func() bool { return volumeFrontendReady(b.status, "v1") })

	killAt := time.Now()
	env.masterProcess.kill(t)
	var state map[string]string
	gaveUp := false
	for deadline := killAt.Add(12 * time.Minute); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		s, err := gap1ReadUnit(unit)
		if err == nil {
			state = s
			if s["ActiveState"] == "failed" {
				gaveUp = true
				break
			}
		}
	}
	downFor := time.Since(killAt)
	journal := unitJournal(unit)
	t.Logf("PHASE0-SYSTEMD master down %v: unit gave up (start limit %s in %s) %v; NRestarts %s; ActiveState %s; Result %s; 'Start request repeated too quickly' %v",
		downFor.Round(time.Second), burst, interval, gaveUp, state["NRestarts"], state["ActiveState"], state["Result"], strings.Contains(journal, "Start request repeated too quickly"))

	env.masterProcess = env.h.start(t, "blockmaster-back", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	backAt := time.Now()
	served := false
	for deadline := backAt.Add(90 * time.Second); time.Now().Before(deadline) && !served; time.Sleep(time.Second) {
		served = volumeFrontendReady(b.status, "v1")
	}
	if served {
		t.Logf("PHASE0-SYSTEMD with the master back, v1 served by itself after %v", time.Since(backAt).Round(time.Millisecond))
		return
	}
	t.Logf("PHASE0-SYSTEMD with the master back, v1 did not serve by itself within 90 s (unit %s)", state["ActiveState"])
	if out, err := runSystemctl("reset-failed", unit); err != nil {
		t.Fatalf("systemctl reset-failed %s: %v %s", unit, err, out)
	}
	resetAt := time.Now()
	if out, err := runSystemctl("start", unit); err != nil {
		t.Fatalf("systemctl start %s: %v %s", unit, err, out)
	}
	poll(t, 3*time.Minute, "v1 never served after reset-failed and start", func() bool { return volumeFrontendReady(b.status, "v1") })
	t.Logf("PHASE0-SYSTEMD after reset-failed + start, v1 served after %v", time.Since(resetAt).Round(time.Millisecond))
}

// shippedStartLimit reads StartLimitIntervalSec and StartLimitBurst from the
// shipped sw-blockvolume.service.
func shippedStartLimit(t *testing.T) (interval, burst string) {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "deploy", "systemd", "sw-blockvolume.service"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if v, ok := strings.CutPrefix(line, "StartLimitIntervalSec="); ok {
			interval = v
		}
		if v, ok := strings.CutPrefix(line, "StartLimitBurst="); ok {
			burst = v
		}
	}
	if _, err := strconv.Atoi(burst); err != nil || interval == "" {
		t.Fatalf("shipped unit start limit unreadable: interval %q burst %q", interval, burst)
	}
	return interval, burst
}

// writeRuntimeUnit installs a runtime unit (removed, with daemon-reload,
// when the test ends) that runs exe with args, minus the test-only lock
// directory: under systemd blockvolume uses the real one, which is created
// for the test when missing and removed only if this test created it.
func writeRuntimeUnit(t *testing.T, unit, exe string, args, unitSection, serviceSection []string) {
	t.Helper()
	quoted := []string{fmt.Sprintf("%q", exe)}
	for i := 0; i < len(args); i++ {
		if args[i] == "--volume-lock-dir-test-only" {
			i++
			continue
		}
		quoted = append(quoted, fmt.Sprintf("%q", args[i]))
	}
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
	path := filepath.Join("/run/systemd/system", unit)
	t.Cleanup(func() {
		gap1CleanupUnit(t, unit)
		_ = os.Remove(path)
		_, _ = runSystemctl("daemon-reload")
		for i := len(created) - 1; i >= 0; i-- {
			_ = os.Remove(created[i])
		}
	})
	lines := append([]string{"[Unit]", "Description=sw-block test unit"}, unitSection...)
	lines = append(lines, "[Service]", "Type=exec", "ExecStart="+strings.Join(quoted, " "))
	lines = append(lines, serviceSection...)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := runSystemctl("daemon-reload"); err != nil {
		t.Fatalf("daemon-reload: %v %s", err, out)
	}
}

func runSystemctl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
	return string(out), err
}

func unitJournal(unit string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager").CombinedOutput()
	return string(out)
}
