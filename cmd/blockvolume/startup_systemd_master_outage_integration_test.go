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

// A 10-minute blockmaster outage under real systemd (root), with the shipped
// unit's restart policy and start limit: the serving process fences once and
// exits 75; the unit starts it again and that process waits inside itself
// (no restart loop): the unit never reaches its start limit (NRestarts 1, 2
// starts), /status shows MasterWait unreachable with its age, and when the
// master returns the volume serves by itself within 2 s of the master's
// barrier time.
//
// A runtime unit file (/run/systemd/system) stands for the installed unit.
func TestUnitSurvivesTenMinuteMasterOutage(t *testing.T) {
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
	for deadline := killAt.Add(10 * time.Minute); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		s, err := gap1ReadUnit(unit)
		if err != nil {
			continue
		}
		state = s
		if s["ActiveState"] == "failed" {
			t.Fatalf("the unit gave up %v into the outage (NRestarts %s, Result %s)", time.Since(killAt).Round(time.Second), s["NRestarts"], s["Result"])
		}
	}
	mw, ok := masterWaitOf(b.status)
	t.Logf("PHASE0-SYSTEMD master down %v: ActiveState %s NRestarts %s; MasterWait %+v",
		time.Since(killAt).Round(time.Second), state["ActiveState"], state["NRestarts"], mw)
	if state["ActiveState"] != "active" || state["NRestarts"] != "1" {
		t.Fatalf("after 10 min: ActiveState %s NRestarts %s; want active after exactly one restart (the fence)", state["ActiveState"], state["NRestarts"])
	}
	if !ok || mw.Reason != "unreachable" || time.Since(mw.Since) < 9*time.Minute {
		t.Fatalf("/status MasterWait %+v (present %v); want unreachable for most of the outage", mw, ok)
	}

	env.masterProcess = env.h.start(t, "blockmaster-back", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	var until time.Time
	poll(t, 30*time.Second, "/status never showed the master's barrier", func() bool {
		mw, ok := masterWaitOf(b.status)
		if ok && mw.Reason == "barrier" && !mw.Until.IsZero() {
			until = mw.Until
			return true
		}
		return false
	})
	for !volumeFrontendReady(b.status, "v1") {
		if time.Now().After(until.Add(60 * time.Second)) {
			t.Fatal("v1 never served by itself after the master returned")
		}
		time.Sleep(100 * time.Millisecond)
	}
	late := time.Since(until)
	s, _ := gap1ReadUnit(unit)
	t.Logf("PHASE0-SYSTEMD master back: served %v after the barrier time; NRestarts %s", late.Round(time.Millisecond), s["NRestarts"])
	if late > 2*time.Second || s["NRestarts"] != "1" {
		t.Fatalf("served %v after the barrier (want <= 2 s), NRestarts %s (want 1)", late.Round(time.Millisecond), s["NRestarts"])
	}
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
