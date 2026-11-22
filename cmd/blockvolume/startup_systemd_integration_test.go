//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Real systemd owns every restart. No test goroutine relaunches the holder.
// The first assertion is RED on GAP-1: the expired startup stays alive.
func TestSystemdExpiredStartupExits75ThenRegrantServes(t *testing.T) {
	if os.Getenv("GAP1_SYSTEMD_AUTHORIZED") != "1" {
		t.Fatal("explicit booked WSL systemd qualification required; no skip counts as proof")
	}
	if os.Geteuid() != 0 {
		t.Fatal("systemd qualification needs the booked root harness")
	}
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Fatal("real CHUNKSTORE_WEED binary required")
	}
	if out, err := exec.Command("systemctl", "show", "--property=Version").CombinedOutput(); err != nil {
		t.Fatalf("real systemd unavailable: %v %s", err, out)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"blockvolume", "blockmaster"} {
		cmd := exec.Command("go", "build", "-p", "4", "-o", filepath.Join(bin, name), "../"+name)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("real binary build: %v %s", err, out)
		}
	}
	cluster := startWorkspaceWeed(t)
	stem := fmt.Sprintf("codex03-gap1-%d", time.Now().UnixNano())
	volume := stem
	base := "/" + stem
	masterAddr := leaseBinaryAddress(t)
	dataAddr, ctrlAddr, statusAddr, nvmeAddr := leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t)
	cfg := fmt.Sprintf("nodes:\n  - server_id: s1\n    data_addr: %s\n    ctrl_addr: %s\nvolumes:\n  - id: %s\n    size_bytes: 262144\n    replication_factor: 1\n    placements:\n      - server_id: s1\n        replica_id: r1\n        source: existing_replica\n", dataAddr, ctrlAddr, volume)
	spec := filepath.Join(root, "cluster.yaml")
	if err := os.WriteFile(spec, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	startGAP1Unit(t, stem+"-master", false, filepath.Join(bin, "blockmaster"),
		"--listen", masterAddr, "--authority-store", filepath.Join(root, "auth"), "--lifecycle-store", filepath.Join(root, "life"),
		"--cluster-spec", spec, "--lifecycle-product-loop-interval", "200ms", "--expected-slots-per-volume", "1",
		"--workspace-rpc", "--workspace-head-base-url", "http://"+cluster.filerHTTP+base)
	w4WaitTCP(t, masterAddr, 20*time.Second)
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ws := control.NewWorkspaceServiceClient(conn)
	args := []string{"--master", masterAddr, "--server-id", "s1", "--volume-id", volume, "--replica-id", "r1",
		"--data-addr", dataAddr, "--ctrl-addr", ctrlAddr, "--heartbeat-interval", "200ms", "--t1-readiness", "--status-addr", statusAddr,
		"--durable-root", filepath.Join(root, "store"), "--durable-impl", "walstore-chunk", "--durable-blocks", "64",
		"--chunk-filer", cluster.filerGRPC, "--chunk-path", base, "--chunk-control-endpoint", "--workspace-lease",
		"--nvme-listen", nvmeAddr, "--nvme-subsysnqn", "nqn.2026-11.test:" + stem}
	// Bootstrap is a different, non-restarting unit. Kill only this exact unit.
	bootstrap := stem + "-bootstrap"
	startGAP1Unit(t, bootstrap, false, filepath.Join(bin, "blockvolume"), args...)
	waitGAP1(t, "bootstrap did not attach under a real lease", 60*time.Second, func() bool { return gap1Operational(t, statusAddr, volume) })
	remote := cluster.remote(t, base+"/"+volume)
	before, err := remote.GetHead(context.Background())
	if err != nil || before.Gen == 0 {
		t.Fatalf("bootstrap HEAD: %+v %v", before, err)
	}
	gap1Systemctl(t, "kill", "--signal=KILL", "--kill-whom=main", bootstrap)
	waitGAP1(t, "bootstrap still running", 10*time.Second, func() bool { return gap1Unit(t, bootstrap)["MainPID"] == "0" })
	// QueryWorkspace is read-only. Do not renew or advance a fake master clock.
	waitGAP1(t, "holder's reported validity never lapsed", 60*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, e := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: volume})
		if e != nil || s.GetLease() == nil {
			return false
		}
		t.Logf("captured master lease: %s", s.String())
		return s.Lease.ValidForNs <= 0
	})
	unit := stem + "-returning"
	startGAP1Unit(t, unit, true, filepath.Join(bin, "blockvolume"), args...)
	firstPID := gap1Unit(t, unit)["MainPID"]
	if firstPID == "0" {
		t.Fatal("returning unit did not start a real process")
	}
	// Existing assignment wait is30s. This is an outer failed-run guard, not
	// an injected success delay or a replacement product deadline.
	waitGAP1(t, "GAP-1: first startup never exited75; stayed alive non-serving", 45*time.Second, func() bool {
		s := gap1Unit(t, unit)
		if s["ExecMainStatus"] == "75" {
			return true
		}
		if s["MainPID"] != "0" && s["MainPID"] != firstPID {
			t.Fatalf("restart without captured75: %+v", s)
		}
		return false
	})
	waitGAP1(t, "systemd did not restart/regrant/attach", 100*time.Second, func() bool {
		s := gap1Unit(t, unit)
		n, e := strconv.Atoi(s["NRestarts"])
		if e != nil {
			t.Fatalf("invalid captured NRestarts: %+v", s)
		}
		if n > 2 {
			t.Fatalf("barrier retry exceeded three total launches: %+v", s)
		}
		return n >= 1 && s["MainPID"] != "0" && s["MainPID"] != firstPID && gap1Operational(t, statusAddr, volume)
	})
	after, err := remote.GetHead(context.Background())
	if err != nil || after.Gen <= before.Gen || after.Holder != before.Holder || after.Epoch != before.Epoch {
		t.Fatalf("return did not claim HEAD: before%+v after%+v err%v", before, after, err)
	}
	// A real control cut must pass the lease and layer gates after restart.
	req, err := http.NewRequest(http.MethodPost, "http://"+statusAddr+"/chunk/cut", bytes.NewBufferString(fmt.Sprintf(`{"volumeID":%q,"timeoutMs":3000}`, volume)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-regrant real cut status%d", resp.StatusCode)
	}
	s := gap1Unit(t, unit)
	if s["Result"] == "start-limit-hit" {
		t.Fatalf("systemd rate limit hit: %+v", s)
	}
}

func startGAP1Unit(t *testing.T, unit string, restart bool, exe string, args ...string) {
	t.Helper()
	policy := "no"
	if restart {
		policy = "on-failure"
	}
	cmdArgs := []string{"--unit=" + unit, "--property=Type=exec", "--property=Restart=" + policy, "--property=RestartSec=5s", "--property=RestartPreventExitStatus=78", "--property=TimeoutStopSec=10s", "--property=StartLimitIntervalSec=10s", "--property=StartLimitBurst=5", "--", exe}
	cmdArgs = append(cmdArgs, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "systemd-run", cmdArgs...).CombinedOutput()
	if err != nil {
		t.Fatalf("systemd-run %s: %v %s", unit, err, out)
	}
	t.Cleanup(func() {
		// Capture before stopping; never remove a run root owned by another run.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		log, _ := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager", "--output=short-monotonic").CombinedOutput()
		cancel()
		t.Logf("real unit %s journal:\n%s", unit, log)
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		_, err := exec.CommandContext(ctx, "systemctl", "stop", unit).CombinedOutput()
		cancel()
		if err != nil {
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			_ = exec.CommandContext(ctx, "systemctl", "kill", "--signal=KILL", unit).Run()
			cancel()
			t.Errorf("unit stop required kill: %s", unit)
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		_ = exec.CommandContext(ctx, "systemctl", "reset-failed", unit).Run()
		cancel()
	})
}
func gap1Systemctl(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput(); err != nil {
		t.Fatalf("systemctl: %v %s", err, out)
	}
}
func gap1Unit(t *testing.T, unit string) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := exec.CommandContext(ctx, "systemctl", "show", unit, "--property=MainPID,NRestarts,ExecMainStatus,Result,ExecMainStartTimestampMonotonic").CombinedOutput()
	if err != nil {
		t.Fatalf("unit show: %v %s", err, b)
	}
	m := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		k, v, ok := strings.Cut(l, "=")
		if ok {
			m[k] = v
		}
	}
	for _, key := range []string{"MainPID", "NRestarts", "ExecMainStatus", "Result", "ExecMainStartTimestampMonotonic"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("captured systemd response lacks %s: %s", key, b)
		}
	}
	t.Logf("captured unit %s: %s", unit, strings.TrimSpace(string(b)))
	return m
}
func gap1Operational(t *testing.T, addr, volume string) bool {
	t.Helper()
	r, e := (&http.Client{Timeout: time.Second}).Get("http://" + addr + "/status/durable?volume=" + volume)
	if e != nil {
		return false
	}
	defer r.Body.Close()
	var body struct {
		Volumes []durable.VolumeStatus `json:"volumes"`
	}
	if r.StatusCode != http.StatusOK {
		return false
	}
	if e = json.NewDecoder(r.Body).Decode(&body); e != nil {
		return false
	}
	for _, v := range body.Volumes {
		if v.VolumeID == volume {
			return v.Operational && v.Chunk != nil && v.Chunk.Attached && !v.Chunk.Fenced
		}
	}
	return false
}
func waitGAP1(t *testing.T, what string, bound time.Duration, ready func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(what)
		case <-tick.C:
		}
	}
}
