//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
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
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer probeCancel()
	if out, err := exec.CommandContext(probeCtx, "systemctl", "show", "--property=Version").CombinedOutput(); err != nil {
		t.Fatalf("real systemd unavailable: %v %s", err, out)
	}
	fixture := newGAP1Fixture(t)
	root := fixture.root
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"blockvolume", "blockmaster"} {
		buildCtx, buildCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd := exec.CommandContext(buildCtx, "go", "build", "-p", "4", "-o", filepath.Join(bin, name), "../"+name)
		cmd.WaitDelay = time.Second // bound inherited child pipes after cancellation
		out, err := cmd.CombinedOutput()
		buildCancel()
		if err != nil {
			t.Fatalf("real binary build: %v %s", err, out)
		}
	}
	cluster := startWorkspaceWeedAt(t, filepath.Join(root, "weed"))
	gap1PreserveFailure(t, root, cluster.dir)
	stem := fmt.Sprintf("codex03-gap1-%d", time.Now().UnixNano())
	gap1ProbeTerminalSuccess(t, fixture, stem+"-policy-probe")
	volume := stem
	base := "/" + stem
	masterAddr := leaseBinaryAddress(t)
	dataAddr, ctrlAddr, statusAddr, nvmeAddr := leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t)
	cfg := fmt.Sprintf("nodes:\n  - server_id: s1\n    data_addr: %s\n    ctrl_addr: %s\nvolumes:\n  - id: %s\n    size_bytes: 262144\n    replication_factor: 1\n    placements:\n      - server_id: s1\n        replica_id: r1\n        source: existing_replica\n", dataAddr, ctrlAddr, volume)
	spec := filepath.Join(root, "cluster.yaml")
	if err := os.WriteFile(spec, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	fixture.start(t, stem+"-master", false, filepath.Join(bin, "blockmaster"),
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
	fixture.start(t, bootstrap, false, filepath.Join(bin, "blockvolume"), args...)
	waitGAP1(t, "bootstrap did not attach under a real lease", 60*time.Second, func() bool { return gap1Operational(t, statusAddr, volume) })
	// Calibrate verdict parsing against actual binary responses BEFORE kill.
	status, payload, err := gap1Status(statusAddr, volume)
	t.Logf("captured real status200: status%d body%s", status, payload)
	if err != nil || !gap1StatusOperational(status, payload, volume) {
		t.Fatalf("real status200 parser control: %v", err)
	}
	missing, body404, err := gap1Status(statusAddr, volume+"-not-served")
	t.Logf("captured real status404: status%d body%s", missing, body404)
	if err != nil || missing != http.StatusNotFound || gap1StatusOperational(missing, body404, volume) {
		t.Fatalf("real status404 parser control: %v", err)
	}
	want := bytes.Repeat([]byte{0x6d}, 4096)
	client := gap1ConnectNVMe(t, nvmeAddr, "nqn.2026-11.test:"+stem)
	client.writeFlush(t, 8, want)
	t.Logf("acknowledged NVMe record: lba=8 bytes=%d sha256=%x", len(want), sha256.Sum256(want))
	client.readExact(t, 8, want)
	client.close()
	remote := cluster.remote(t, base+"/"+volume)
	before, err := gap1ReadHead(t, remote)
	if err != nil || before.Gen == 0 {
		t.Fatalf("bootstrap HEAD: %+v %v", before, err)
	}
	gap1Systemctl(t, "kill", "--signal=KILL", "--kill-who=main", bootstrap)
	waitGAP1(t, "bootstrap still running", 10*time.Second, func() bool { return gap1Unit(t, bootstrap)["MainPID"] == "0" })
	// QueryWorkspace is read-only. These real binaries and this test share
	// one WSL kernel wall clock. Use the reported MASTER expiry timestamp,
	// not ValidForNs (which ends half a skew earlier and could renew live).
	// No product barrier formula or fake master clock is introduced.
	waitGAP1(t, "holder's reported validity never lapsed", 60*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s, e := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: volume})
		if e != nil || s.GetLease() == nil {
			return false
		}
		t.Logf("captured master lease: %s", s.String())
		return s.Lease.ValidForNs <= 0 && time.Now().UnixNano() >= s.Lease.ExpiresAtNs
	})
	unit := stem + "-returning"
	fixture.start(t, unit, true, filepath.Join(bin, "blockvolume"), args...)
	first := gap1Unit(t, unit)
	t.Logf("captured effective restart policy: %+v", first)
	firstPID := first["MainPID"]
	if firstPID == "0" {
		t.Fatal("returning unit did not start a real process")
	}
	// Existing assignment wait is30s. This is an outer failed-run guard, not
	// an injected success delay or a replacement product deadline.
	waitGAP1(t, "GAP-1: first startup never exited75; stayed alive non-serving", 45*time.Second, func() bool {
		s := gap1Unit(t, unit)
		if gap1Operational(t, statusAddr, volume) {
			t.Fatal("fixture returned before the expired-grant barrier: cannot count this as a startup-timeout red")
		}
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
	after, err := gap1ReadHead(t, remote)
	if err != nil || after.Gen <= before.Gen || after.Holder != before.Holder || after.Epoch != before.Epoch {
		t.Fatalf("return did not claim HEAD: before%+v after%+v err%v", before, after, err)
	}
	returned := gap1ConnectNVMe(t, nvmeAddr, "nqn.2026-11.test:"+stem)
	returned.readExact(t, 8, want)
	returned.writeFlush(t, 9, want)
	returned.readExact(t, 9, want)
	returned.close()
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
	gap1PlannedStopDoesNotRestart(t, ws, unit, statusAddr, volume, after.Epoch, "r1", s["NRestarts"], s["InvocationID"])
	gap1NeverAssignedRetries(t, fixture, stem, filepath.Join(bin, "blockvolume"), args, root, volume)
}

// Roots are deliberately independent of testing.TempDir: unknown unit cleanup
// must never remove an executable or WAL beneath a possible surviving service.
type gap1Fixture struct {
	root  string
	units []string
}

func newGAP1Fixture(t *testing.T) *gap1Fixture {
	t.Helper()
	root, err := os.MkdirTemp("", "codex03-gap1-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	f := &gap1Fixture{root: root}
	t.Cleanup(func() {
		safe := true
		for _, unit := range f.units {
			state, err := gap1ReadUnit(unit)
			if err != nil || !gap1UnitGone(state) {
				t.Errorf("final unit absence unproved: %s state%v err%v", unit, state, err)
				safe = false
			}
		}
		if !safe || t.Failed() {
			t.Logf("retained run root: %s (safe=%v failed=%v)", root, safe, t.Failed())
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove confirmed idle run root: %v", err)
		}
	})
	return f
}
func (f *gap1Fixture) start(t *testing.T, unit string, restart bool, exe string, args ...string) {
	t.Helper()
	f.units = append(f.units, unit)
	// Persist ownership before the create, including an aggregate test crash.
	ownership, err := os.OpenFile(filepath.Join(f.root, "owned-units.txt"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fmt.Fprintln(ownership, unit)
	closeErr := ownership.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("record unit ownership: %v %v", writeErr, closeErr)
	}
	// Register BEFORE submission: systemd may create the unit even when the
	// command loses its answer or times out. Every uncertain create is owned.
	t.Cleanup(func() { gap1CleanupUnit(t, unit) })
	policy := "no"
	if restart {
		policy = "on-failure"
	}
	shipped := gap1ShippedPolicy(t)
	cmdArgs := []string{"--unit=" + unit, "--property=Type=exec", "--property=Restart=" + policy, "--property=RestartSec=" + shipped["RestartSec"], "--property=RestartPreventExitStatus=" + shipped["RestartPreventExitStatus"], "--property=TimeoutStopSec=" + shipped["TimeoutStopSec"], "--property=StartLimitIntervalSec=10s", "--property=StartLimitBurst=5", "--", exe}
	cmdArgs = append(cmdArgs, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "systemd-run", cmdArgs...).CombinedOutput(); err != nil {
		t.Fatalf("systemd-run %s (creation outcome uncertain; registered cleanup owns it): %v %s", unit, err, out)
	}
}
func gap1CleanupUnit(t *testing.T, unit string) {
	t.Helper()
	command := func(bound time.Duration, args ...string) error {
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
		if err != nil {
			t.Logf("unit cleanup %s: %v %s", unit, err, out)
		}
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	journal, _ := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager", "--output=short-monotonic").CombinedOutput()
	cancel()
	t.Logf("real unit %s journal:\n%s", unit, journal)
	_ = command(15*time.Second, "stop", unit)
	state, err := gap1ReadUnit(unit)
	if err != nil || !gap1UnitGone(state) {
		_ = command(5*time.Second, "kill", "--kill-who=all", "--signal=KILL", unit)
		_ = command(5*time.Second, "stop", unit)
	}
	_ = command(5*time.Second, "reset-failed", unit)
	state, err = gap1ReadUnit(unit)
	if err != nil || !gap1UnitGone(state) {
		t.Errorf("owned unit/PID cleanup not confirmed; retain run root: %s state%v err%v", unit, state, err)
		return
	}
	t.Logf("confirmed owned unit stopped: %s state%v", unit, state)
}
func gap1UnitGone(s map[string]string) bool {
	job, hasJob := s["Job"]
	return hasJob && job == "" && s["MainPID"] == "0" && s["ControlPID"] == "0" &&
		(s["LoadState"] == "not-found" || s["LoadState"] == "loaded") && s["ActiveState"] == "inactive" && s["SubState"] == "dead"
}

// The absent-unit shape is captured from real systemd249. Refuse incomplete,
// contradictory, queued-job and live-process observations before removing WAL.
func TestGAP1AbsenceRequiresNoJobAndBothPIDsZero(t *testing.T) {
	for _, tc := range []struct {
		name, key, value string
		want             bool
	}{
		{"collected", "LoadState", "not-found", true},
		{"loaded stopped", "LoadState", "loaded", true},
		{"main lives", "MainPID", "123", false},
		{"control lives", "ControlPID", "123", false},
		{"job queued", "Job", "123", false},
		{"restart pending", "SubState", "auto-restart", false},
		{"contradictory active", "ActiveState", "active", false},
		{"unknown load", "LoadState", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := map[string]string{"MainPID": "0", "ControlPID": "0", "LoadState": "not-found", "ActiveState": "inactive", "SubState": "dead", "Job": ""}
			s[tc.key] = tc.value
			if gap1UnitGone(s) != tc.want {
				t.Fatalf("absence classification %+v want%v", s, tc.want)
			}
			delete(s, "Job")
			if gap1UnitGone(s) {
				t.Fatal("missing observation treated as absence")
			}
		})
	}
}
func gap1ReadHead(t *testing.T, remote storage.ChunkRemote) (storage.ChunkHead, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return remote.GetHead(ctx)
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
	m, err := gap1ReadUnit(unit)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("captured unit %s: %+v", unit, m)
	return m
}
func gap1ReadUnit(unit string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := exec.CommandContext(ctx, "systemctl", "show", unit, "--property=Job,MainPID,ControlPID,NRestarts,ExecMainStatus,Result,ExecMainStartTimestampMonotonic,RestartUSec,RestartPreventExitStatus,StartLimitBurst,StartLimitIntervalUSec,ActiveState,SubState,LoadState,InvocationID").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("unit show %s: %w %s", unit, err, b)
	}
	m := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		k, v, ok := strings.Cut(l, "=")
		if ok {
			m[k] = v
		}
	}
	for _, key := range []string{"Job", "MainPID", "ControlPID", "NRestarts", "ExecMainStatus", "Result", "ExecMainStartTimestampMonotonic", "LoadState", "ActiveState", "SubState"} {
		if _, ok := m[key]; !ok {
			return nil, fmt.Errorf("captured systemd response lacks %s: %s", key, b)
		}
	}
	return m, nil
}
func gap1Status(addr, volume string) (int, []byte, error) {
	r, e := (&http.Client{Timeout: time.Second}).Get("http://" + addr + "/status/durable?volume=" + volume)
	if e != nil {
		return 0, nil, e
	}
	defer r.Body.Close()
	body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return r.StatusCode, body, e
}
func gap1StatusOperational(status int, payload []byte, volume string) bool {
	if status != http.StatusOK {
		return false
	}
	var body struct{ Volumes []durable.VolumeStatus }
	if json.Unmarshal(payload, &body) != nil {
		return false
	}
	for _, v := range body.Volumes {
		if v.VolumeID == volume {
			return v.Operational && v.Chunk != nil && v.Chunk.Attached && !v.Chunk.Fenced
		}
	}
	return false
}
func gap1Operational(t *testing.T, addr, volume string) bool {
	t.Helper()
	status, payload, e := gap1Status(addr, volume)
	return e == nil && gap1StatusOperational(status, payload, volume)
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

// The transient unit exercises the shipped restart policy, not an independently
// tuned retry loop. Both deployed frontend variants must carry the same policy.
func gap1ShippedPolicy(t *testing.T) map[string]string {
	t.Helper()
	var first map[string]string
	for _, name := range []string{"sw-blockvolume.service", "sw-blockvolume-ublk.service"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "systemd", name))
		if err != nil {
			t.Fatal(err)
		}
		fields := map[string]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
			if ok {
				fields[key] = value
			}
		}
		if fields["Restart"] != "on-failure" || fields["RestartPreventExitStatus"] != "78" || fields["RestartSec"] != "5s" {
			t.Fatalf("shipped policy changed: %s %+v", name, fields)
		}
		if first != nil {
			for _, key := range []string{"Restart", "RestartSec", "RestartPreventExitStatus", "TimeoutStopSec"} {
				if fields[key] != first[key] {
					t.Fatalf("unit policies differ at %s", key)
				}
			}
		}
		first = fields
	}
	return first
}

// A successful planned revoke is not a retry outcome. The master's ACK and
// systemd's terminal inactive/dead state prove no automatic restart is queued.
func gap1PlannedStopDoesNotRestart(t *testing.T, ws control.WorkspaceServiceClient, unit, addr, volume string, epoch uint64, holder, restarts, invocation string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/chunk/detach", bytes.NewBufferString(fmt.Sprintf(`{"volumeID":%q,"plain":true,"timeoutMs":5000}`, volume)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	t.Logf("captured plain seal response: status%d body%s", resp.StatusCode, body)
	if readErr != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("plain seal: %v status%d", readErr, resp.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = ws.RevokeWorkspace(ctx, &control.WorkspaceIdentity{VolumeId: volume, ReplicaId: holder, Epoch: epoch})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	waitGAP1(t, "planned revoke never recorded ACK and exited normally", 20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		status, err := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: volume})
		if err != nil || status.GetRevocationAck() == nil {
			return false
		}
		ack := status.GetRevocationAck()
		t.Logf("captured planned-stop ACK: %s", ack.String())
		if ack.GetIdentity().GetVolumeId() != volume || ack.GetIdentity().GetReplicaId() != holder || ack.GetIdentity().GetEpoch() != epoch || ack.GetHeadGen() == 0 {
			t.Fatalf("wrong planned ACK: %s", ack.String())
		}
		state := gap1Unit(t, unit)
		// systemd may unload a successful transient unit, resetting its
		// counters. In that case use the exact invocation's journal proof.
		if state["LoadState"] != "not-found" && state["NRestarts"] != restarts {
			t.Fatalf("planned stop restarted: %+v", state)
		}
		if state["MainPID"] != "0" {
			return false
		}
		if state["ExecMainStatus"] != "0" || state["ActiveState"] != "inactive" || state["SubState"] != "dead" {
			t.Fatalf("planned stop is not terminal success: %+v", state)
		}
		starts, parseErr := strconv.Atoi(restarts)
		if parseErr != nil || invocation == "" {
			t.Fatalf("missing pre-stop systemd identity: starts=%q invocation=%q", restarts, invocation)
		}
		return gap1JournalTerminalSuccess(t, unit, invocation, starts+1)
	})
}

// The master keeps the real r1 authority line. A fresh r-old process has
// zero observed-own lineage, so a foreign fact selects retry75, never78.
// Run after the live holder has ACKed and exited, avoiding a node registration
// change while that holder is serving. This is not a superseded-holder witness.
func gap1NeverAssignedRetries(t *testing.T, fixture *gap1Fixture, stem, exe string, args []string, root, volume string) {
	t.Helper()
	changed := append([]string(nil), args...)
	addr := leaseBinaryAddress(t)
	replacements := map[string]string{"--replica-id": "r-old", "--data-addr": leaseBinaryAddress(t), "--ctrl-addr": leaseBinaryAddress(t), "--status-addr": addr, "--durable-root": filepath.Join(root, "superseded-store"), "--nvme-listen": leaseBinaryAddress(t), "--nvme-subsysnqn": "nqn.2026-11.test:" + stem + "-superseded"}
	for i := 0; i+1 < len(changed); i++ {
		if value, ok := replacements[changed[i]]; ok {
			changed[i+1] = value
		}
	}
	unit := stem + "-superseded"
	fixture.start(t, unit, true, exe, changed...)
	waitGAP1(t, "superseded process never observed the master's other-replica fact", 15*time.Second, func() bool {
		resp, err := (&http.Client{Timeout: time.Second}).Get("http://" + addr + "/status?volume=" + volume)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		t.Logf("captured real supersede status: status%d body%s", resp.StatusCode, raw)
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var status hostvolume.StatusProjection
		if err := json.Unmarshal(raw, &status); err != nil {
			t.Fatal(err)
		}
		if status.Healthy || status.FrontendPrimaryReady {
			t.Fatalf("unassigned old identity served: %+v", status)
		}
		// A never-assigned replica remains ModeIdle, so /status intentionally
		// reports unknown even after recording the other-replica fact. Prove
		// receipt in this process before checking its retry75 decision.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		journal, err := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager", "--output=cat").CombinedOutput()
		if err != nil {
			t.Fatalf("supersede journal: %v %s", err, journal)
		}
		observed := fmt.Sprintf("volume %s authority is now r1@1 (not this replica r-old); recording supersede", volume)
		if strings.Contains(string(journal), observed) {
			t.Logf("captured authoritative other-replica observation: %s", observed)
			return true
		}
		return false
	})
	waitGAP1(t, "never-assigned foreign identity did not exit75", 45*time.Second, func() bool {
		s := gap1Unit(t, unit)
		if s["ExecMainStatus"] == "78" {
			t.Fatalf("zero-lineage process chose no-retry: %+v", s)
		}
		return s["ExecMainStatus"] == "75"
	})
	waitGAP1(t, "never-assigned retry75 did not restart", 15*time.Second, func() bool {
		s := gap1Unit(t, unit)
		n, err := strconv.Atoi(s["NRestarts"])
		if err != nil || n > 2 {
			t.Fatalf("invalid or unbounded restart count: %+v", s)
		}
		return n >= 1 && s["MainPID"] != "0"
	})
}

// Register before the units: their exact-unit cleanup runs first, then this
// retains failed-run local state before testing.TempDir removes it. Nothing is
// deleted from the evidence directory, including a partial copy on failure.
func gap1PreserveFailure(t *testing.T, roots ...string) {
	t.Helper()
	destination := os.Getenv("GAP1_EVIDENCE_DIR")
	if destination == "" {
		return
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		if err := os.MkdirAll(destination, 0700); err != nil {
			t.Logf("evidence mkdir: %v", err)
			return
		}
		for i, root := range roots {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			out, err := exec.CommandContext(ctx, "cp", "-a", "--sparse=always", root, filepath.Join(destination, fmt.Sprintf("runtime-%d", i))).CombinedOutput()
			cancel()
			t.Logf("failed-run state retained: source=%s destination=%s err=%v output=%s", root, destination, err, out)
		}
	})
}

// Captured systemd249 journal records supply stable manager message IDs. The
// raw journal is logged when this witness succeeds; no fabricated PID status
// can turn an unknown unit into an ACKed successful invocation.
func gap1JournalTerminalSuccess(t *testing.T, unit, invocation string, expectedStarts int) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager", "--output=json").CombinedOutput()
	if err != nil {
		t.Fatalf("unit journal: %v %s", err, raw)
	}
	starts := 0
	started := map[string]bool{}
	succeeded := map[string]bool{}
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var entry struct {
			Unit       string `json:"UNIT"`
			Invocation string `json:"INVOCATION_ID"`
			Message    string `json:"MESSAGE_ID"`
			PID        string `json:"_PID"`
			JobResult  string `json:"JOB_RESULT"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("captured journal JSON: %v", err)
		}
		if entry.Unit != unit+".service" || entry.PID != "1" {
			continue
		}
		switch entry.Message {
		case "39f53479d3a045ac8e11786248231fbf":
			if entry.JobResult == "done" {
				starts++
				started[entry.Invocation] = true
			}
		case "7ad2d189f7e94e70a38c781354912448":
			succeeded[entry.Invocation] = true
		}
	}
	if starts > expectedStarts {
		t.Fatalf("unexpected restart in captured unit journal: starts%d want%d", starts, expectedStarts)
	}
	if invocation == "" && expectedStarts == 1 {
		for id := range started {
			invocation = id
		}
	}
	complete := invocation != "" && starts == expectedStarts && started[invocation] && succeeded[invocation]
	if complete {
		t.Logf("captured terminal-success journal for %s invocation%s: %s", unit, invocation, raw)
	}
	return complete
}

// Calibrate the terminal parser against the real supervisor before any holder
// is killed. Successful transient units can be collected; their journal is the
// proof, never the default zeros returned for an unknown unit.
func gap1ProbeTerminalSuccess(t *testing.T, fixture *gap1Fixture, unit string) {
	t.Helper()
	fixture.start(t, unit, true, "/bin/true")
	waitGAP1(t, "systemd terminal-success parser preflight failed", 5*time.Second, func() bool {
		return gap1JournalTerminalSuccess(t, unit, "", 1)
	})
}
