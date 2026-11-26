//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/lifecycleclient"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
)

// Real systemd owns every restart. No test goroutine relaunches the holder.
// The first assertion is RED on GAP-1: the expired startup stays alive.
func TestSystemdExpiredStartupExits75ThenRegrantServes(t *testing.T) {
	if os.Getenv("GAP1_SYSTEMD_AUTHORIZED") != "1" {
		t.Fatal("explicit real-systemd qualification authorization required; no skip counts as proof")
	}
	if os.Geteuid() != 0 {
		t.Fatal("systemd qualification needs root")
	}
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Fatal("real CHUNKSTORE_WEED binary required")
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer probeCancel()
	if out, err := exec.CommandContext(probeCtx, "systemctl", "show", "--property=Version").CombinedOutput(); err != nil {
		t.Fatalf("real systemd unavailable: %v %s", err, out)
	}
	// Under systemd the test lock override is refused: the units use the
	// real host lock directory, which the shipped tmpfiles.d entry makes.
	if err := os.MkdirAll(durable.HostVolumeLockDir, 0o700); err != nil {
		t.Fatalf("host volume lock directory: %v", err)
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
	stem := fmt.Sprintf("gap1-%d", time.Now().UnixNano())
	gap1ProbeTerminalSuccess(t, fixture, stem+"-policy-probe")
	volume := stem
	base := "/" + stem
	masterAddr := leaseBinaryAddress(t)
	target, masterListen := gap1TargetAddresses(t, masterAddr)
	dataAddr, ctrlAddr, statusAddr, nvmeAddr := leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t)
	cfg := fmt.Sprintf("nodes:\n  - server_id: s1\n    data_addr: %s\n    ctrl_addr: %s\n  - server_id: s2\n    data_addr: %s\n    ctrl_addr: %s\nvolumes:\n  - id: %s\n    size_bytes: 262144\n    replication_factor: 1\n    placements:\n      - server_id: s1\n        replica_id: r1\n        source: existing_replica\n", dataAddr, ctrlAddr, target.dataAddr, target.ctrlAddr, volume)
	spec := filepath.Join(root, "cluster.yaml")
	if err := os.WriteFile(spec, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	// The master listens off loopback, so it runs with mutual TLS: the test
	// is an admin, s1 and s2 are nodes (certificates in root/tls).
	tlsDir := gap1WriteTLS(t, root, target)
	fixture.start(t, stem+"-master", false, filepath.Join(bin, "blockmaster"),
		"--listen", masterListen, "--tls-cert", filepath.Join(tlsDir, "master.crt"), "--tls-key", filepath.Join(tlsDir, "master.key"),
		"--tls-client-ca", filepath.Join(tlsDir, "ca.crt"), "--role-file", filepath.Join(tlsDir, "roles"), "--audit-log", filepath.Join(tlsDir, "audit.log"), "--authority-store", filepath.Join(root, "auth"), "--lifecycle-store", filepath.Join(root, "life"),
		"--cluster-spec", spec, "--lifecycle-product-loop-interval", "200ms", "--expected-slots-per-volume", "1",
		"--workspace-rpc",
		// A workspace master writes the layout VOLUME and unclaimed HEAD itself.
		"--chunk-filer", cluster.filerGRPC, "--chunk-path", base)
	w4WaitTCP(t, masterAddr, 20*time.Second)
	admin, err := link.ClientCredentials(link.TLSFiles{Cert: filepath.Join(tlsDir, "admin.crt"), Key: filepath.Join(tlsDir, "admin.key"), CA: filepath.Join(tlsDir, "ca.crt")}, "")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(masterAddr, admin)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ws := control.NewWorkspaceServiceClient(conn)
	// The master mints the volume's layout identity; the holder and every
	// HEAD read name it.
	var layoutID layout.VolumeID
	waitGAP1(t, "master never resolved the volume's layout identity", 20*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		id, e := lifecycleclient.ResolveVolume(ctx, control.NewLifecycleServiceClient(conn), volume)
		layoutID = id
		return e == nil
	})
	args := []string{"--master", masterAddr, "--server-id", "s1",
		"--master-tls-cert", filepath.Join(tlsDir, "s1.crt"), "--master-tls-key", filepath.Join(tlsDir, "s1.key"), "--master-ca", filepath.Join(tlsDir, "ca.crt"),
		"--volume-id", volume, "--replica-id", "r1", "--layout-id", layoutID.String(),
		"--data-addr", dataAddr, "--ctrl-addr", ctrlAddr, "--heartbeat-interval", "200ms", "--t1-readiness", "--status-addr", statusAddr,
		"--durable-root", filepath.Join(root, "store"), "--durable-impl", "walstore-chunk", "--durable-blocks", "64",
		"--chunk-filer", cluster.filerGRPC, "--chunk-path", base, "--chunk-control-endpoint", "--workspace-lease",
		"--nvme-listen", nvmeAddr, "--nvme-subsysnqn", "nqn.2026-11.test:" + stem}
	// Bootstrap is a different, non-restarting unit. Kill only this exact unit.
	bootstrap := stem + "-bootstrap"
	fixture.start(t, bootstrap, false, filepath.Join(bin, "blockvolume"), args...)
	waitGAP1(t, "bootstrap did not attach under a real lease", 60*time.Second, func() bool { return gap1Operational(t, statusAddr, volume) })
	// Calibrate the readiness parser on the serving source's actual /status
	// response before using it for the target's negative readiness assertion.
	waitGAP1(t, "source readiness control never became true", 20*time.Second, func() bool {
		status, body, err := gap1StatusAt(statusAddr, volume, "/status")
		if err != nil || status != http.StatusOK {
			return false
		}
		serving, err := gap1Readiness(status, body, volume)
		if err != nil {
			t.Fatal(err)
		}
		if serving {
			t.Logf("captured real readiness control: status%d body%s", status, body)
		}
		return serving
	})
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
	remote := cluster.remote(t, base, chunkstore.VolumeRef{Name: volume, ID: layoutID})
	before, err := gap1ReadHead(t, remote)
	if err != nil || before.Gen == 0 {
		t.Fatalf("bootstrap HEAD: %+v %v", before, err)
	}
	gap1Systemctl(t, "kill", "--signal=KILL", "--kill-who=main", bootstrap)
	waitGAP1(t, "bootstrap still running", 10*time.Second, func() bool { return gap1Unit(t, bootstrap)["MainPID"] == "0" })
	// HEAD as the killed holder left it (no process can move it now).
	ref := chunkstore.VolumeRef{Name: volume, ID: layoutID}
	atKill := layoutHead(t, cluster, base, ref)
	if atKill.Holder != "s1/r1" || atKill.ClaimID == "" || atKill.State != layout.StateLive {
		t.Fatalf("HEAD at the kill: %+v; want live, held by s1/r1 under a claim", atKill)
	}
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
	// The returning process waits in process for the master's barrier and
	// serves after it, with no fence exit and no restart (the startup wait,
	// #535): first it reports the barrier as its MasterWait, never serving.
	var barrierMs int64
	waitGAP1(t, "returning unit never reported the master's barrier", 30*time.Second, func() bool {
		if gap1Operational(t, statusAddr, volume) {
			t.Fatal("the returning unit served before reporting a barrier wait: an expired lease was re-granted without the barrier")
		}
		if s := gap1Unit(t, unit); s["MainPID"] != firstPID {
			t.Fatalf("the returning unit exited while waiting for the barrier: %+v", s)
		}
		if w := gap1MasterWait(t, statusAddr, volume); w.Reason == "barrier" && w.UntilMs > 0 {
			barrierMs = w.UntilMs
			t.Logf("captured master wait: reason=%s until=%d", w.Reason, w.UntilMs)
			return true
		}
		return false
	})
	// Then the same process serves: not before the barrier, and within the
	// startup wait's bound (barrier + 30 s; the outer guard adds slack).
	waitGAP1(t, "the returning unit never served after the barrier", 75*time.Second, func() bool {
		s := gap1Unit(t, unit)
		if s["MainPID"] != firstPID || s["NRestarts"] != "0" {
			t.Fatalf("the returning unit restarted (a fence exit) instead of serving in process: %+v", s)
		}
		if !gap1Operational(t, statusAddr, volume) {
			return false
		}
		if now := time.Now().UnixMilli(); now < barrierMs {
			t.Fatalf("served at %d, before the master's barrier %d", now, barrierMs)
		}
		if late := time.Now().UnixMilli() - barrierMs; late > 30_000 {
			t.Fatalf("served %d ms after the barrier: past the startup wait's 30 s bound", late)
		}
		return true
	})
	// The holder returns with its own claim at the same epoch: over an idle
	// HEAD it resumes with no HEAD write; a session of its claim left open at
	// the kill is aborted (a HEAD write) before it serves.
	after := layoutHead(t, cluster, base, ref)
	if after.Holder != atKill.Holder || after.Epoch != atKill.Epoch || after.ClaimID != atKill.ClaimID || after.State != layout.StateLive || after.Activity.Kind != layout.ActIdle {
		t.Fatalf("return did not resume its claim: at kill %+v, after %+v", atKill, after)
	}
	switch {
	case atKill.Activity.Kind == layout.ActIdle && after.Gen != atKill.Gen:
		t.Fatalf("return over an idle HEAD wrote it: gen %d -> %d", atKill.Gen, after.Gen)
	case atKill.Activity.Kind != layout.ActIdle && after.Gen <= atKill.Gen:
		t.Fatalf("return left the open %s session %q: gen %d -> %d", atKill.Activity.Kind, atKill.Activity.Session, atKill.Gen, after.Gen)
	}
	t.Logf("return resumed claim %s at epoch %d: activity at kill %s, gen %d -> %d", after.ClaimID, after.Epoch, atKill.Activity.Kind, atKill.Gen, after.Gen)
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
	gap1PrestartedTargetServesAfterMove(t, fixture, ws, remote, target, stem, filepath.Join(bin, "blockvolume"), args, root, volume, want)
	moved, err := gap1ReadHead(t, remote)
	if err != nil || moved.Holder != "s2/r1" {
		t.Fatal("target did not claim the moved line", moved, err)
	}
	gap1MovedSourceNeverServes(t, fixture, filepath.Join(bin, "blockvolume"), args, stem, statusAddr, volume, moved.Epoch)
}

// gap1MovedSourceNeverServes is the negative case of the in-process wait:
// after the volume moved to another host (and the new holder stopped), the
// old source started again never serves and never adopts the line naming the
// other host. The master proves it was the issued move's source and returns
// typed MOVED even before any own assignment: exit 78, no restart loop.
func gap1MovedSourceNeverServes(t *testing.T, fixture *gap1Fixture, exe string, args []string, stem, statusAddr, volume string, epoch uint64) {
	t.Helper()
	// The simulated hosts share this kernel's per-host volume lock directory:
	// stop the new holder (the prestarted target) so the old source can open
	// its root at all; on real separate hosts the two never share a lock.
	target := stem + "-prestarted-target"
	gap1Systemctl(t, "stop", target)
	waitGAP1(t, "the new holder did not stop", 30*time.Second, func() bool { return gap1Unit(t, target)["MainPID"] == "0" })
	unit := stem + "-moved-source"
	fixture.start(t, unit, true, exe, args...)
	waitGAP1(t, "the moved-away source never exited", 75*time.Second, func() bool {
		if gap1Operational(t, statusAddr, volume) {
			t.Fatal("the moved-away source served")
		}
		s := gap1Unit(t, unit)
		switch s["ExecMainStatus"] {
		case "", "0":
			return false
		case "78":
			return true
		}
		t.Fatalf("the moved-away source exited %s; want 78 moved, never retry 75: %+v", s["ExecMainStatus"], s)
		return false
	})
	if gap1Operational(t, statusAddr, volume) {
		t.Fatal("the moved-away source served after its exit")
	}
	// Observe beyond the shipped 5 s RestartSec. The prestarted target above
	// is the opposite control: a genuine 75 was restarted by the same policy.
	end := time.Now().Add(6 * time.Second)
	for time.Now().Before(end) {
		s := gap1Unit(t, unit)
		if s["NRestarts"] != "0" || s["MainPID"] != "0" || s["ExecMainStatus"] != "78" {
			t.Fatalf("moved source restarted or lost its terminal status: %+v", s)
		}
		time.Sleep(100 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	journal, err := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager", "--output=cat").CombinedOutput()
	if err != nil || !strings.Contains(string(journal), "status=78 reason=moved") || !strings.Contains(string(journal), fmt.Sprintf("holder s2 at epoch %d", epoch)) {
		t.Fatalf("moved exit lacks the master's destination proof: %v %s", err, journal)
	}
	if strings.Contains(string(journal), "adopted as PRIMARY") {
		t.Fatalf("the moved-away source adopted a lineage:\n%s", journal)
	}
	gap1Systemctl(t, "stop", unit)
}

// gap1MasterWait reads the volume's /status MasterWait (the startup wait's
// reason and the master's barrier time), zero when absent.
func gap1MasterWait(t *testing.T, addr, volume string) struct {
	Reason  string
	UntilMs int64
} {
	t.Helper()
	var out struct {
		MasterWait *struct {
			Reason  string
			UntilMs int64
		}
	}
	code, body, err := gap1StatusAt(addr, volume, "/status")
	if err != nil || code != http.StatusOK || json.Unmarshal(body, &out) != nil || out.MasterWait == nil {
		return struct {
			Reason  string
			UntilMs int64
		}{}
	}
	return *out.MasterWait
}

// Roots are deliberately independent of testing.TempDir: unknown unit cleanup
// must never remove an executable or WAL beneath a possible surviving service.
type gap1Fixture struct {
	root  string
	units []string
}

func newGAP1Fixture(t *testing.T) *gap1Fixture {
	t.Helper()
	root, err := os.MkdirTemp("", "gap1-runtime-")
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
	return gap1StatusAt(addr, volume, "/status/durable")
}
func gap1StatusAt(addr, volume, route string) (int, []byte, error) {
	r, e := (&http.Client{Timeout: time.Second}).Get("http://" + addr + route + "?volume=" + volume)
	if e != nil {
		return 0, nil, e
	}
	defer r.Body.Close()
	body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return r.StatusCode, body, e
}

// /status is a different schema from /status/durable. Presence checks make
// a wrong route or a missing field a failed oracle, not false readiness.
func gap1Readiness(status int, payload []byte, volume string) (bool, error) {
	if status != http.StatusOK {
		return false, fmt.Errorf("readiness HTTP status%d", status)
	}
	var body struct {
		VolumeID                      string
		Healthy, FrontendPrimaryReady *bool
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return false, err
	}
	if body.VolumeID != volume || body.Healthy == nil || body.FrontendPrimaryReady == nil {
		return false, fmt.Errorf("readiness response lacks exact volume and readiness fields: %s", payload)
	}
	return *body.Healthy || *body.FrontendPrimaryReady, nil
}

func TestGAP1ReadinessRejectsWrongSchema(t *testing.T) {
	projection := hostvolume.StatusProjection{}
	projection.VolumeID = "v"
	projectionPayload := func(healthy, ready bool) []byte {
		t.Helper()
		projection.Healthy, projection.FrontendPrimaryReady = healthy, ready
		b, err := json.Marshal(projection)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, tc := range []struct {
		name             string
		status           int
		body             []byte
		serving, refused bool
	}{
		{"not-ready", 200, projectionPayload(false, false), false, false},
		{"healthy", 200, projectionPayload(true, false), true, false},
		{"frontend-ready", 200, projectionPayload(false, true), true, false},
		{"durable-route", 200, []byte(`{"VolumeID":"v","Volumes":[{"VolumeID":"v","Operational":false}]}`), false, true},
		{"missing-readiness", 200, []byte(`{"VolumeID":"v","Healthy":false}`), false, true},
		{"wrong-volume", 200, []byte(`{"VolumeID":"other","Healthy":false,"FrontendPrimaryReady":false}`), false, true},
		{"http-refusal", 404, projectionPayload(false, false), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := gap1Readiness(tc.status, tc.body, "v")
			if got != tc.serving || (err != nil) != tc.refused {
				t.Fatalf("serving=%v err=%v want=%v refused=%v", got, err, tc.serving, tc.refused)
			}
		})
	}
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

// The host-consistency check uses the actual gRPC peer address. Model two
// registered hosts with loopback and a real non-loopback local IPv4 address;
// the target dials the master over that second address. Both processes and
// every restart still run under the same real WSL systemd (not two machines).
type gap1Target struct {
	masterAddr, dataAddr, ctrlAddr string
}

// gap1WriteTLS writes a CA, the master's certificate (valid for loopback and
// the target's address), node certificates for s1 and s2, an admin
// certificate and the role file to root/tls.
func gap1WriteTLS(t *testing.T, root string, target gap1Target) string {
	t.Helper()
	dir := filepath.Join(root, "tls")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	host, _, err := net.SplitHostPort(target.masterAddr)
	if err != nil {
		t.Fatal(err)
	}
	ca := authtest.NewCA(t, "gap1 CA")
	ca.WriteCA(t, dir)
	until := time.Now().Add(24 * time.Hour)
	ca.Issue(t, "blockmaster", until, net.ParseIP(host)).WriteFiles(t, dir, "master")
	var roles strings.Builder
	for _, p := range []struct{ name, role, id string }{{"s1", "node", "s1"}, {"s2", "node", "s2"}, {"admin", "admin", "gap1-test"}} {
		leaf := ca.Issue(t, p.id, until)
		leaf.WriteFiles(t, dir, p.name)
		fmt.Fprintln(&roles, leaf.Fingerprint, p.role, p.id)
	}
	if err := os.WriteFile(filepath.Join(dir, "roles"), []byte(roles.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func gap1TargetAddresses(t *testing.T, masterAddr string) (gap1Target, string) {
	t.Helper()
	_, port, err := net.SplitHostPort(masterAddr)
	if err != nil {
		t.Fatal(err)
	}
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			t.Fatal(err)
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err != nil || ip.To4() == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			allocate := func() string {
				ln, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
				if err != nil {
					t.Fatal(err)
				}
				addr := ln.Addr().String()
				if err := ln.Close(); err != nil {
					t.Fatal(err)
				}
				return addr
			}
			return gap1Target{net.JoinHostPort(ip.String(), port), allocate(), allocate()}, net.JoinHostPort("0.0.0.0", port)
		}
	}
	t.Fatal("qualification requires a real non-loopback IPv4 address; no skip is proof")
	return gap1Target{}, ""
}

// Start the future target before a clean move is issued. It has the same
// logical r1 identity but a different server id, and no own lineage: the source
// line cannot mean it lost anything. Prove75 and automatic restart, then issue
// a real MoveWorkspace and prove lease/HEAD/data admission without touching
// the target unit again. This is not a positive78/no-restart witness.
func gap1PrestartedTargetServesAfterMove(t *testing.T, fixture *gap1Fixture, ws control.WorkspaceServiceClient, remote storage.ChunkRemote, target gap1Target, stem, exe string, args []string, root, volume string, want []byte) {
	t.Helper()
	changed := append([]string(nil), args...)
	addr, nvmeAddr := leaseBinaryAddress(t), leaseBinaryAddress(t)
	nqn := "nqn.2026-11.test:" + stem + "-prestarted-target"
	replacements := map[string]string{"--master": target.masterAddr, "--server-id": "s2", "--data-addr": target.dataAddr, "--ctrl-addr": target.ctrlAddr, "--status-addr": addr, "--durable-root": filepath.Join(root, "target-store"), "--nvme-listen": nvmeAddr, "--nvme-subsysnqn": nqn,
		"--master-tls-cert": filepath.Join(root, "tls", "s2.crt"), "--master-tls-key": filepath.Join(root, "tls", "s2.key")}
	for i := 0; i+1 < len(changed); i++ {
		if value, ok := replacements[changed[i]]; ok {
			changed[i+1] = value
		}
	}
	unit := stem + "-prestarted-target"
	fixture.start(t, unit, true, exe, changed...)
	waitGAP1(t, "target never observed the source holder before the move", 15*time.Second, func() bool {
		status, body, err := gap1StatusAt(addr, volume, "/status")
		if err != nil || status != http.StatusOK {
			return false
		}
		serving, err := gap1Readiness(status, body, volume)
		if err != nil {
			t.Fatal(err)
		}
		if serving {
			t.Fatalf("future target served before the move: %s", body)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "journalctl", "--unit="+unit, "--no-pager", "--output=cat")
		cmd.WaitDelay = time.Second
		journal, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("target journal: %v %s", err, journal)
		}
		observed := fmt.Sprintf("assignment identity refusal volume=%q replica=%q server=%q", volume, "r1", "s1")
		return strings.Contains(string(journal), observed)
	})
	waitGAP1(t, "prestarted target did not exit75 before the move", 45*time.Second, func() bool {
		s := gap1Unit(t, unit)
		if s["ExecMainStatus"] == "78" {
			t.Fatalf("zero-lineage target chose no-retry before its move: %+v", s)
		}
		return s["ExecMainStatus"] == "75"
	})
	waitGAP1(t, "prestarted target retry75 did not restart", 15*time.Second, func() bool {
		s := gap1Unit(t, unit)
		n, err := strconv.Atoi(s["NRestarts"])
		if err != nil || n > 2 {
			t.Fatalf("invalid or unbounded target restart count: %+v", s)
		}
		return n >= 1 && s["MainPID"] != "0"
	})
	sealed, err := gap1ReadHead(t, remote)
	if err != nil || sealed.State != storage.ChunkHeadDetached || sealed.Holder != "s1/r1" || sealed.Gen == 0 {
		t.Fatalf("move source not sealed by its exact holder: %+v %v", sealed, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	grant, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: stem + "-target-move", VolumeId: volume, ExpectedReplicaId: "r1", ExpectedEpoch: sealed.Epoch, TargetReplicaId: "r1", TargetServerId: "s2", TargetDataAddr: target.dataAddr, TargetCtrlAddr: target.ctrlAddr, Kind: "clean"})
	cancel()
	if err != nil || !grant.GetIssued() || grant.GetNewEpoch() <= sealed.Epoch || grant.GetOldHost() != "s1" || grant.GetNewHost() != "s2" || grant.GetHeadGen() != sealed.Gen {
		t.Fatalf("real clean move failed: grant=%v err=%v", grant, err)
	}
	t.Logf("prestarted target move issued after its automatic restart: %s", grant)
	waitGAP1(t, "automatically restarted target did not serve after the move", 60*time.Second, func() bool {
		s := gap1Unit(t, unit)
		n, err := strconv.Atoi(s["NRestarts"])
		if err != nil || n < 1 || n > 2 || s["Result"] == "start-limit-hit" {
			t.Fatalf("target restart bound violated after move: %+v", s)
		}
		return gap1Operational(t, addr, volume)
	})
	head, err := gap1ReadHead(t, remote)
	if err != nil || head.Gen <= sealed.Gen || head.Epoch != grant.NewEpoch || head.Holder != "s2/r1" || head.State != storage.ChunkHeadActive {
		t.Fatalf("target did not claim its issued HEAD: %+v %v", head, err)
	}
	client := gap1ConnectNVMe(t, nvmeAddr, nqn)
	client.readExact(t, 8, want)
	client.readExact(t, 9, want)
	client.writeFlush(t, 10, want)
	client.readExact(t, 10, want)
	client.close()
	t.Logf("prestarted target served after a real clean move without operator restart: unit=%s HEAD=%+v", unit, head)
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
