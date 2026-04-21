package host_test

// Harness for D1 L2 subprocess tests. Starts cmd/blockmaster and
// cmd/blockvolume as real compiled binaries, talks to them only
// via gRPC and observable process state. Deliberately does NOT
// import core/authority internals — L2 must behave like an
// external operator driving the daemons.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// l2bins holds paths to the compiled blockmaster / blockvolume
// binaries for this test run. Built once per test invocation via
// buildL2Binaries.
type l2bins struct {
	master string
	volume string
}

// buildL2Binaries compiles the two product binaries into a
// dedicated tempdir and returns their paths. Skips the test if
// `go build` fails — build errors block the L2 suite.
func buildL2Binaries(t *testing.T) l2bins {
	t.Helper()
	outDir := t.TempDir()

	name := func(s string) string {
		if runtime.GOOS == "windows" {
			return s + ".exe"
		}
		return s
	}
	bins := l2bins{
		master: filepath.Join(outDir, name("blockmaster")),
		volume: filepath.Join(outDir, name("blockvolume")),
	}
	for target, out := range map[string]string{
		"github.com/seaweedfs/seaweed-block/cmd/blockmaster": bins.master,
		"github.com/seaweedfs/seaweed-block/cmd/blockvolume": bins.volume,
	} {
		cmd := exec.Command("go", "build", "-o", out, target)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("build %s: %v", target, err)
		}
	}
	return bins
}

// writeL2Topology writes a three-slot accepted topology for
// volume v1 to a YAML file under dir and returns the path.
func writeL2Topology(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "topology.yaml")
	body := `
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r2
        server_id: s2
      - replica_id: r3
        server_id: s3
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("topology yaml: %v", err)
	}
	return path
}

// masterProc wraps a running blockmaster process.
type masterProc struct {
	cmd       *exec.Cmd
	addr      string
	storeDir  string
	topology  string
	logPath   string
	logFile   *os.File
	stdoutBuf *strings.Builder
	readyLine []byte
}

// startMaster launches cmd/blockmaster with --t0-print-ready, waits
// for the ready line, returns the bound addr. Deletes the process
// on test cleanup.
func startMaster(t *testing.T, bins l2bins, storeDir, topologyPath, artifactDir string) *masterProc {
	t.Helper()
	logPath := filepath.Join(artifactDir, "blockmaster.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("open master log: %v", err)
	}
	stdoutBuf := &strings.Builder{}

	cmd := exec.Command(bins.master,
		"--authority-store", storeDir,
		"--listen", "127.0.0.1:0",
		"--topology", topologyPath,
		"--t0-print-ready",
	)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		lf.Close()
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = lf

	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("start master: %v", err)
	}

	// Read first stdout line — the ready line.
	readyCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		// Tee stdout to the log and the buffer.
		br := bufio.NewReader(io.TeeReader(stdoutPipe, &teeWriter{buf: stdoutBuf, also: lf}))
		line, rerr := br.ReadBytes('\n')
		if rerr != nil && rerr != io.EOF {
			errCh <- rerr
			return
		}
		readyCh <- line
		// Drain remaining stdout into log so pipe never blocks.
		_, _ = io.Copy(io.Discard, br)
	}()

	var ready []byte
	select {
	case ready = <-readyCh:
	case err := <-errCh:
		_ = cmd.Process.Kill()
		lf.Close()
		t.Fatalf("master stdout: %v", err)
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		lf.Close()
		t.Fatalf("master did not emit ready line within 15s; stdout so far: %q", stdoutBuf.String())
	}

	// Parse ready line: {"component":"blockmaster","phase":"listening","addr":"127.0.0.1:PORT"}
	var rl struct {
		Component string `json:"component"`
		Phase     string `json:"phase"`
		Addr      string `json:"addr"`
	}
	if err := json.Unmarshal(ready, &rl); err != nil {
		_ = cmd.Process.Kill()
		lf.Close()
		t.Fatalf("parse master ready line %q: %v", ready, err)
	}
	if rl.Addr == "" {
		_ = cmd.Process.Kill()
		lf.Close()
		t.Fatalf("master ready line missing addr: %s", ready)
	}

	mp := &masterProc{
		cmd:       cmd,
		addr:      rl.Addr,
		storeDir:  storeDir,
		topology:  topologyPath,
		logPath:   logPath,
		logFile:   lf,
		stdoutBuf: stdoutBuf,
		readyLine: ready,
	}
	t.Cleanup(func() { mp.stop(t) })
	return mp
}

func (m *masterProc) stop(t *testing.T) {
	t.Helper()
	if m.cmd != nil && m.cmd.Process != nil {
		_ = m.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = m.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = m.cmd.Process.Kill()
			<-done
		}
	}
	if m.logFile != nil {
		_ = m.logFile.Close()
	}
}

// sigKill sends SIGKILL (simulate crash; no graceful shutdown).
func (m *masterProc) sigKill(t *testing.T) {
	t.Helper()
	if m.cmd == nil || m.cmd.Process == nil {
		return
	}
	_ = m.cmd.Process.Kill()
	_ = m.cmd.Wait()
}

// volumeProc wraps a running blockvolume process.
type volumeProc struct {
	cmd     *exec.Cmd
	logFile *os.File
	logPath string
}

type volumeOpts struct {
	ServerID  string
	VolumeID  string
	ReplicaID string
	DataAddr  string
	CtrlAddr  string
}

// startVolume launches cmd/blockvolume pointed at a master addr.
func startVolume(t *testing.T, bins l2bins, masterAddr, artifactDir string, opts volumeOpts) *volumeProc {
	t.Helper()
	logPath := filepath.Join(artifactDir, fmt.Sprintf("blockvolume-%s.log", opts.ServerID))
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("open volume log: %v", err)
	}
	cmd := exec.Command(bins.volume,
		"--master", masterAddr,
		"--server-id", opts.ServerID,
		"--volume-id", opts.VolumeID,
		"--replica-id", opts.ReplicaID,
		"--data-addr", opts.DataAddr,
		"--ctrl-addr", opts.CtrlAddr,
		"--heartbeat-interval", "200ms",
	)
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("start volume %s: %v", opts.ServerID, err)
	}
	vp := &volumeProc{cmd: cmd, logFile: lf, logPath: logPath}
	t.Cleanup(func() { vp.stop(t) })
	return vp
}

func (v *volumeProc) stop(t *testing.T) {
	t.Helper()
	if v.cmd != nil && v.cmd.Process != nil {
		_ = v.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = v.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = v.cmd.Process.Kill()
			<-done
		}
	}
	if v.logFile != nil {
		_ = v.logFile.Close()
	}
}

// sigKill sends SIGKILL to a volume process (crash simulation).
func (v *volumeProc) sigKill(t *testing.T) {
	t.Helper()
	if v.cmd == nil || v.cmd.Process == nil {
		return
	}
	_ = v.cmd.Process.Kill()
	_ = v.cmd.Wait()
}

// teeWriter writes to both a string builder and a file — used to
// capture subprocess stdout into both an in-memory buffer (for
// fast in-test assertions) and a persistent log file (for failure
// artifact collection).
type teeWriter struct {
	mu   sync.Mutex
	buf  *strings.Builder
	also io.Writer
}

func (t *teeWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf.Write(p)
	if t.also != nil {
		_, _ = t.also.Write(p)
	}
	return len(p), nil
}

// queryStatus invokes the master's EvidenceService.QueryVolumeStatus
// over gRPC and returns the response. Used by tests to observe
// authority state without importing authority internals.
func queryStatus(ctx context.Context, masterAddr, volumeID string) (*control.StatusResponse, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	cli := control.NewEvidenceServiceClient(conn)
	return cli.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: volumeID})
}

// pollAssigned polls QueryVolumeStatus until Assigned=true, times
// out otherwise. Returns the final observed status.
func pollAssigned(t *testing.T, masterAddr, volumeID string, deadline time.Duration) *control.StatusResponse {
	t.Helper()
	end := time.Now().Add(deadline)
	var last *control.StatusResponse
	for time.Now().Before(end) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := queryStatus(ctx, masterAddr, volumeID)
		cancel()
		if err == nil {
			last = resp
			if resp.Assigned {
				return resp
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if last != nil {
		t.Fatalf("volume %s not assigned within %v; last status: %+v", volumeID, deadline, last)
	}
	t.Fatalf("volume %s never observed within %v (no successful status query)", volumeID, deadline)
	return nil
}

// authorityTuple is the minimal identity-plus-lineage snapshot
// used by kill-restart tests to detect state drift across cycles.
type authorityTuple struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	Assigned        bool
}

func (a authorityTuple) String() string {
	return fmt.Sprintf("{vol=%s rid=%s epoch=%d ev=%d assigned=%v}",
		a.VolumeID, a.ReplicaID, a.Epoch, a.EndpointVersion, a.Assigned)
}

func tupleOf(resp *control.StatusResponse) authorityTuple {
	return authorityTuple{
		VolumeID:        resp.VolumeId,
		ReplicaID:       resp.ReplicaId,
		Epoch:           resp.Epoch,
		EndpointVersion: resp.EndpointVersion,
		Assigned:        resp.Assigned,
	}
}

// mkArtifactDir creates a per-test artifact dir. On test success
// it is cleaned up; on failure the caller preserves it via
// t.Cleanup ordering (the log files it contains survive because
// test failure suppresses the cleanup of t.TempDir).
func mkArtifactDir(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "l2-artifacts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir artifact: %v", err)
	}
	return dir
}

// ensureNotErr fails if err is not nil, logging both err and
// any context provided.
func ensureNotErr(t *testing.T, err error, ctx string) {
	t.Helper()
	if err == nil {
		return
	}
	if errors.Is(err, io.EOF) {
		return
	}
	t.Fatalf("%s: %v", ctx, err)
}
