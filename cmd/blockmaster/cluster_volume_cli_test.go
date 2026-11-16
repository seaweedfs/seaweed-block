package main

import (
	"context"
	"encoding/json"
	"fmt"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestClusterVolumeCLIHelper(t *testing.T) {
	file := os.Getenv("BLOCKMASTER_IMPORT_ARGS")
	if file == "" {
		t.Skip("subprocess helper")
	}
	data, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	var args []string
	if e = json.Unmarshal(data, &args); e != nil {
		t.Fatal(e)
	}
	f, e := parseFlags(args)
	if e != nil {
		t.Fatal(e)
	}
	os.Exit(run(f))
}

type clusterImportProcess struct {
	addr, log string
	stop      func()
}

func startClusterImportCLI(t *testing.T, root, spec string) *clusterImportProcess {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("native daemon signal control")
	}
	tmp := t.TempDir()
	args := []string{"--authority-store", filepath.Join(root, "authority"), "--lifecycle-store", filepath.Join(root, "lifecycle"), "--cluster-spec", spec, "--listen", "127.0.0.1:0", "--expected-slots-per-volume", "1", "--lifecycle-product-loop-interval", "5ms", "--t0-print-ready"}
	argsPath := filepath.Join(tmp, "args.json")
	data, _ := json.Marshal(args)
	if e := os.WriteFile(argsPath, data, 0600); e != nil {
		t.Fatal(e)
	}
	outPath, logPath := filepath.Join(tmp, "out"), filepath.Join(tmp, "log")
	out, e := os.Create(outPath)
	if e != nil {
		t.Fatal(e)
	}
	log, e := os.Create(logPath)
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestClusterVolumeCLIHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "BLOCKMASTER_IMPORT_ARGS="+argsPath)
	cmd.Stdout = out
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stopped := false
	p := &clusterImportProcess{log: logPath}
	p.stop = func() {
		if stopped {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
			stopped = true
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-done:
				stopped = true
			case <-time.After(5 * time.Second):
				t.Error("blockmaster failed to reap")
			}
		}
		out.Close()
		log.Close()
	}
	t.Cleanup(p.stop)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(outPath)
		for _, line := range strings.Split(string(b), "\n") {
			var ready readyLine
			if json.Unmarshal([]byte(line), &ready) == nil && ready.Component == "blockmaster" && ready.Phase == "listening" {
				p.addr = ready.Addr
				return p
			}
		}
		select {
		case e := <-done:
			stopped = true
			b, _ := os.ReadFile(logPath)
			t.Fatalf("blockmaster startup failed: %v %s", e, b)
		case <-time.After(5 * time.Millisecond):
		}
	}
	b, _ := os.ReadFile(logPath)
	t.Fatalf("blockmaster ready timeout: %s", b)
	return nil
}
func importedVolumeClients(t *testing.T, p *clusterImportProcess) (control.ObservationServiceClient, control.EvidenceServiceClient, control.LifecycleServiceClient) {
	t.Helper()
	c, e := grpc.NewClient(p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return control.NewObservationServiceClient(c), control.NewEvidenceServiceClient(c), control.NewLifecycleServiceClient(c)
}
func reportImportedVolume(t *testing.T, o control.ObservationServiceClient) {
	t.Helper()
	ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	_, e := o.ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "s2", SentAt: timestamppb.Now(), Reachable: true, Eligible: true, Slots: []*control.HeartbeatSlot{{VolumeId: "v1", ReplicaId: "r2", DataAddr: "10.0.0.2:19080", CtrlAddr: "10.0.0.2:19081"}}})
	if e != nil {
		t.Fatal(e)
	}
}
func waitImportedAuthority(t *testing.T, p *clusterImportProcess, evidence control.EvidenceServiceClient) {
	t.Helper()
	ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	for {
		s, e := evidence.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: "v1"})
		if e == nil && s.Assigned && s.Epoch == 1 && s.ReplicaId == "r2" {
			return
		}
		select {
		case <-ctx.Done():
			log, _ := os.ReadFile(p.log)
			t.Fatalf("cluster-spec volume did not publish authority: %v %v log=%s", s, e, log)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func TestClusterSpecCLIRegistersVolumeBeforePublication(t *testing.T) {
	root := t.TempDir()
	spec := writeClusterSpecForTest(t)
	p := startClusterImportCLI(t, root, spec)
	o, e, l := importedVolumeClients(t, p)
	reportImportedVolume(t, o)
	waitImportedAuthority(t, p, e)
	ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	same := &control.CreateVolumeRequest{VolumeId: "v1", SizeBytes: 1048576, ReplicationFactor: 1}
	got, err := l.CreateVolume(ctx, same)
	if err != nil || got.SizeBytes != 1048576 || got.ReplicationFactor != 1 {
		t.Fatalf("imported desired shape changed: %v %v", got, err)
	}
	if _, err = l.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v1", SizeBytes: 2097152, ReplicationFactor: 1}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("import did not preserve exact capacity: %v", err)
	}
	p.stop()
	p2 := startClusterImportCLI(t, root, spec)
	o2, e2, _ := importedVolumeClients(t, p2)
	reportImportedVolume(t, o2)
	waitImportedAuthority(t, p2, e2)
}
func TestClusterSpecCLIMissingSizeExplainsCreateVolumePrerequisite(t *testing.T) {
	spec := writeClusterSpecForTest(t)
	b, err := os.ReadFile(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(spec, []byte(strings.Replace(string(b), "    size_bytes: 1048576\n", "", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	p := startClusterImportCLI(t, root, spec)
	o, e, l := importedVolumeClients(t, p)
	reportImportedVolume(t, o)
	expected := "no lifecycle VolumeRecord for v1; register it (CreateVolume) before publishing"
	deadline := time.Now().Add(3 * time.Second)
	for {
		log, _ := os.ReadFile(p.log)
		if strings.Contains(string(log), expected) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing actionable CreateVolume prerequisite: %s", log)
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	before, err := e.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: "v1"})
	if err != nil || before.Assigned {
		t.Fatal(fmt.Sprint("unregistered volume assigned: ", before, err))
	}
	if _, err = l.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v1", SizeBytes: 1048576, ReplicationFactor: 1, Protocol: "nvme", FrontendTransport: "tcp", PvcNamespace: "workspaces", PvcName: "claim-v1"}); err != nil {
		t.Fatal(err)
	}
	reportImportedVolume(t, o)
	waitImportedAuthority(t, p, e)
	// A later complete spec must not replace fields it does not own.
	p.stop()
	if err = os.WriteFile(spec, b, 0600); err != nil {
		t.Fatal(err)
	}
	p2 := startClusterImportCLI(t, root, spec)
	o2, e2, l2 := importedVolumeClients(t, p2)
	reportImportedVolume(t, o2)
	waitImportedAuthority(t, p2, e2)
	got, err := l2.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v1", SizeBytes: 1048576, ReplicationFactor: 1, Protocol: "nvme", FrontendTransport: "tcp"})
	if err != nil || got.Protocol != "nvme" || got.FrontendTransport != "tcp" || got.PvcNamespace != "workspaces" || got.PvcName != "claim-v1" {
		t.Fatalf("cluster import changed existing protocol/metadata: %v %v", got, err)
	}

}
