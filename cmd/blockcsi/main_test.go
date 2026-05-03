package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBlockCSI_BinaryStartsAndServesIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; -short skip")
	}
	bin := buildBlockCSIBinary(t)
	logPath := filepath.Join(t.TempDir(), "blockcsi.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("log file: %v", err)
	}
	defer lf.Close()

	cmd := exec.Command(bin,
		"--endpoint", "tcp://127.0.0.1:0",
		"--node-id", "node-a",
		"--t0-print-ready",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start blockcsi: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		if t.Failed() {
			if b, err := os.ReadFile(logPath); err == nil {
				t.Logf("=== blockcsi.log ===\n%s", b)
			}
		}
	})

	var ready readyLine
	if err := json.NewDecoder(stdout).Decode(&ready); err != nil {
		t.Fatalf("read ready line: %v", err)
	}
	if ready.Component != "blockcsi" || ready.Phase != "listening" || ready.Addr == "" {
		t.Fatalf("ready=%+v", ready)
	}

	conn, err := grpc.NewClient(ready.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial csi: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := csipb.NewIdentityClient(conn).GetPluginInfo(ctx, &csipb.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo: %v", err)
	}
	if info.GetName() != "block.csi.seaweedfs.com" {
		t.Fatalf("driver name=%q", info.GetName())
	}
}

func TestG15a_BlockCSIControllerPublishUsesMasterFrontendFact(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; -short skip")
	}
	bins := buildG15aBinaries(t)
	art := t.TempDir()

	master := startG15aMaster(t, bins.master, art)
	primaryData, primaryCtrl := pickTCPAddr(t), pickTCPAddr(t)
	replicaData, replicaCtrl := pickTCPAddr(t), pickTCPAddr(t)
	iscsiAddr := pickTCPAddr(t)
	iqn := "iqn.2026-05.io.seaweedfs:g15a-csi-v1"

	_ = startG15aVolume(t, bins.volume, art, g15aVolumeOpts{
		masterAddr: master.addr,
		serverID:   "s1",
		replicaID:  "r1",
		dataAddr:   primaryData,
		ctrlAddr:   primaryCtrl,
		iscsiAddr:  iscsiAddr,
		iscsiIQN:   iqn,
		logTag:     "primary",
	})
	_ = startG15aVolume(t, bins.volume, art, g15aVolumeOpts{
		masterAddr: master.addr,
		serverID:   "s2",
		replicaID:  "r2",
		dataAddr:   replicaData,
		ctrlAddr:   replicaCtrl,
		logTag:     "replica",
	})
	csi := startG15aCSI(t, bins.csi, master.addr)

	conn, err := grpc.NewClient(csi.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial blockcsi: %v", err)
	}
	defer conn.Close()
	client := csipb.NewControllerClient(conn)

	var last map[string]string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
		resp, err := client.ControllerPublishVolume(ctx, &csipb.ControllerPublishVolumeRequest{
			VolumeId: "v1",
			NodeId:   "node-a",
		})
		cancel()
		if err == nil {
			last = resp.GetPublishContext()
			if last["iscsiAddr"] == iscsiAddr && last["iqn"] == iqn {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("ControllerPublish did not return frontend fact; last publish_context=%v", last)
}

type g15aBins struct {
	master string
	volume string
	csi    string
}

func buildG15aBinaries(t *testing.T) g15aBins {
	t.Helper()
	dir := t.TempDir()
	suf := ""
	if os.PathSeparator == '\\' {
		suf = ".exe"
	}
	bins := g15aBins{
		master: filepath.Join(dir, "blockmaster"+suf),
		volume: filepath.Join(dir, "blockvolume"+suf),
		csi:    filepath.Join(dir, "blockcsi"+suf),
	}
	for _, b := range []struct{ pkg, out string }{
		{"github.com/seaweedfs/seaweed-block/cmd/blockmaster", bins.master},
		{"github.com/seaweedfs/seaweed-block/cmd/blockvolume", bins.volume},
		{"github.com/seaweedfs/seaweed-block/cmd/blockcsi", bins.csi},
	} {
		cmd := exec.Command("go", "build", "-o", b.out, b.pkg)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", b.pkg, err, out)
		}
	}
	return bins
}

type g15aProc struct {
	cmd     *exec.Cmd
	logPath string
	addr    string
}

func (p *g15aProc) stop(t *testing.T) {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = p.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = p.cmd.Process.Kill()
			<-done
		}
	}
	if t.Failed() {
		if b, err := os.ReadFile(p.logPath); err == nil {
			t.Logf("=== %s ===\n%s", filepath.Base(p.logPath), b)
		}
	}
}

func startG15aMaster(t *testing.T, bin, art string) *g15aProc {
	t.Helper()
	topo := filepath.Join(art, "topology.yaml")
	if err := os.WriteFile(topo, []byte(`volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r2
        server_id: s2
`), 0o644); err != nil {
		t.Fatalf("topology: %v", err)
	}
	logPath := filepath.Join(art, "blockmaster.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bin,
		"--authority-store", filepath.Join(art, "authority-store"),
		"--listen", "127.0.0.1:0",
		"--topology", topo,
		"--expected-slots-per-volume", "2",
		"--t0-print-ready",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		lf.Close()
		t.Fatalf("master stdout: %v", err)
	}
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("master start: %v", err)
	}
	p := &g15aProc{cmd: cmd, logPath: logPath}
	t.Cleanup(func() {
		p.stop(t)
		lf.Close()
	})
	var ready struct {
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(stdout).Decode(&ready); err != nil {
		t.Fatalf("master ready: %v", err)
	}
	if ready.Addr == "" {
		t.Fatal("master ready missing addr")
	}
	p.addr = ready.Addr
	return p
}

type g15aVolumeOpts struct {
	masterAddr string
	serverID   string
	replicaID  string
	dataAddr   string
	ctrlAddr   string
	iscsiAddr  string
	iscsiIQN   string
	logTag     string
}

func startG15aVolume(t *testing.T, bin, art string, o g15aVolumeOpts) *g15aProc {
	t.Helper()
	logPath := filepath.Join(art, fmt.Sprintf("blockvolume-%s.log", o.logTag))
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("volume log: %v", err)
	}
	cmd := exec.Command(bin,
		"--master", o.masterAddr,
		"--server-id", o.serverID,
		"--volume-id", "v1",
		"--replica-id", o.replicaID,
		"--data-addr", o.dataAddr,
		"--ctrl-addr", o.ctrlAddr,
		"--durable-root", filepath.Join(art, "store-"+o.logTag),
		"--durable-impl", "walstore",
		"--durable-blocks", "256",
		"--durable-blocksize", "4096",
		"--heartbeat-interval", "200ms",
		"--t1-readiness",
	)
	if o.iscsiAddr != "" {
		cmd.Args = append(cmd.Args,
			"--iscsi-listen", o.iscsiAddr,
			"--iscsi-iqn", o.iscsiIQN,
		)
	}
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("volume start: %v", err)
	}
	p := &g15aProc{cmd: cmd, logPath: logPath}
	t.Cleanup(func() {
		p.stop(t)
		lf.Close()
	})
	return p
}

func startG15aCSI(t *testing.T, bin, masterAddr string) *g15aProc {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "blockcsi.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("csi log: %v", err)
	}
	cmd := exec.Command(bin,
		"--endpoint", "tcp://127.0.0.1:0",
		"--master", masterAddr,
		"--node-id", "node-a",
		"--t0-print-ready",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		lf.Close()
		t.Fatalf("csi stdout: %v", err)
	}
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("csi start: %v", err)
	}
	p := &g15aProc{cmd: cmd, logPath: logPath}
	t.Cleanup(func() {
		p.stop(t)
		lf.Close()
	})
	var ready readyLine
	if err := json.NewDecoder(stdout).Decode(&ready); err != nil {
		t.Fatalf("csi ready: %v", err)
	}
	if ready.Addr == "" {
		t.Fatal("csi ready missing addr")
	}
	p.addr = ready.Addr
	return p
}

func pickTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func buildBlockCSIBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	suf := ""
	if os.PathSeparator == '\\' {
		suf = ".exe"
	}
	out := filepath.Join(dir, "blockcsi"+suf)
	cmd := exec.Command("go", "build", "-o", out, "github.com/seaweedfs/seaweed-block/cmd/blockcsi")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build blockcsi: %v\n%s", err, b)
	}
	return out
}
