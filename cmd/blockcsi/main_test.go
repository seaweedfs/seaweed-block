package main

import (
	"context"
	"encoding/json"
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
