package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestG15c_BlockCSICreateVolumeWritesMasterLifecycleIntent(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess dynamic provisioning test; -short skip")
	}
	bins := buildG15aBinaries(t)
	art := t.TempDir()
	lifecycleDir := filepath.Join(art, "lifecycle")

	master := startG15cMaster(t, bins.master, art, lifecycleDir)
	csi := startG15aCSI(t, bins.csi, master.addr)

	conn, err := grpc.NewClient(csi.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial blockcsi: %v", err)
	}
	defer conn.Close()
	client := csipb.NewControllerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.CreateVolume(ctx, &csipb.CreateVolumeRequest{
		Name: "pvc-a",
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: 1 << 20,
		},
		Parameters: map[string]string{"replicationFactor": "2"},
		VolumeCapabilities: []*csipb.VolumeCapability{
			testMountCapability(),
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.GetVolume().GetVolumeId() != "pvc-a" {
		t.Fatalf("volume id=%q want pvc-a", resp.GetVolume().GetVolumeId())
	}
	raw, err := os.ReadFile(filepath.Join(lifecycleDir, "volumes", "pvc-a.json"))
	if err != nil {
		t.Fatalf("read lifecycle volume record: %v", err)
	}
	var rec struct {
		Spec struct {
			VolumeID          string `json:"volume_id"`
			SizeBytes         uint64 `json:"size_bytes"`
			ReplicationFactor int    `json:"replication_factor"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("parse lifecycle volume record: %v", err)
	}
	if rec.Spec.VolumeID != "pvc-a" || rec.Spec.SizeBytes != 1<<20 || rec.Spec.ReplicationFactor != 2 {
		t.Fatalf("record=%+v", rec.Spec)
	}
}

func startG15cMaster(t *testing.T, bin, art, lifecycleDir string) *g15aProc {
	t.Helper()
	logPath := filepath.Join(art, "blockmaster-g15c.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bin,
		"--authority-store", filepath.Join(art, "authority-store"),
		"--lifecycle-store", lifecycleDir,
		"--listen", "127.0.0.1:0",
		"--lifecycle-product-loop-interval", "100ms",
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
	var ready readyLine
	if err := json.NewDecoder(stdout).Decode(&ready); err != nil {
		t.Fatalf("master ready: %v", err)
	}
	if ready.Addr == "" {
		t.Fatal("master ready missing addr")
	}
	p.addr = ready.Addr
	return p
}

func testMountCapability() *csipb.VolumeCapability {
	return &csipb.VolumeCapability{
		AccessType: &csipb.VolumeCapability_Mount{
			Mount: &csipb.VolumeCapability_MountVolume{FsType: "ext4"},
		},
		AccessMode: &csipb.VolumeCapability_AccessMode{
			Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
}
