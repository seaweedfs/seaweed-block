package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/host/master"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestParseFlags_LifecycleStoreOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.lifecycleStore != "lifecycle-dir" {
		t.Fatalf("lifecycleStore=%q want lifecycle-dir", f.lifecycleStore)
	}
}

func TestParseFlags_VersionDoesNotRequireStores(t *testing.T) {
	f, err := parseFlags([]string{"--version"})
	if err != nil {
		t.Fatalf("parseFlags --version: %v", err)
	}
	if !f.version {
		t.Fatal("version flag not set")
	}
}

func TestParseFlags_LifecyclePlacementSeedOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
		"--lifecycle-placement-seed", "seed.json",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.lifecyclePlacementSeed != "seed.json" {
		t.Fatalf("lifecyclePlacementSeed=%q want seed.json", f.lifecyclePlacementSeed)
	}
}

func TestParseFlags_FailbackRuntimeRPCDisabledByDefault(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.failbackRuntimeRPC {
		t.Fatal("failback runtime RPC must be disabled by default")
	}
	f, err = parseFlags([]string{
		"--authority-store", "authority-dir",
		"--failback-runtime-rpc",
	})
	if err != nil {
		t.Fatalf("parseFlags with failback runtime RPC: %v", err)
	}
	if !f.failbackRuntimeRPC {
		t.Fatal("failback runtime RPC flag not set")
	}
}

func TestParseFlags_FrontendPublicationRuntimeHTTPDisabledByDefault(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.frontendPublicationRuntimeHTTP {
		t.Fatal("frontend publication runtime HTTP must be disabled by default")
	}
	f, err = parseFlags([]string{
		"--authority-store", "authority-dir",
		"--frontend-publication-runtime-http",
		"--frontend-publication-runtime-listen", "0.0.0.0:9334",
	})
	if err != nil {
		t.Fatalf("parseFlags with frontend publication runtime HTTP: %v", err)
	}
	if !f.frontendPublicationRuntimeHTTP || f.frontendPublicationRuntimeListen != "0.0.0.0:9334" {
		t.Fatalf("frontend publication runtime flags=%+v", f)
	}
}

func TestParseFlags_ClusterSpecOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
		"--cluster-spec", "m01.yaml",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.clusterSpec != "m01.yaml" {
		t.Fatalf("clusterSpec=%q want m01.yaml", f.clusterSpec)
	}
}

func TestParseFlags_LauncherExternalStatusOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
		"--launcher-status",
		"--launcher-external-status",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !f.launcherStatus || !f.launcherExternalStatus {
		t.Fatalf("launcher status flags = %v/%v want true/true", f.launcherStatus, f.launcherExternalStatus)
	}
}

func TestParseFlags_LauncherExternalISCSIRequiresCHAP(t *testing.T) {
	if _, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--launcher-external-iscsi",
	}); err == nil {
		t.Fatal("expected --launcher-external-iscsi without CHAP secret to fail")
	}
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--launcher-external-iscsi",
		"--launcher-iscsi-chap-secret-name", "sw-block-iscsi-chap",
	})
	if err != nil {
		t.Fatalf("parseFlags with CHAP: %v", err)
	}
	if !f.launcherExternalISCSI || f.launcherCHAPSecretName != "sw-block-iscsi-chap" {
		t.Fatalf("external iscsi flags = %v/%q", f.launcherExternalISCSI, f.launcherCHAPSecretName)
	}
}

func TestParseFlags_LauncherExternalNVMeOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--launcher-external-nvme",
	})
	if err != nil {
		t.Fatalf("parseFlags with external NVMe: %v", err)
	}
	if !f.launcherExternalNVMe {
		t.Fatal("launcherExternalNVMe=false")
	}
}

func TestParseFlags_LauncherNVMeMaxH2CDataLength(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--launcher-nvme-max-h2c-data-length", "65536",
	})
	if err != nil {
		t.Fatalf("parseFlags with launcher NVMe H2C candidate: %v", err)
	}
	if f.launcherNVMeMaxH2CDataLength != 65536 {
		t.Fatalf("launcherNVMeMaxH2CDataLength=%d want 65536", f.launcherNVMeMaxH2CDataLength)
	}

	_, err = parseFlags([]string{
		"--authority-store", "authority-dir",
		"--launcher-nvme-max-h2c-data-length", "49152",
	})
	if err == nil {
		t.Fatal("parseFlags succeeded; want invalid launcher NVMe H2C rejected")
	}
	if !strings.Contains(err.Error(), "--launcher-nvme-max-h2c-data-length=49152 invalid") {
		t.Fatalf("error=%v, want invalid launcher NVMe H2C", err)
	}
}

func TestPhase150_ParseFlagsLauncherWALMultiBlockRecords(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "/tmp/a",
		"--topology", "topology.yaml",
		"--launcher-durable-wal-multiblock-records",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !f.launcherWALMultiBlockRecords {
		t.Fatal("launcherWALMultiBlockRecords=false, want true")
	}
}

func TestPhase152_ParseFlagsLauncherWALRecoveryTestDisableFlusher(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "/tmp/a",
		"--topology", "topology.yaml",
		"--launcher-durable-wal-recovery-test-disable-flusher",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !f.launcherWALRecoveryTestDisableFlusher {
		t.Fatal("launcherWALRecoveryTestDisableFlusher=false, want true")
	}
}

func TestBlockmasterBareTopologyRegistersVolumeControlServices(t *testing.T) {
	h, err := master.New(master.Config{
		AuthorityStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = h.Close(ctx)
	}()

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{})
	if err == nil {
		t.Fatal("ReportHeartbeat with empty report unexpectedly succeeded")
	}
	if status.Code(err) == codes.Unimplemented || strings.Contains(err.Error(), "unknown service") {
		t.Fatalf("ObservationService not registered on bare blockmaster: %v", err)
	}
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		t.Fatalf("transport error while checking ObservationService registration: %v", err)
	}

	stream, err := control.NewAssignmentServiceClient(conn).SubscribeAssignments(ctx, &control.SubscribeRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if err == nil {
		t.Fatal("SubscribeAssignments with empty request unexpectedly succeeded")
	}
	if status.Code(err) == codes.Unimplemented || strings.Contains(err.Error(), "unknown service") {
		t.Fatalf("AssignmentService not registered on bare blockmaster: %v", err)
	}
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		t.Fatalf("transport error while checking AssignmentService registration: %v", err)
	}

	_, err = control.NewFailbackServiceClient(conn).ExecuteFailback(ctx, &control.FailbackRequest{VolumeId: "vol-a"})
	if status.Code(err) == codes.Unimplemented || strings.Contains(err.Error(), "unknown service") {
		t.Fatalf("FailbackService not registered on bare blockmaster: %v", err)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteFailback default-disabled code=%s err=%v", status.Code(err), err)
	}
}
