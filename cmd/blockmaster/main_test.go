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
}
