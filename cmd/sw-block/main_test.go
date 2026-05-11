package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

func TestOpsStatusWritesArtifactsAndReturnsClean(t *testing.T) {
	oldRunCommand := opsStatusRunCommand
	opsStatusRunCommand = cleanCmdResidueCommand
	defer func() { opsStatusRunCommand = oldRunCommand }()

	masterAddr, closeMaster := startCmdFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			writeCmdJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:        "v1",
					ReplicaID:       "r1",
					Epoch:           7,
					EndpointVersion: 2,
					Healthy:         true,
				},
				FrontendPrimaryReady: true,
				AuthorityRole:        hostvolume.AuthorityRolePrimary,
				ReplicationRole:      hostvolume.ReplicationRoleNone,
			})
		case "/status/peers":
			writeCmdJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{Peers: []replication.ReplicaPeerStatus{{ReplicaID: "r2", State: "healthy", Epoch: 7, EndpointVersion: 2}}})
		case "/status/durable":
			writeCmdJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "v1", ReplicaID: "r1", Latched: true, Operational: true}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	outDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"ops", "status",
		"--volume", "v1",
		"--master", masterAddr,
		"--status-addr", statusServer.URL,
		"--out", outDir,
		"--product-revision", "product-rev",
		"--runner-revision", "runner-rev",
	}, &stdout, &stderr)
	if code != ops.VolumeStatusExitOK {
		t.Fatalf("exit=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, name := range []string{ops.VolumeStatusReportArtifact, ops.VolumeStatusSummaryArtifact} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	if !strings.Contains(stdout.String(), "status: ok") {
		t.Fatalf("stdout missing summary:\n%s", stdout.String())
	}
	raw, err := os.ReadFile(filepath.Join(outDir, ops.VolumeStatusReportArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var report ops.VolumeStatusReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if report.Volume.VolumeID != "v1" || len(report.Volume.Frontends) != 1 {
		t.Fatalf("report mismatch: %+v", report)
	}
	if len(report.CollectionErrors) != 0 {
		t.Fatalf("unexpected collection errors: %+v", report.CollectionErrors)
	}
}

func cleanCmdResidueCommand(_ context.Context, name string, args ...string) ([]byte, error) {
	switch name {
	case "iscsiadm":
		return []byte("iscsiadm: No active sessions.\n"), errors.New("exit status 21")
	case "nvme":
		return []byte(`{"Subsystems":[]}`), nil
	case "ps", "tasklist":
		return []byte("PID ARGS\n1 unrelated\n"), nil
	default:
		return nil, fmt.Errorf("unexpected command %s", name)
	}
}

func TestOpsStatusMissingRequiredArgsReturnsInvalid(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "status", "--volume", "v1"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "--volume and --out are required") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

func TestOpsStatusRequiresMasterAndStatusAddr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"ops", "status", "--volume", "v1", "--out", t.TempDir(), "--status-addr", "127.0.0.1:1"}, &stdout, &stderr)
	if code != ops.VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, ops.VolumeStatusExitInvalid)
	}
	if !strings.Contains(stderr.String(), "--master and --status-addr are both required") {
		t.Fatalf("stderr=%s", stderr.String())
	}
}

type cmdFakeEvidenceServer struct {
	control.UnimplementedEvidenceServiceServer
}

func (cmdFakeEvidenceServer) QueryVolumeStatus(context.Context, *control.StatusRequest) (*control.StatusResponse, error) {
	return &control.StatusResponse{
		VolumeId:        "v1",
		ReplicaId:       "r1",
		Epoch:           7,
		EndpointVersion: 2,
		Assigned:        true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
		},
	}, nil
}

func startCmdFakeMaster(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	control.RegisterEvidenceServiceServer(srv, cmdFakeEvidenceServer{})
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() {
		srv.Stop()
		_ = ln.Close()
	}
}

func writeCmdJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
}
