package ops

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

func TestNewLiveVolumeStatusReportCollector_CollectsHTTPAndMasterSources(t *testing.T) {
	masterAddr, closeMaster := startOpsFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("volume"); got != "v1" {
			t.Fatalf("volume query=%q want v1", got)
		}
		switch r.URL.Path {
		case "/status":
			writeLiveJSON(t, w, hostvolume.StatusProjection{
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
			writeLiveJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{Peers: []replication.ReplicaPeerStatus{{ReplicaID: "r2", State: "healthy", Epoch: 7, EndpointVersion: 2}}})
		case "/status/durable":
			writeLiveJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "v1", ReplicaID: "r1", Latched: true, Operational: true}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	report, err := NewLiveVolumeStatusReportCollector(LiveVolumeStatusConfig{
		VolumeID:        "v1",
		MasterAddr:      masterAddr,
		StatusAddr:      statusServer.URL,
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
	}).Collect(context.Background())
	if err == nil {
		t.Fatal("expected residue non-claim collection error")
	}
	if !strings.Contains(err.Error(), "residue collection not implemented") {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Volume.VolumeID != "v1" || report.Volume.ReplicaID != "r1" {
		t.Fatalf("volume identity mismatch: %+v", report.Volume)
	}
	if len(report.Volume.Frontends) != 1 || report.Volume.Frontends[0].Protocol != "iscsi" {
		t.Fatalf("master frontend not collected: %+v", report.Volume.Frontends)
	}
	if report.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary || !report.Authority.FrontendPrimaryReady {
		t.Fatalf("local status not collected: %+v", report.Authority)
	}
	if len(report.Replication.Peers) != 1 || report.Replication.Peers[0].ReplicaID != "r2" {
		t.Fatalf("peers not collected: %+v", report.Replication.Peers)
	}
	if len(report.Durable) != 1 || !report.Durable[0].Latched {
		t.Fatalf("durable not collected: %+v", report.Durable)
	}
}

func TestNewLiveVolumeStatusReportCollector_TreatsOptionalPeerAndDurable404AsEmpty(t *testing.T) {
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		writeLiveJSON(t, w, hostvolume.StatusProjection{
			Projection: frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r2",
				Epoch:           7,
				EndpointVersion: 2,
			},
			AuthorityRole:   hostvolume.AuthorityRoleUnknown,
			ReplicationRole: hostvolume.ReplicationRoleReady,
		})
	}))
	defer statusServer.Close()

	report, err := NewLiveVolumeStatusReportCollector(LiveVolumeStatusConfig{
		VolumeID:        "v1",
		StatusAddr:      statusServer.URL,
		ProductRevision: "product-rev",
	}).Collect(context.Background())
	if err == nil {
		t.Fatal("expected residue non-claim collection error")
	}
	if !strings.Contains(err.Error(), "residue collection not implemented") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Replication.Peers) != 0 || report.Replication.Peers == nil {
		t.Fatalf("404 peers should collect as empty array: %+v", report.Replication.Peers)
	}
	if len(report.Durable) != 0 || report.Durable == nil {
		t.Fatalf("404 durable should collect as empty array: %+v", report.Durable)
	}
}

func TestStatusEndpointAddsSchemePathAndVolume(t *testing.T) {
	got, err := statusEndpoint("127.0.0.1:23260", "/status", "v1")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "http" || u.Host != "127.0.0.1:23260" || u.Path != "/status" || u.Query().Get("volume") != "v1" {
		t.Fatalf("endpoint=%q", got)
	}
}

func TestStatusEndpointPreservesBasePath(t *testing.T) {
	got, err := statusEndpoint("http://127.0.0.1:23260/proxy/base", "/status/peers", "v1")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/proxy/base/status/peers" || u.Query().Get("volume") != "v1" {
		t.Fatalf("endpoint=%q", got)
	}
}

type opsFakeEvidenceServer struct {
	control.UnimplementedEvidenceServiceServer
}

func (opsFakeEvidenceServer) QueryVolumeStatus(context.Context, *control.StatusRequest) (*control.StatusResponse, error) {
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

func startOpsFakeMaster(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := netListenLocal()
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	control.RegisterEvidenceServiceServer(srv, opsFakeEvidenceServer{})
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() {
		srv.Stop()
		_ = ln.Close()
	}
}

func netListenLocal() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

func writeLiveJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
}
