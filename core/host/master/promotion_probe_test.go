package master

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestWorkloadPlanPromotionEvidenceProvider_ProbesDurableFrontiers(t *testing.T) {
	var statusHits, durableHits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("volume") != "vol-rf3" {
			t.Fatalf("volume query=%q want vol-rf3", r.URL.Query().Get("volume"))
		}
		switch r.URL.Path {
		case "/status":
			statusHits++
			_ = json.NewEncoder(w).Encode(volume.StatusProjection{
				Projection:      frontend.Projection{VolumeID: "vol-rf3", ReplicaID: "r2"},
				ReplicationRole: "replica_ready",
			})
		case "/status/durable":
			durableHits++
			_ = json.NewEncoder(w).Encode(struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{
				VolumeID: "vol-rf3", ReplicaID: "r2", FrontierKnown: true, DurableLSN: 52,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider := NewWorkloadPlanPromotionEvidenceProvider([]lifecycle.BlockVolumeWorkloadPlan{{
		VolumeID: "vol-rf3",
		Protocol: "iscsi",
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{{
			ReplicaID: "r2", ISCSIListenPort: 3260,
		}},
	}}, WorkloadPlanPromotionProbeConfig{
		AckProfile:          "sync-quorum",
		RequiredLSNByVolume: map[string]uint64{"vol-rf3": 52},
		HTTPClient:          rewriteHTTPClient(server.URL, "127.0.0.1:23260"),
	})

	result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2"}})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if result.AckProfile != "sync-quorum" || result.SyncAckLSN != 52 {
		t.Fatalf("result profile/lsn=%+v", result)
	}
	if len(result.Candidates) != 1 {
		t.Fatalf("candidates=%d want 1", len(result.Candidates))
	}
	got := result.Candidates[0]
	if got.ReplicaID != "r2" || !got.Ready || got.DurableLSN != 52 {
		t.Fatalf("candidate=%+v want r2 ready durable 52", got)
	}
	if statusHits != 1 || durableHits != 1 {
		t.Fatalf("hits status=%d durable=%d want 1/1", statusHits, durableHits)
	}
}

func TestWorkloadPlanPromotionEvidenceProvider_NotReadyWithoutReplicaReadyRole(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_ = json.NewEncoder(w).Encode(volume.StatusProjection{
				Projection:      frontend.Projection{VolumeID: "vol-rf3", ReplicaID: "r2"},
				ReplicationRole: "not_ready",
			})
		case "/status/durable":
			_ = json.NewEncoder(w).Encode(struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{
				VolumeID: "vol-rf3", ReplicaID: "r2", FrontierKnown: true, DurableLSN: 90,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider := NewWorkloadPlanPromotionEvidenceProvider([]lifecycle.BlockVolumeWorkloadPlan{{
		VolumeID: "vol-rf3",
		Protocol: "iscsi",
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{{
			ReplicaID: "r2", ISCSIListenPort: 3260,
		}},
	}}, WorkloadPlanPromotionProbeConfig{
		AckProfile: "sync-quorum",
		HTTPClient: rewriteHTTPClient(server.URL, "127.0.0.1:23260"),
	})

	result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2"}})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	got := result.Candidates[0]
	if got.Ready || got.DurableLSN != 90 {
		t.Fatalf("candidate=%+v want durable evidence but not ready", got)
	}
}

func TestWorkloadPlanPromotionEvidenceProvider_CurrentUnknownWithoutProbeAddress(t *testing.T) {
	provider := NewWorkloadPlanPromotionEvidenceProvider(nil, WorkloadPlanPromotionProbeConfig{
		AckProfile: "sync-quorum",
	})
	result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{ReplicaID: "r1"}, nil)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if result.CurrentKnown {
		t.Fatalf("current without probe address must not be marked known: %+v", result)
	}
	if result.Current.ReplicaID != "r1" || result.Current.ProbeAddr != "missing" {
		t.Fatalf("current evidence=%+v", result.Current)
	}
}

func TestWorkloadPlanPromotionEvidenceProvider_CurrentKnownAfterSuccessfulProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_ = json.NewEncoder(w).Encode(volume.StatusProjection{
				Projection:           frontend.Projection{VolumeID: "vol-rf3", ReplicaID: "r1", Healthy: true},
				AuthorityRole:        volume.AuthorityRolePrimary,
				FrontendPrimaryReady: true,
			})
		case "/status/durable":
			_ = json.NewEncoder(w).Encode(struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{
				VolumeID: "vol-rf3", ReplicaID: "r1", FrontierKnown: true, DurableLSN: 52,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider := NewWorkloadPlanPromotionEvidenceProvider([]lifecycle.BlockVolumeWorkloadPlan{{
		VolumeID: "vol-rf3",
		Protocol: "iscsi",
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{{
			ReplicaID: "r1", ISCSIListenPort: 3260,
		}},
	}}, WorkloadPlanPromotionProbeConfig{
		AckProfile: "sync-quorum",
		HTTPClient: rewriteHTTPClient(server.URL, "127.0.0.1:23260"),
	})
	result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{ReplicaID: "r1"}, nil)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !result.CurrentKnown || result.Current.ReplicaID != "r1" || !result.Current.Ready {
		t.Fatalf("current evidence=%+v known=%t", result.Current, result.CurrentKnown)
	}
}

func TestNodeLoss_WorkloadPlanPromotionEvidenceProvider_ExternalStatusUsesNodeAddress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_ = json.NewEncoder(w).Encode(volume.StatusProjection{
				Projection:      frontend.Projection{VolumeID: "vol-rf3", ReplicaID: "r2"},
				ReplicationRole: volume.ReplicationRoleReady,
			})
		case "/status/durable":
			_ = json.NewEncoder(w).Encode(struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{
				VolumeID: "vol-rf3", ReplicaID: "r2", FrontierKnown: true, DurableLSN: 52,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider := NewWorkloadPlanPromotionEvidenceProvider([]lifecycle.BlockVolumeWorkloadPlan{{
		VolumeID: "vol-rf3",
		Protocol: "iscsi",
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{{
			ReplicaID: "r2", DataAddr: "10.0.0.2:19103", ISCSIListenPort: 3260,
		}},
	}}, WorkloadPlanPromotionProbeConfig{
		AckProfile:     "sync-quorum",
		ExternalStatus: true,
		HTTPClient:     rewriteHTTPClient(server.URL, "10.0.0.2:23260"),
	})

	result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2"}})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(result.Candidates) != 1 || !result.Candidates[0].Ready {
		t.Fatalf("candidates=%+v want r2 ready", result.Candidates)
	}
	if result.Candidates[0].ProbeAddr != "10.0.0.2:23260" {
		t.Fatalf("probe addr=%q want external node status addr", result.Candidates[0].ProbeAddr)
	}
}

func TestNodeLoss_WorkloadPlanPromotionEvidenceProvider_ExternalStatusRejectsLoopbackNodeAddress(t *testing.T) {
	provider := NewWorkloadPlanPromotionEvidenceProvider([]lifecycle.BlockVolumeWorkloadPlan{{
		VolumeID: "vol-rf3",
		Protocol: "iscsi",
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{{
			ReplicaID: "r2", DataAddr: "127.0.0.1:19103", ISCSIListenPort: 3260,
		}},
	}}, WorkloadPlanPromotionProbeConfig{
		AckProfile:     "sync-quorum",
		ExternalStatus: true,
	})

	result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2"}})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ProbeAddr != "missing" {
		t.Fatalf("candidates=%+v want missing probe addr for loopback node address", result.Candidates)
	}
}

func rewriteHTTPClient(targetBase, wantHost string) *http.Client {
	return &http.Client{Transport: rewriteRoundTripper{
		base: strings.TrimRight(targetBase, "/"),
		host: wantHost,
	}}
}

type rewriteRoundTripper struct {
	base string
	host string
}

func (r rewriteRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == r.host {
		rewritten := *req.URL
		baseReq, _ := http.NewRequest(http.MethodGet, r.base, nil)
		rewritten.Scheme = baseReq.URL.Scheme
		rewritten.Host = baseReq.URL.Host
		req = req.Clone(req.Context())
		req.URL = &rewritten
		req.Host = rewritten.Host
	}
	return http.DefaultTransport.RoundTrip(req)
}
