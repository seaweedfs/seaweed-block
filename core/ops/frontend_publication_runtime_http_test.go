package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult(t *testing.T) {
	var got FrontendPublicationRuntimeRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("content-type=%q", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(FrontendPublicationRuntimeResult{
			FrontendPublished:           true,
			FailbackStarted:             false,
			NoStorageMutation:           true,
			NoCrossVolumeIdentityChange: true,
			EvidenceRefs:                []string{"frontend-runtime.txt"},
		})
	}))
	defer server.Close()

	result, err := NewHTTPFrontendPublicationRuntime(server.URL, server.Client()).ExecuteFrontendPublication(context.Background(), FrontendPublicationRuntimeRequest{
		VolumeName:                   "demo",
		VolumeID:                     "pvc-demo",
		PVCName:                      "demo-pvc",
		ReplicaID:                    "r2",
		RuntimeEndpoint:              server.URL,
		AckEligibilityKnown:          true,
		AckEligible:                  true,
		FrontendFencedAfterExecution: true,
		PrimaryUnchanged:             true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"target.txt"},
	})
	if err != nil {
		t.Fatalf("execute frontend publication: %v", err)
	}
	if got.VolumeName != "demo" ||
		got.VolumeID != "pvc-demo" ||
		got.ReplicaID != "r2" ||
		!got.AckEligible ||
		!got.NoCrossVolumeIdentityChange {
		t.Fatalf("request=%+v", got)
	}
	if !result.FrontendPublished ||
		result.FailbackStarted ||
		!result.NoStorageMutation ||
		!result.NoCrossVolumeIdentityChange ||
		!authorityExecutorStringSliceContains(result.EvidenceRefs, "frontend-runtime.txt") {
		t.Fatalf("result=%+v", result)
	}
}

func TestHTTPFrontendPublicationRuntimeReturnsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := NewHTTPFrontendPublicationRuntime(server.URL, server.Client()).ExecuteFrontendPublication(context.Background(), FrontendPublicationRuntimeRequest{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") || !strings.Contains(err.Error(), "runtime unavailable") {
		t.Fatalf("err=%v", err)
	}
}

func TestHTTPFrontendPublicationRuntimeRequiresEndpoint(t *testing.T) {
	_, err := NewHTTPFrontendPublicationRuntime("", nil).ExecuteFrontendPublication(context.Background(), FrontendPublicationRuntimeRequest{})
	if err == nil || !strings.Contains(err.Error(), "endpoint is required") {
		t.Fatalf("err=%v", err)
	}
}
