package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPFailbackRuntimePostsRequestAndDecodesResult(t *testing.T) {
	var got FailbackRuntimeRequest
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
		_ = json.NewEncoder(w).Encode(FailbackRuntimeResult{
			FailbackStarted:                   true,
			AuthorityEpochAdvanced:            true,
			SinglePrimaryAfterFailback:        true,
			PublishTargetSwappedAfterFailback: true,
			NoStorageMutation:                 true,
			NoCrossVolumeIdentityChange:       true,
			EvidenceRefs:                      []string{"failback-runtime.txt"},
		})
	}))
	defer server.Close()

	result, err := NewHTTPFailbackRuntime(server.URL, server.Client()).ExecuteFailback(context.Background(), FailbackRuntimeRequest{
		VolumeName:                   "demo",
		VolumeID:                     "pvc-demo",
		PVCName:                      "demo-pvc",
		ReplicaID:                    "r2",
		RuntimeEndpoint:              server.URL,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"target.txt"},
	})
	if err != nil {
		t.Fatalf("execute failback: %v", err)
	}
	if got.VolumeName != "demo" ||
		got.VolumeID != "pvc-demo" ||
		got.ReplicaID != "r2" ||
		!got.AckEligible ||
		!got.FrontendFencedBeforeFailback ||
		!got.NoCrossVolumeIdentityChange {
		t.Fatalf("request=%+v", got)
	}
	if !result.FailbackStarted ||
		!result.AuthorityEpochAdvanced ||
		!result.SinglePrimaryAfterFailback ||
		!result.PublishTargetSwappedAfterFailback ||
		!result.NoStorageMutation ||
		!result.NoCrossVolumeIdentityChange ||
		!authorityExecutorStringSliceContains(result.EvidenceRefs, "failback-runtime.txt") {
		t.Fatalf("result=%+v", result)
	}
}

func TestHTTPFailbackRuntimeReturnsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := NewHTTPFailbackRuntime(server.URL, server.Client()).ExecuteFailback(context.Background(), FailbackRuntimeRequest{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") || !strings.Contains(err.Error(), "runtime unavailable") {
		t.Fatalf("err=%v", err)
	}
}

func TestHTTPFailbackRuntimeRequiresEndpoint(t *testing.T) {
	_, err := NewHTTPFailbackRuntime("", nil).ExecuteFailback(context.Background(), FailbackRuntimeRequest{})
	if err == nil || !strings.Contains(err.Error(), "endpoint is required") {
		t.Fatalf("err=%v", err)
	}
}
