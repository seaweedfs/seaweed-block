package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPAuthorityRebuildRuntimePostsRequestAndDecodesResult(t *testing.T) {
	var got AuthorityRebuildRuntimeRequest
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
		_ = json.NewEncoder(w).Encode(AuthorityRebuildRuntimeResult{
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			EvidenceRefs:         []string{"http-runtime.txt"},
		})
	}))
	defer server.Close()

	result, err := NewHTTPAuthorityRebuildRuntime(server.URL, server.Client()).ExecuteRebuild(context.Background(), AuthorityRebuildRuntimeRequest{
		VolumeName:            "rebuild",
		VolumeID:              "pvc-rebuild",
		PVCName:               "rebuild-pvc",
		ReplicaID:             "r1",
		DurableFrontierKnown:  true,
		DurableFrontierLSN:    51,
		RequiredFrontierKnown: true,
		RequiredFrontierLSN:   52,
		FrontendFenced:        true,
		NoFrontendPublication: true,
		NoCrossVolumeMutation: true,
		EvidenceRefs:          []string{"contract.txt"},
	})
	if err != nil {
		t.Fatalf("execute rebuild: %v", err)
	}
	if got.VolumeName != "rebuild" ||
		got.VolumeID != "pvc-rebuild" ||
		got.ReplicaID != "r1" ||
		!got.NoFrontendPublication ||
		!got.NoCrossVolumeMutation {
		t.Fatalf("request=%+v", got)
	}
	if !result.DurableFrontierKnown || result.DurableFrontierLSN != 52 || !authorityExecutorStringSliceContains(result.EvidenceRefs, "http-runtime.txt") {
		t.Fatalf("result=%+v", result)
	}
}

func TestHTTPAuthorityRebuildRuntimeReturnsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := NewHTTPAuthorityRebuildRuntime(server.URL, server.Client()).ExecuteRebuild(context.Background(), AuthorityRebuildRuntimeRequest{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") || !strings.Contains(err.Error(), "runtime unavailable") {
		t.Fatalf("err=%v", err)
	}
}

func TestHTTPAuthorityRebuildRuntimeRequiresEndpoint(t *testing.T) {
	_, err := NewHTTPAuthorityRebuildRuntime("", nil).ExecuteRebuild(context.Background(), AuthorityRebuildRuntimeRequest{})
	if err == nil || !strings.Contains(err.Error(), "endpoint is required") {
		t.Fatalf("err=%v", err)
	}
}
