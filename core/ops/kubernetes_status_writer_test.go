package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestKubernetesStatusClientPatchesOnlyStatusSubresources(t *testing.T) {
	var requests []recordedStatusPatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		requests = append(requests, recordedStatusPatch{
			Method:        r.Method,
			Path:          r.URL.Path,
			ContentType:   r.Header.Get("Content-Type"),
			Authorization: r.Header.Get("Authorization"),
			Body:          body,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := &KubernetesStatusClient{
		BaseURL:     server.URL,
		BearerToken: "test-token",
		HTTPClient:  server.Client(),
	}
	observedAt := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	if err := client.WriteClusterStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "sw-block",
	}, SwBlockClusterCRDStatus{
		ObservedAt:         observedAt,
		VolumeCount:        1,
		ReadyVolumeCount:   1,
		MutationAllowed:    false,
		AllowedActionModes: []string{"read_only", "dry_run"},
	}); err != nil {
		t.Fatalf("write cluster status: %v", err)
	}
	if err := client.WriteVolumeStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "demo-pvc",
	}, SwBlockVolumeCRDStatus{
		VolumeID:   "pvc-123",
		PVCName:    "demo-pvc",
		Status:     ManagedVolumeStatusReady,
		ReasonCode: ReasonFirstVolumeVerified,
		ObservedAt: observedAt,
	}); err != nil {
		t.Fatalf("write volume status: %v", err)
	}

	if len(requests) != 2 {
		t.Fatalf("requests=%d want 2: %+v", len(requests), requests)
	}
	wantPaths := []string{
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockclusters/sw-block/status",
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/demo-pvc/status",
	}
	for i, req := range requests {
		if req.Method != http.MethodPatch {
			t.Fatalf("request %d method=%s want PATCH", i, req.Method)
		}
		if req.Path != wantPaths[i] {
			t.Fatalf("request %d path=%s want %s", i, req.Path, wantPaths[i])
		}
		if req.ContentType != "application/merge-patch+json" {
			t.Fatalf("request %d content-type=%s", i, req.ContentType)
		}
		if req.Authorization != "Bearer test-token" {
			t.Fatalf("request %d authorization=%s", i, req.Authorization)
		}
		if _, ok := req.Body["status"]; !ok {
			t.Fatalf("request %d missing status patch: %+v", i, req.Body)
		}
		if _, ok := req.Body["spec"]; ok {
			t.Fatalf("request %d must not patch spec: %+v", i, req.Body)
		}
	}
}

func TestKubernetesStatusClientReturnsHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing swblockvolume"))
	}))
	defer server.Close()

	err := (&KubernetesStatusClient{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}).WriteVolumeStatus(context.Background(), OperatorObjectRef{
		Namespace: "default",
		Name:      "missing",
	}, SwBlockVolumeCRDStatus{Status: ManagedVolumeStatusReady})
	if err == nil || !strings.Contains(err.Error(), "http 404") || !strings.Contains(err.Error(), "missing swblockvolume") {
		t.Fatalf("err=%v", err)
	}
}

type recordedStatusPatch struct {
	Method        string
	Path          string
	ContentType   string
	Authorization string
	Body          map[string]any
}
