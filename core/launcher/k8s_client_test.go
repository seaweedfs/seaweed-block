package launcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestKubernetesDeploymentClient_ListApplyDelete(t *testing.T) {
	var seenPatch bool
	var seenDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-123" {
			t.Fatalf("authorization=%q", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/default/deployments":
			if got := r.URL.Query().Get("labelSelector"); got != "app=sw-blockvolume" {
				t.Fatalf("labelSelector=%q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"metadata": map[string]any{
						"name":      "sw-blockvolume-pvc-a-r1",
						"namespace": "default",
						"labels": map[string]string{
							LabelApp:     AppBlockVolume,
							LabelVolume:  "pvc-a",
							LabelReplica: "r1",
						},
					},
					"spec": map[string]any{"replicas": 0},
				}},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/apps/v1/namespaces/default/deployments/sw-blockvolume-pvc-a-r1":
			seenPatch = true
			if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/apply-patch+yaml") {
				t.Fatalf("content-type=%q", got)
			}
			if got := r.URL.Query().Get("fieldManager"); got != "sw-block-launcher" {
				t.Fatalf("fieldManager=%q", got)
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && r.URL.Path == "/apis/apps/v1/namespaces/default/deployments/sw-blockvolume-pvc-old-r1":
			seenDelete = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client := NewKubernetesDeploymentClient(KubernetesDeploymentClientConfig{
		BaseURL: server.URL,
		Token:   "token-123",
	})
	existing, err := client.ListBlockVolumeDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("ListBlockVolumeDeployments: %v", err)
	}
	if len(existing) != 1 || existing[0].Name != "sw-blockvolume-pvc-a-r1" {
		t.Fatalf("existing=%+v", existing)
	}
	if existing[0].SpecReplicas == nil || *existing[0].SpecReplicas != 0 {
		t.Fatalf("existing replicas=%v want 0", existing[0].SpecReplicas)
	}
	manifest := RenderedManifest{
		Name: "sw-blockvolume-pvc-a-r1",
		YAML: []byte(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sw-blockvolume-pvc-a-r1
  namespace: default
  labels:
    app: sw-blockvolume
    sw-block.seaweedfs.com/volume: pvc-a
    sw-block.seaweedfs.com/replica: r1
`),
	}
	if err := client.ApplyDeployment(context.Background(), manifest); err != nil {
		t.Fatalf("ApplyDeployment: %v", err)
	}
	if err := client.DeleteDeployment(context.Background(), DeploymentIdentity{
		Namespace: "default",
		Name:      "sw-blockvolume-pvc-old-r1",
	}); err != nil {
		t.Fatalf("DeleteDeployment: %v", err)
	}
	if !seenPatch || !seenDelete {
		t.Fatalf("seenPatch=%t seenDelete=%t", seenPatch, seenDelete)
	}
}

func TestKubernetesDeploymentClient_DeleteIgnoresNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client := NewKubernetesDeploymentClient(KubernetesDeploymentClientConfig{BaseURL: server.URL})
	if err := client.DeleteDeployment(context.Background(), DeploymentIdentity{Namespace: "default", Name: "missing"}); err != nil {
		t.Fatalf("DeleteDeployment: %v", err)
	}
}
