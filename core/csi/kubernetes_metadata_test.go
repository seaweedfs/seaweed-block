package csi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPhase44SwBlockVolumeRegistrarCreatesIdentityObject(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s want POST", r.Method)
		}
		if r.URL.Path != "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("authorization=%s", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	registrar := &InClusterSwBlockVolumeRegistrar{
		client:    server.Client(),
		host:      server.URL,
		token:     "token",
		namespace: "kube-system",
	}
	if err := registrar.EnsureVolumeObject(context.Background(), VolumeSpec{
		VolumeID:     "pvc-1234",
		PVCName:      "Demo_PVC",
		StorageClass: "sw-block-dynamic",
	}); err != nil {
		t.Fatalf("ensure object: %v", err)
	}
	if got["apiVersion"] != "block.seaweedfs.com/v1alpha1" || got["kind"] != "SwBlockVolume" {
		t.Fatalf("type meta=%+v", got)
	}
	metadata := got["metadata"].(map[string]any)
	if metadata["name"] != "demo-pvc" || metadata["namespace"] != "kube-system" {
		t.Fatalf("metadata=%+v", metadata)
	}
	spec := got["spec"].(map[string]any)
	if spec["pvcName"] != "Demo_PVC" || spec["storageClass"] != "sw-block-dynamic" {
		t.Fatalf("spec=%+v", spec)
	}
	if _, ok := got["status"]; ok {
		t.Fatalf("registrar must not write status: %+v", got)
	}
}

func TestPhase44SwBlockVolumeRegistrarPatchesSpecOnConflict(t *testing.T) {
	var calls []string
	var patch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch len(calls) {
		case 1:
			if r.Method != http.MethodPost {
				t.Fatalf("first method=%s want POST", r.Method)
			}
			w.WriteHeader(http.StatusConflict)
		case 2:
			if r.Method != http.MethodPatch {
				t.Fatalf("second method=%s want PATCH", r.Method)
			}
			if r.URL.Path != "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/demo-pvc" {
				t.Fatalf("patch path=%s", r.URL.Path)
			}
			if r.Header.Get("Content-Type") != "application/merge-patch+json" {
				t.Fatalf("patch content-type=%s", r.Header.Get("Content-Type"))
			}
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
				t.Fatalf("decode patch: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected extra call %d", len(calls))
		}
	}))
	defer server.Close()

	registrar := &InClusterSwBlockVolumeRegistrar{
		client:    server.Client(),
		host:      server.URL,
		namespace: "kube-system",
	}
	if err := registrar.EnsureVolumeObject(context.Background(), VolumeSpec{
		VolumeID: "pvc-1234",
		PVCName:  "demo-pvc",
	}); err != nil {
		t.Fatalf("ensure object: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls=%v", calls)
	}
	if _, ok := patch["status"]; ok {
		t.Fatalf("patch must not include status: %+v", patch)
	}
	spec := patch["spec"].(map[string]any)
	if spec["pvcName"] != "demo-pvc" {
		t.Fatalf("patch spec=%+v", spec)
	}
}
