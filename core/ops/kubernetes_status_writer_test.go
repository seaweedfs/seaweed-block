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
		DeleteSafety: &SwBlockVolumeCRDDeleteSafety{
			ActionType:              SwBlockVolumeDeleteActionReleaseFinalizer,
			Decision:                ManagedVolumeActionDecisionAllowed,
			State:                   DeleteSafetyStateReleasable,
			Reason:                  ReasonDeleteFinalizerReleasable,
			FinalizerReleaseAllowed: true,
			EvidenceRefs:            []string{"cleanup-summary.txt"},
		},
		AllowedActions: []SwBlockVolumeCRDAction{{
			Type:             "observe.collect_bundle",
			Mode:             "read_only",
			SideEffectClass:  "none",
			OwnerExecutor:    "ops",
			Decision:         ManagedVolumeActionDecisionAllowed,
			DecisionReason:   "",
			MutationAllowed:  false,
			EvidenceRequired: "projection_inputs_or_bundle",
		}},
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
	volumeStatus := requests[1].Body["status"].(map[string]any)
	actions := volumeStatus["allowedActions"].([]any)
	action := actions[0].(map[string]any)
	if _, ok := action["mutationAllowed"]; !ok {
		t.Fatalf("volume action missing camelCase mutationAllowed: %+v", action)
	}
	if _, ok := action["decision"]; !ok {
		t.Fatalf("volume action missing decision: %+v", action)
	}
	if _, ok := action["evidenceRequired"]; !ok {
		t.Fatalf("volume action missing camelCase evidenceRequired: %+v", action)
	}
	if _, ok := action["mutation_allowed"]; ok {
		t.Fatalf("volume action leaked snake_case mutation_allowed: %+v", action)
	}
	if _, ok := action["evidence_required"]; ok {
		t.Fatalf("volume action leaked snake_case evidence_required: %+v", action)
	}
	deleteSafety := volumeStatus["deleteSafety"].(map[string]any)
	if _, ok := deleteSafety["finalizerReleaseAllowed"]; !ok {
		t.Fatalf("deleteSafety missing camelCase finalizerReleaseAllowed: %+v", deleteSafety)
	}
	if _, ok := deleteSafety["actionType"]; !ok {
		t.Fatalf("deleteSafety missing camelCase actionType: %+v", deleteSafety)
	}
	for _, forbidden := range []string{"finalizer_release_allowed", "action_type", "safe_next_action"} {
		if _, ok := deleteSafety[forbidden]; ok {
			t.Fatalf("deleteSafety leaked snake_case %s: %+v", forbidden, deleteSafety)
		}
	}
}

func TestKubernetesStatusClientCreatesCoreEvents(t *testing.T) {
	var eventBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/namespaces/kube-system/events" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer event-token" {
			t.Fatalf("authorization=%s", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&eventBody); err != nil {
			t.Fatalf("decode event body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	err := (&KubernetesStatusClient{
		BaseURL:     server.URL,
		BearerToken: "event-token",
		HTTPClient:  server.Client(),
	}).EmitEvent(context.Background(), OperatorKubernetesEvent{
		InvolvedObject: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  "kube-system",
			Name:       "blocked-pvc",
		},
		Type:       "Warning",
		Reason:     ReasonCSINodeImagePullFailed,
		Message:    "CSI node image pull failed",
		ObservedAt: time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("emit event: %v", err)
	}
	if eventBody["kind"] != "Event" || eventBody["type"] != "Warning" || eventBody["reason"] != ReasonCSINodeImagePullFailed {
		t.Fatalf("event body=%+v", eventBody)
	}
	involved := eventBody["involvedObject"].(map[string]any)
	if involved["kind"] != SwBlockVolumeKind || involved["name"] != "blocked-pvc" {
		t.Fatalf("involvedObject=%+v", involved)
	}
	metadata := eventBody["metadata"].(map[string]any)
	if metadata["name"] != "blocked-pvc-warning-csi-node-image-pull-failed" {
		t.Fatalf("event name=%s", metadata["name"])
	}
}

func TestKubernetesStatusClientEnsuresAndReleasesVolumeFinalizer(t *testing.T) {
	var patchBodies []map[string]any
	currentFinalizers := []string{"example.com/keep"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/demo-pvc":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"finalizers": currentFinalizers},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/demo-pvc":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode patch: %v", err)
			}
			patchBodies = append(patchBodies, body)
			metadata := body["metadata"].(map[string]any)
			rawFinalizers := metadata["finalizers"].([]any)
			currentFinalizers = nil
			for _, value := range rawFinalizers {
				currentFinalizers = append(currentFinalizers, value.(string))
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := &KubernetesStatusClient{BaseURL: server.URL, HTTPClient: server.Client()}
	ref := OperatorObjectRef{Namespace: "kube-system", Name: "demo-pvc"}
	patched, err := client.EnsureVolumeFinalizer(context.Background(), ref, SwBlockVolumeFinalizerName)
	if err != nil || !patched {
		t.Fatalf("ensure patched=%t err=%v", patched, err)
	}
	if len(currentFinalizers) != 2 ||
		currentFinalizers[0] != "example.com/keep" ||
		currentFinalizers[1] != SwBlockVolumeFinalizerName {
		t.Fatalf("finalizers after ensure=%+v", currentFinalizers)
	}
	patched, err = client.EnsureVolumeFinalizer(context.Background(), ref, SwBlockVolumeFinalizerName)
	if err != nil || patched {
		t.Fatalf("second ensure patched=%t err=%v", patched, err)
	}
	patched, err = client.ReleaseVolumeFinalizer(context.Background(), ref, SwBlockVolumeFinalizerName)
	if err != nil || !patched {
		t.Fatalf("release patched=%t err=%v", patched, err)
	}
	if len(currentFinalizers) != 1 || currentFinalizers[0] != "example.com/keep" {
		t.Fatalf("finalizers after release=%+v", currentFinalizers)
	}
	if len(patchBodies) != 2 {
		t.Fatalf("patch count=%d bodies=%+v", len(patchBodies), patchBodies)
	}
	for _, body := range patchBodies {
		if _, ok := body["status"]; ok {
			t.Fatalf("finalizer patch must not patch status: %+v", body)
		}
		if _, ok := body["spec"]; ok {
			t.Fatalf("finalizer patch must not patch spec: %+v", body)
		}
	}
}

func TestKubernetesStatusClientTreatsPersistentEventAsIdempotentSuccess(t *testing.T) {
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode event body: %v", err)
		}
		name := body["metadata"].(map[string]any)["name"].(string)
		if seen[name] {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"reason":"AlreadyExists"}`))
			return
		}
		seen[name] = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := &KubernetesStatusClient{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}
	event := OperatorKubernetesEvent{
		InvolvedObject: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  "kube-system",
			Name:       "unknown",
		},
		Type:       "Warning",
		Reason:     ReasonCSINodeImagePullFailed,
		Message:    "managed volume is blocked",
		ObservedAt: time.Date(2026, 6, 4, 1, 0, 0, 0, time.UTC),
	}
	if err := client.EmitEvent(context.Background(), event); err != nil {
		t.Fatalf("first event: %v", err)
	}
	event.ObservedAt = event.ObservedAt.Add(time.Minute)
	if err := client.EmitEvent(context.Background(), event); err != nil {
		t.Fatalf("persistent event must be idempotent success: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("seen events=%+v", seen)
	}
}

func TestKubernetesEventNameSeparatesTypeAndReason(t *testing.T) {
	base := OperatorKubernetesEvent{
		InvolvedObject: OperatorObjectRef{Name: "demo-pvc"},
		Reason:         ReasonFirstVolumeVerified,
	}
	normal := base
	normal.Type = "Normal"
	warning := base
	warning.Type = "Warning"
	if kubernetesEventName(normal) == kubernetesEventName(warning) {
		t.Fatalf("event names must separate type: normal=%s warning=%s", kubernetesEventName(normal), kubernetesEventName(warning))
	}
	if got := kubernetesEventName(normal); strings.Contains(got, ".") {
		t.Fatalf("event name must be stable and not timestamp-suffixed: %s", got)
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
