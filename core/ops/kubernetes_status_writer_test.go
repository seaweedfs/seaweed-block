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
		ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
			ReplicaID:             "r1",
			State:                 ReturnedReplicaStateFenced,
			ReasonCode:            ReasonReturnedReplicaFrontendFenced,
			FrontendFenced:        true,
			FrontendPrimaryReady:  false,
			AckEligible:           false,
			DurableFrontierKnown:  true,
			DurableFrontierLSN:    52,
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
			EvidenceRefs:          []string{"returned-replica-summary.txt"},
		}},
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
	returned := volumeStatus["replicaReintegrations"].([]any)[0].(map[string]any)
	for _, want := range []string{"replicaID", "frontendFenced", "frontendPrimaryReady", "ackEligible", "durableFrontierKnown", "durableFrontierLsn", "requiredFrontierKnown", "requiredFrontierLsn"} {
		if _, ok := returned[want]; !ok {
			t.Fatalf("returned replica missing camelCase %s: %+v", want, returned)
		}
	}
	for _, forbidden := range []string{"replica_id", "frontend_primary_ready", "ack_eligible", "durable_frontier_lsn"} {
		if _, ok := returned[forbidden]; ok {
			t.Fatalf("returned replica leaked snake_case %s: %+v", forbidden, returned)
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

func TestKubernetesStatusClientListsSwBlockVolumesForLifecycleOwner(t *testing.T) {
	deletingAt := "2026-06-15T01:02:03Z"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method=%s want GET", r.Method)
		}
		if r.URL.Path != "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
		  "items": [
		    {
		      "metadata":{"name":"a","namespace":"kube-system","finalizers":["example.com/foreign"]},
		      "spec":{"pvcName":"pvc-a","storageClass":"seaweed-block"},
		      "status":{"status":"ready","deleteSafety":{"state":"releasable","decision":"allowed","finalizerReleaseAllowed":true}}
		    },
		    {"metadata":{"name":"b","deletionTimestamp":"` + deletingAt + `"}}
		  ]
		}`))
	}))
	defer server.Close()

	volumes, err := (&KubernetesStatusClient{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}).ListSwBlockVolumes(context.Background(), "kube-system")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(volumes) != 2 {
		t.Fatalf("volumes=%+v", volumes)
	}
	if volumes[0].Ref.Name != "a" || volumes[0].Ref.Namespace != "kube-system" ||
		!stringSliceContains(volumes[0].Finalizers, "example.com/foreign") {
		t.Fatalf("volume a=%+v", volumes[0])
	}
	if volumes[0].Spec.PVCName != "pvc-a" || volumes[0].Spec.StorageClass != "seaweed-block" {
		t.Fatalf("volume a spec=%+v", volumes[0].Spec)
	}
	if volumes[0].Status.DeleteSafety == nil || !volumes[0].Status.DeleteSafety.FinalizerReleaseAllowed {
		t.Fatalf("volume a status=%+v", volumes[0].Status)
	}
	if volumes[1].Ref.Name != "b" || volumes[1].Ref.Namespace != "kube-system" || volumes[1].DeletionTimestamp == nil {
		t.Fatalf("volume b=%+v", volumes[1])
	}
}

func TestKubernetesStatusClientPatchesOnlySwBlockVolumeFinalizers(t *testing.T) {
	var request recordedStatusPatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		request.Method = r.Method
		request.Path = r.URL.Path
		request.ContentType = r.Header.Get("Content-Type")
		request.Authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	err := (&KubernetesStatusClient{
		BaseURL:     server.URL,
		BearerToken: "owner-token",
		HTTPClient:  server.Client(),
	}).PatchSwBlockVolumeFinalizers(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "demo",
	}, []string{"example.com/foreign", SwBlockVolumeFinalizerName})
	if err != nil {
		t.Fatalf("patch finalizers: %v", err)
	}
	if request.Method != http.MethodPatch {
		t.Fatalf("method=%s", request.Method)
	}
	if request.Path != "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/demo" {
		t.Fatalf("path=%s", request.Path)
	}
	if request.ContentType != "application/merge-patch+json" {
		t.Fatalf("content-type=%s", request.ContentType)
	}
	if request.Authorization != "Bearer owner-token" {
		t.Fatalf("authorization=%s", request.Authorization)
	}
	metadata := request.Body["metadata"].(map[string]any)
	if _, ok := request.Body["spec"]; ok {
		t.Fatalf("finalizer patch must not include spec: %+v", request.Body)
	}
	if _, ok := request.Body["status"]; ok {
		t.Fatalf("finalizer patch must not include status: %+v", request.Body)
	}
	if len(request.Body) != 1 || len(metadata) != 1 {
		t.Fatalf("finalizer patch must contain only metadata.finalizers: %+v", request.Body)
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
