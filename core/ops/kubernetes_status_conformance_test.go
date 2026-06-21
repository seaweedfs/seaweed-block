package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC(t *testing.T) {
	api := newStatusConformanceAPIServer(t)
	defer api.Close()

	client := &KubernetesStatusClient{
		BaseURL:     api.URL,
		BearerToken: "operator-status-token",
		HTTPClient:  api.Client(),
	}
	observedAt := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
	if err := client.WriteClusterStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "sw-block",
	}, SwBlockClusterCRDStatus{
		ObservedAt:         observedAt,
		NodeCount:          1,
		VolumeCount:        1,
		BlockedVolumeCount: 1,
		Nodes: []SwBlockNodeCRDStatus{{
			Name:           "m02",
			KubernetesNode: "m02",
			InternalIP:     "192.168.1.184",
			Schedulable:    true,
			Ready:          false,
			Status:         ManagedVolumeStatusUnknown,
			ReasonCode:     ReasonNodeNotReady,
			Conditions: []ObservationCondition{{
				Type:     ConditionReady,
				Status:   "Unknown",
				Reason:   ReasonNodeNotReady,
				Severity: "warning",
			}},
			EvidenceRefs: []string{"nodes/m02.txt"},
		}},
		Conditions: []ObservationCondition{{
			Type:     ConditionBlocked,
			Status:   "True",
			Reason:   ReasonCleanupRequired,
			Severity: "warning",
		}},
		Cleanup: &SwBlockCleanupStatus{
			Status:            "failed",
			ISCSIResidueCount: 1,
			FailureCount:      1,
			ReasonCodes:       []string{"iscsi_node_records_present"},
			EvidenceRef:       "cleanup-summary.txt",
		},
		InstallDrift: &SwBlockInstallDrift{
			Status:          InstallDriftStatusMismatch,
			ReasonCode:      ReasonInstallDriftMismatch,
			CurrentImage:    "sw-block:old",
			DesiredImage:    "sw-block:new",
			CurrentCSIImage: "sw-block-csi:old",
			DesiredCSIImage: "sw-block-csi:new",
			EvidenceRef:     "install-drift-summary.txt",
		},
		SafeNextSteps: []SwBlockSafeNextStep{{
			Type:            ManagedVolumeActionVerifyCleanup,
			Mode:            ManagedVolumeActionModeScripted,
			Command:         "bash scripts/verify-helm-cleanup.sh",
			ReasonCode:      "iscsi_node_records_present",
			MutationAllowed: false,
			EvidenceRefs:    []string{"cleanup-summary.txt"},
		}},
		MutationAllowed:    false,
		AllowedActionModes: []string{ManagedVolumeActionModeReadOnly, ManagedVolumeActionModeDryRun},
		NonClaims:          []string{"no_finalizer_mutation"},
	}); err != nil {
		t.Fatalf("write cluster status: %v", err)
	}
	if err := client.WriteVolumeStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "pvc-a",
	}, SwBlockVolumeCRDStatus{
		VolumeID:   "pvc-a",
		PVCName:    "pvc-a",
		Status:     ManagedVolumeStatusBlocked,
		ReasonCode: "iscsi_node_records_present",
		ObservedAt: observedAt,
		Conditions: []ObservationCondition{
			{Type: ConditionReady, Status: "False", Reason: "iscsi_node_records_present", Severity: "warning"},
			{Type: ConditionCleanupRequired, Status: "True", Reason: "iscsi_node_records_present", Severity: "warning"},
		},
		ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
			ReplicaID:             "r1",
			State:                 ReturnedReplicaStateFenced,
			ReasonCode:            ReasonReturnedReplicaFrontendFenced,
			FrontendFenced:        true,
			FrontendPrimaryReady:  false,
			AckEligibilityKnown:   true,
			AckEligible:           false,
			DurableFrontierKnown:  true,
			DurableFrontierLSN:    52,
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
			EvidenceRefs:          []string{"returned-replica-summary.txt"},
		}},
		ExecutorPreflights: []SwBlockVolumeCRDExecutorPreflight{{
			ActionType:             ManagedVolumeActionReintegrateReturned,
			ReplicaID:              "r1",
			Decision:               ReturnedReplicaExecutorPreflightReady,
			Reason:                 ReturnedReplicaExecutorPreflightReasonSatisfied,
			Mode:                   ManagedVolumeActionModeDryRun,
			SideEffectClass:        ManagedVolumeSideEffectAuthorityMutating,
			OwnerExecutor:          "authority_recovery_executor",
			MutationAllowed:        false,
			FrontendFenced:         true,
			AckEligibilityKnown:    true,
			AckEligible:            false,
			DurableFrontierKnown:   true,
			DurableFrontierLSN:     52,
			RequiredFrontierKnown:  true,
			RequiredFrontierLSN:    52,
			EvidenceRequired:       "returned_replica_reintegration_evidence",
			EvidenceRefs:           []string{"returned-replica-summary.txt"},
			ForbiddenMutationClass: []string{"ack_eligibility"},
		}},
		DeleteSafety: &SwBlockVolumeCRDDeleteSafety{
			ActionType:     SwBlockVolumeDeleteActionReleaseFinalizer,
			Decision:       ManagedVolumeActionDecisionRejected,
			State:          DeleteSafetyStateBlocked,
			Reason:         "iscsi_node_records_present",
			EvidenceRefs:   []string{"cleanup-summary.txt"},
			SafeNextAction: ManagedVolumeActionVerifyCleanup,
		},
		AllowedActions: []SwBlockVolumeCRDAction{{
			Type:             ManagedVolumeActionVerifyCleanup,
			Mode:             ManagedVolumeActionModeScripted,
			SideEffectClass:  ManagedVolumeSideEffectObserve,
			OwnerExecutor:    "ops",
			Decision:         ManagedVolumeActionDecisionRejected,
			DecisionReason:   "iscsi_node_records_present",
			MutationAllowed:  false,
			EvidenceRequired: "cleanup-summary.txt",
			EvidenceRefs:     []string{"cleanup-summary.txt"},
		}, {
			Type:             ManagedVolumeActionReintegrateReturned,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectAuthorityMutating,
			OwnerExecutor:    "authority_recovery_executor",
			Decision:         ManagedVolumeActionDecisionAllowed,
			MutationAllowed:  false,
			Preconditions:    []string{"returned_replica_frontend_fenced", "durable_frontier_evidence"},
			InvariantRefs:    []string{"INV-RETURNED-REPLICA-FENCING-001", "INV-RETURNED-REPLICA-FRONTIER-001"},
			EvidenceRequired: "returned_replica_reintegration_evidence",
			EvidenceRefs:     []string{"returned-replica-summary.txt"},
		}},
		NonClaims:    []string{"no_automatic_cleanup_execution"},
		EvidenceRefs: []string{"cleanup-summary.txt"},
	}); err != nil {
		t.Fatalf("write volume status: %v", err)
	}
	event := OperatorKubernetesEvent{
		InvolvedObject: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  "kube-system",
			Name:       "pvc-a",
		},
		Type:       "Warning",
		Reason:     "iscsi_node_records_present",
		Message:    "delete-safety blocked by iSCSI residue",
		ObservedAt: observedAt,
	}
	if err := client.EmitEvent(context.Background(), event); err != nil {
		t.Fatalf("emit first event: %v", err)
	}
	if err := client.EmitEvent(context.Background(), event); err != nil {
		t.Fatalf("duplicate event should be idempotent: %v", err)
	}
	if got, want := api.statusPatchCount, 2; got != want {
		t.Fatalf("status patches=%d want %d", got, want)
	}
	if got, want := len(api.events), 1; got != want {
		t.Fatalf("stored events=%d want %d", got, want)
	}
}

func TestPhase40D1StatusConformanceRejectsSchemaAndRBACDrift(t *testing.T) {
	api := newStatusConformanceAPIServer(t)
	defer api.Close()

	cases := []struct {
		name string
		path string
		body map[string]any
		want int
	}{
		{
			name: "snake-case action misses required camelCase mutationAllowed",
			path: "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a/status",
			body: map[string]any{"status": map[string]any{
				"status": "blocked",
				"allowedActions": []any{map[string]any{
					"type":             ManagedVolumeActionVerifyCleanup,
					"mode":             ManagedVolumeActionModeScripted,
					"mutation_allowed": false,
				}},
			}},
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "unsupported condition type is rejected",
			path: "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockclusters/sw-block/status",
			body: map[string]any{"status": map[string]any{
				"nodeCount":          1,
				"volumeCount":        0,
				"readyVolumeCount":   0,
				"blockedVolumeCount": 0,
				"staleVolumeCount":   0,
				"mutationAllowed":    false,
				"nodes": []any{map[string]any{
					"name":        "m02",
					"schedulable": true,
					"ready":       false,
					"conditions": []any{map[string]any{
						"type":   "KubernetesNodeReady",
						"status": "False",
					}},
				}},
			}},
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "main resource patch is forbidden",
			path: "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a",
			body: map[string]any{"metadata": map[string]any{"finalizers": []any{"block.seaweedfs.com/swblockvolume-protection"}}},
			want: http.StatusForbidden,
		},
		{
			name: "finalizers endpoint is forbidden",
			path: "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a/finalizers",
			body: map[string]any{"metadata": map[string]any{"finalizers": []any{"block.seaweedfs.com/swblockvolume-protection"}}},
			want: http.StatusForbidden,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			req, err := http.NewRequest(http.MethodPatch, api.URL+tc.path, bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/merge-patch+json")
			resp, err := api.Client().Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status=%d want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

func TestPhase41D2LifecycleOwnerFinalizerBoundary(t *testing.T) {
	api := newStatusConformanceAPIServer(t)
	defer api.Close()

	finalizerPatch := map[string]any{"metadata": map[string]any{
		"finalizers": []any{"block.seaweedfs.com/swblockvolume-protection"},
	}}
	cases := []struct {
		name  string
		token string
		path  string
		body  map[string]any
		want  int
	}{
		{
			name:  "operator-status cannot patch finalizers through main object",
			token: "operator-status-token",
			path:  "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a",
			body:  finalizerPatch,
			want:  http.StatusForbidden,
		},
		{
			name:  "lifecycle-owner can patch only finalizers through main object",
			token: "lifecycle-owner-token",
			path:  "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a",
			body:  finalizerPatch,
			want:  http.StatusOK,
		},
		{
			name:  "lifecycle-owner still cannot use non-existent finalizers endpoint",
			token: "lifecycle-owner-token",
			path:  "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a/finalizers",
			body:  finalizerPatch,
			want:  http.StatusForbidden,
		},
		{
			name:  "lifecycle-owner cannot patch spec",
			token: "lifecycle-owner-token",
			path:  "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a",
			body:  map[string]any{"spec": map[string]any{"volumeID": "pvc-a"}},
			want:  http.StatusForbidden,
		},
		{
			name:  "lifecycle-owner cannot patch unrelated metadata",
			token: "lifecycle-owner-token",
			path:  "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/pvc-a",
			body:  map[string]any{"metadata": map[string]any{"labels": map[string]any{"changed": "true"}}},
			want:  http.StatusForbidden,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			req, err := http.NewRequest(http.MethodPatch, api.URL+tc.path, bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/merge-patch+json")
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			resp, err := api.Client().Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status=%d want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

type statusConformanceAPIServer struct {
	*httptest.Server
	clusterStatusSchema map[string]any
	volumeStatusSchema  map[string]any
	statusPatchCount    int
	events              map[string]map[string]any
}

func newStatusConformanceAPIServer(t *testing.T) *statusConformanceAPIServer {
	t.Helper()
	api := &statusConformanceAPIServer{
		clusterStatusSchema: crdStatusSchema(t, "charts/seaweed-block/crds/swblockclusters.block.seaweedfs.com.yaml"),
		volumeStatusSchema:  crdStatusSchema(t, "charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"),
		events:              map[string]map[string]any{},
	}
	api.Server = httptest.NewServer(http.HandlerFunc(api.handle))
	return api
}

func (api *statusConformanceAPIServer) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/status"):
		api.handleStatusPatch(w, r)
	case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/swblockvolumes/") && !strings.HasSuffix(r.URL.Path, "/finalizers"):
		api.handleLifecycleOwnerPatch(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/") && strings.HasSuffix(r.URL.Path, "/events"):
		api.handleEventCreate(w, r)
	default:
		http.Error(w, "operator-status RBAC forbids this request", http.StatusForbidden)
	}
}

func (api *statusConformanceAPIServer) handleLifecycleOwnerPatch(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer lifecycle-owner-token" {
		http.Error(w, "main resource patch forbidden for observer", http.StatusForbidden)
		return
	}
	if got := r.Header.Get("Content-Type"); got != "application/merge-patch+json" {
		http.Error(w, "resource patches must use merge-patch", http.StatusUnsupportedMediaType)
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !isFinalizerOnlyPatch(body) {
		http.Error(w, "lifecycle-owner may patch only metadata.finalizers", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"kind":"SwBlockVolume","metadata":{"name":"pvc-a"}}`))
}

func isFinalizerOnlyPatch(body map[string]any) bool {
	if len(body) != 1 {
		return false
	}
	metadata, ok := body["metadata"].(map[string]any)
	if !ok || len(metadata) != 1 {
		return false
	}
	finalizers, ok := metadata["finalizers"].([]any)
	if !ok {
		return false
	}
	for _, item := range finalizers {
		if _, ok := item.(string); !ok {
			return false
		}
	}
	return true
}

func (api *statusConformanceAPIServer) handleStatusPatch(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Content-Type"); got != "application/merge-patch+json" {
		http.Error(w, "status patches must use merge-patch", http.StatusUnsupportedMediaType)
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	status, ok := body["status"].(map[string]any)
	if !ok {
		http.Error(w, "missing status object", http.StatusUnprocessableEntity)
		return
	}
	var schema map[string]any
	switch {
	case strings.Contains(r.URL.Path, "/swblockclusters/"):
		schema = api.clusterStatusSchema
	case strings.Contains(r.URL.Path, "/swblockvolumes/"):
		schema = api.volumeStatusSchema
	default:
		http.Error(w, "unknown CRD resource", http.StatusNotFound)
		return
	}
	if err := validateOpenAPISubset(schema, status, "status"); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	api.statusPatchCount++
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"kind":"Status","status":"Success"}`))
}

func (api *statusConformanceAPIServer) handleEventCreate(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metadata, ok := body["metadata"].(map[string]any)
	if !ok {
		http.Error(w, "missing event metadata", http.StatusUnprocessableEntity)
		return
	}
	name, _ := metadata["name"].(string)
	if name == "" {
		http.Error(w, "missing event metadata.name", http.StatusUnprocessableEntity)
		return
	}
	if _, exists := api.events[name]; exists {
		http.Error(w, "AlreadyExists", http.StatusConflict)
		return
	}
	api.events[name] = body
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"kind":"Event"}`))
}

func crdStatusSchema(t *testing.T, repoPath string) map[string]any {
	t.Helper()
	doc := readYAMLMap(t, repoPath)
	spec := yamlMap(t, doc, "spec")
	version := yamlSlice(t, spec, "versions")[0].(map[string]any)
	schema := yamlMap(t, version, "schema")
	openAPI := yamlMap(t, schema, "openAPIV3Schema")
	properties := yamlMap(t, openAPI, "properties")
	return yamlMap(t, properties, "status")
}

func validateOpenAPISubset(schema map[string]any, value any, path string) error {
	if required, ok := schema["required"].([]any); ok {
		valueMap, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: required fields on non-object %T", path, value)
		}
		for _, item := range required {
			key, _ := item.(string)
			if _, exists := valueMap[key]; !exists {
				return fmt.Errorf("%s.%s: Required value", path, key)
			}
		}
	}
	if enum, ok := schema["enum"].([]any); ok {
		got, _ := value.(string)
		if got == "" {
			return fmt.Errorf("%s: enum value is %T, want string", path, value)
		}
		for _, item := range enum {
			if want, _ := item.(string); got == want {
				return nil
			}
		}
		return fmt.Errorf("%s: Unsupported value %q", path, got)
	}
	switch schema["type"] {
	case "object":
		valueMap, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: got %T, want object", path, value)
		}
		properties, _ := schema["properties"].(map[string]any)
		for key, child := range valueMap {
			if child == nil {
				continue
			}
			childSchema, ok := properties[key].(map[string]any)
			if !ok {
				continue
			}
			if err := validateOpenAPISubset(childSchema, child, path+"."+key); err != nil {
				return err
			}
		}
	case "array":
		values, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s: got %T, want array", path, value)
		}
		itemSchema, _ := schema["items"].(map[string]any)
		for i, item := range values {
			if err := validateOpenAPISubset(itemSchema, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s: got %T, want string", path, value)
		}
	case "integer":
		switch value.(type) {
		case float64, int, int64:
		default:
			return fmt.Errorf("%s: got %T, want integer", path, value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s: got %T, want boolean", path, value)
		}
	}
	return nil
}
