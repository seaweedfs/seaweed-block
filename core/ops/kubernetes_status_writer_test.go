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
		ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
			ActionType:               ManagedVolumeActionReintegrateReturned,
			ReplicaID:                "r1",
			Decision:                 ReturnedReplicaExecutorContractDisabled,
			Reason:                   ReturnedReplicaExecutorContractReasonExecutorDisabled,
			OwnerExecutor:            "authority_recovery_executor",
			ExecutionEnabled:         false,
			MutationAllowed:          false,
			PreflightDecision:        ReturnedReplicaExecutorPreflightReady,
			PreflightReason:          ReturnedReplicaExecutorPreflightReasonSatisfied,
			AllowedMutationClass:     []string{"ack_eligibility"},
			ForbiddenMutationClass:   []string{"frontend_publication", "rebuild_traffic", "failback"},
			TerminalEvidenceRequired: []string{"ack_eligibility_known", "ack_eligible_true", "frontend_fenced_after_execution", "primary_unchanged", "durable_frontier_covered", "no_cross_volume_identity_change"},
			EvidenceRefs:             []string{"returned-replica-summary.txt"},
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
	if err := client.WriteReplicaEligibilityStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "demo-pvc-r1",
	}, SwBlockReplicaEligibilityCRDStatus{
		ObservedAt:                         observedAt,
		ObservedGeneration:                 7,
		Executor:                           "authority_recovery_executor",
		ReasonCode:                         "ack_eligibility_recorded",
		AckEligibilityKnown:                true,
		AckEligible:                        true,
		FrontendFencedAfterExecution:       true,
		PrimaryUnchanged:                   true,
		DurableFrontierCovered:             true,
		NoCrossVolumeIdentityChange:        true,
		FrontendPublicationDecision:        AuthorityExecutorPublicationDecisionDisabled,
		FrontendPublicationReason:          AuthorityExecutorFrontendPublicationReasonDisabled,
		FrontendPublicationMutationAllowed: false,
		EvidenceGeneration:                 "executor-run-1",
		EvidenceRefs:                       []string{"returned-replica-summary.txt"},
		NonClaims:                          []string{"no_frontend_publication", "no_rebuild_traffic", "no_failback"},
	}); err != nil {
		t.Fatalf("write replica eligibility status: %v", err)
	}
	if err := client.WriteReplicaRebuildStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "demo-pvc-r1-rebuild",
	}, SwBlockReplicaRebuildCRDStatus{
		ObservedAt:                  observedAt,
		ObservedGeneration:          8,
		Executor:                    "authority_recovery_executor",
		State:                       "planned",
		ReasonCode:                  AuthorityExecutorReasonRebuildPlanned,
		FrontendFencedBeforeRebuild: true,
		PrimaryUnchanged:            true,
		DurableFrontierKnown:        true,
		DurableFrontierLSN:          51,
		RequiredFrontierKnown:       true,
		RequiredFrontierLSN:         52,
		DurableFrontierCaughtUp:     false,
		RebuildTrafficStarted:       false,
		PublicationDecision:         AuthorityExecutorPublicationDecisionBlocked,
		PublicationReason:           AuthorityExecutorPublicationReasonCaughtUpRequired,
		PublicationMutationAllowed:  false,
		NoFrontendPublication:       true,
		NoCrossVolumeIdentityChange: true,
		EvidenceGeneration:          "executor-run-2",
		EvidenceRefs:                []string{"returned-replica-summary.txt"},
		NonClaims:                   []string{"no_rebuild_data_movement", "no_frontend_publication", "no_failback"},
	}); err != nil {
		t.Fatalf("write replica rebuild status: %v", err)
	}
	if err := client.WriteFrontendPublicationStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "demo-pvc-r1-frontend-publication",
	}, SwBlockFrontendPublicationCRDStatus{
		ObservedAt:                  observedAt,
		ObservedGeneration:          9,
		Executor:                    "frontend-publication-executor",
		State:                       FrontendPublicationStateBlocked,
		ReasonCode:                  AuthorityExecutorFrontendPublicationReasonDisabled,
		PublicationMutationAllowed:  false,
		FrontendPublished:           false,
		FailbackStarted:             false,
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: true,
		EvidenceGeneration:          "executor-run-3",
		EvidenceRefs:                []string{"frontend-publication-summary.txt"},
		NonClaims:                   []string{"no_frontend_publication", "no_failback", "no_storage_mutation"},
	}); err != nil {
		t.Fatalf("write frontend publication status: %v", err)
	}
	if err := client.WriteReplicaFailbackStatus(context.Background(), OperatorObjectRef{
		Namespace: "kube-system",
		Name:      "demo-pvc-r1-failback",
	}, SwBlockReplicaFailbackCRDStatus{
		ObservedAt:                        observedAt,
		ObservedGeneration:                10,
		Executor:                          "failback-executor",
		State:                             FailbackStateBlocked,
		ReasonCode:                        AuthorityExecutorFailbackReasonDisabled,
		FailbackMutationAllowed:           false,
		FailbackStarted:                   false,
		AuthorityEpochAdvanced:            false,
		SinglePrimaryAfterFailback:        false,
		PublishTargetSwappedAfterFailback: false,
		NoCrossVolumeIdentityChange:       true,
		EvidenceGeneration:                "executor-run-4",
		EvidenceRefs:                      []string{"failback-summary.txt"},
		NonClaims:                         []string{"no_failback", "no_frontend_publication", "no_storage_mutation"},
	}); err != nil {
		t.Fatalf("write replica failback status: %v", err)
	}

	if len(requests) != 6 {
		t.Fatalf("requests=%d want 6: %+v", len(requests), requests)
	}
	wantPaths := []string{
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockclusters/sw-block/status",
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockvolumes/demo-pvc/status",
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockreplicaeligibilities/demo-pvc-r1/status",
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockreplicarebuilds/demo-pvc-r1-rebuild/status",
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockfrontendpublications/demo-pvc-r1-frontend-publication/status",
		"/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockreplicafailbacks/demo-pvc-r1-failback/status",
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
	preflight := volumeStatus["executorPreflights"].([]any)[0].(map[string]any)
	for _, want := range []string{"actionType", "replicaID", "sideEffectClass", "ownerExecutor", "mutationAllowed", "frontendFenced", "ackEligible", "durableFrontierKnown", "durableFrontierLsn", "requiredFrontierKnown", "requiredFrontierLsn", "evidenceRequired", "forbiddenMutationClass"} {
		if _, ok := preflight[want]; !ok {
			t.Fatalf("executor preflight missing camelCase %s: %+v", want, preflight)
		}
	}
	for _, forbidden := range []string{"action_type", "side_effect_class", "owner_executor", "mutation_allowed", "durable_frontier_lsn", "required_frontier_lsn"} {
		if _, ok := preflight[forbidden]; ok {
			t.Fatalf("executor preflight leaked snake_case %s: %+v", forbidden, preflight)
		}
	}
	executorContract := volumeStatus["executorContracts"].([]any)[0].(map[string]any)
	for _, want := range []string{"actionType", "replicaID", "ownerExecutor", "executionEnabled", "mutationAllowed", "preflightDecision", "allowedMutationClass", "forbiddenMutationClass", "terminalEvidenceRequired"} {
		if _, ok := executorContract[want]; !ok {
			t.Fatalf("executor contract missing camelCase %s: %+v", want, executorContract)
		}
	}
	for _, forbidden := range []string{"action_type", "owner_executor", "execution_enabled", "mutation_allowed", "preflight_decision", "allowed_mutation_class", "terminal_evidence_required"} {
		if _, ok := executorContract[forbidden]; ok {
			t.Fatalf("executor contract leaked snake_case %s: %+v", forbidden, executorContract)
		}
	}
	replicaEligibilityStatus := requests[2].Body["status"].(map[string]any)
	for _, want := range []string{
		"ackEligibilityKnown",
		"ackEligible",
		"frontendFencedAfterExecution",
		"primaryUnchanged",
		"durableFrontierCovered",
		"noCrossVolumeIdentityChange",
		"frontendPublicationDecision",
		"frontendPublicationReason",
		"frontendPublicationMutationAllowed",
		"evidenceGeneration",
	} {
		if _, ok := replicaEligibilityStatus[want]; !ok {
			t.Fatalf("replica eligibility status missing camelCase %s: %+v", want, replicaEligibilityStatus)
		}
	}
	for _, forbidden := range []string{"ack_eligible", "frontend_fenced_after_execution", "frontend_publication_decision", "frontend_publication_mutation_allowed", "spec"} {
		if _, ok := replicaEligibilityStatus[forbidden]; ok {
			t.Fatalf("replica eligibility status leaked %s: %+v", forbidden, replicaEligibilityStatus)
		}
	}
	if replicaEligibilityStatus["frontendPublicationDecision"] != AuthorityExecutorPublicationDecisionDisabled ||
		replicaEligibilityStatus["frontendPublicationReason"] != AuthorityExecutorFrontendPublicationReasonDisabled ||
		replicaEligibilityStatus["frontendPublicationMutationAllowed"] != false {
		t.Fatalf("frontend publication preflight=%+v", replicaEligibilityStatus)
	}
	replicaRebuildStatus := requests[3].Body["status"].(map[string]any)
	for _, want := range []string{
		"frontendFencedBeforeRebuild",
		"primaryUnchanged",
		"durableFrontierKnown",
		"durableFrontierLsn",
		"requiredFrontierKnown",
		"requiredFrontierLsn",
		"durableFrontierCaughtUp",
		"rebuildTrafficStarted",
		"publicationDecision",
		"publicationReason",
		"publicationMutationAllowed",
		"noFrontendPublication",
		"noCrossVolumeIdentityChange",
		"evidenceGeneration",
	} {
		if _, ok := replicaRebuildStatus[want]; !ok {
			t.Fatalf("replica rebuild status missing camelCase %s: %+v", want, replicaRebuildStatus)
		}
	}
	for _, forbidden := range []string{"frontend_fenced_before_rebuild", "durable_frontier_lsn", "rebuild_traffic_started", "publication_decision", "publication_mutation_allowed", "spec"} {
		if _, ok := replicaRebuildStatus[forbidden]; ok {
			t.Fatalf("replica rebuild status leaked %s: %+v", forbidden, replicaRebuildStatus)
		}
	}
	frontendPublicationStatus := requests[4].Body["status"].(map[string]any)
	for _, want := range []string{
		"publicationMutationAllowed",
		"frontendPublished",
		"failbackStarted",
		"noStorageMutation",
		"noCrossVolumeIdentityChange",
		"evidenceGeneration",
	} {
		if _, ok := frontendPublicationStatus[want]; !ok {
			t.Fatalf("frontend publication status missing camelCase %s: %+v", want, frontendPublicationStatus)
		}
	}
	for _, forbidden := range []string{"publication_mutation_allowed", "frontend_published", "failback_started", "no_storage_mutation", "spec"} {
		if _, ok := frontendPublicationStatus[forbidden]; ok {
			t.Fatalf("frontend publication status leaked %s: %+v", forbidden, frontendPublicationStatus)
		}
	}
	if frontendPublicationStatus["frontendPublished"] != false ||
		frontendPublicationStatus["failbackStarted"] != false ||
		frontendPublicationStatus["noStorageMutation"] != true {
		t.Fatalf("frontend publication status=%+v", frontendPublicationStatus)
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

func TestKubernetesStatusClientCreatesSwBlockReplicaRebuildWithoutStatus(t *testing.T) {
	var request recordedStatusPatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		request.Method = r.Method
		request.Path = r.URL.Path
		request.ContentType = r.Header.Get("Content-Type")
		request.Authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	err := (&KubernetesStatusClient{
		BaseURL:     server.URL,
		BearerToken: "target-owner-token",
		HTTPClient:  server.Client(),
	}).CreateSwBlockReplicaRebuild(context.Background(), "kube-system", SwBlockReplicaRebuildObject{
		Ref: OperatorObjectRef{
			Name: "demo-pvc-r2-rebuild",
		},
		Spec: SwBlockReplicaRebuildSpec{
			VolumeName:      "demo-pvc",
			VolumeID:        "pvc-demo",
			PVCName:         "demo-pvc",
			ReplicaID:       "r2",
			RuntimeEndpoint: "http://127.0.0.1:23260/rebuild/runtime",
			TargetDataAddr:  "127.0.0.1:19103",
			SessionID:       1001,
			Epoch:           7,
			EndpointVersion: 3,
			FromLSN:         52,
			FrontierHintLSN: 53,
			BasePinLSN:      60,
		},
	})
	if err != nil {
		t.Fatalf("create rebuild target: %v", err)
	}
	if request.Method != http.MethodPost {
		t.Fatalf("method=%s", request.Method)
	}
	if request.Path != "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockreplicarebuilds" {
		t.Fatalf("path=%s", request.Path)
	}
	if request.ContentType != "application/json" {
		t.Fatalf("content-type=%s", request.ContentType)
	}
	if request.Authorization != "Bearer target-owner-token" {
		t.Fatalf("authorization=%s", request.Authorization)
	}
	if request.Body["apiVersion"] != SwBlockVolumeAPIVersion || request.Body["kind"] != SwBlockReplicaRebuildKind {
		t.Fatalf("body identity=%+v", request.Body)
	}
	metadata := request.Body["metadata"].(map[string]any)
	if metadata["name"] != "demo-pvc-r2-rebuild" || metadata["namespace"] != "kube-system" {
		t.Fatalf("metadata=%+v", metadata)
	}
	spec := request.Body["spec"].(map[string]any)
	if spec["volumeName"] != "demo-pvc" ||
		spec["volumeID"] != "pvc-demo" ||
		spec["pvcName"] != "demo-pvc" ||
		spec["replicaID"] != "r2" ||
		spec["runtimeEndpoint"] != "http://127.0.0.1:23260/rebuild/runtime" ||
		spec["targetDataAddr"] != "127.0.0.1:19103" ||
		spec["sessionID"] != float64(1001) ||
		spec["epoch"] != float64(7) ||
		spec["endpointVersion"] != float64(3) ||
		spec["fromLsn"] != float64(52) ||
		spec["frontierHintLsn"] != float64(53) ||
		spec["basePinLsn"] != float64(60) {
		t.Fatalf("spec=%+v", spec)
	}
	for _, forbidden := range []string{"runtime_endpoint", "target_data_addr", "session_id", "endpoint_version", "from_lsn", "frontier_hint_lsn", "base_pin_lsn"} {
		if _, ok := spec[forbidden]; ok {
			t.Fatalf("spec leaked %s: %+v", forbidden, spec)
		}
	}
	if _, ok := request.Body["status"]; ok {
		t.Fatalf("target create must not include status: %+v", request.Body)
	}
}

func TestKubernetesStatusClientCreatesSwBlockFrontendPublicationWithoutStatus(t *testing.T) {
	var request recordedStatusPatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		request.Method = r.Method
		request.Path = r.URL.Path
		request.ContentType = r.Header.Get("Content-Type")
		request.Authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	err := (&KubernetesStatusClient{
		BaseURL:     server.URL,
		BearerToken: "frontend-target-owner-token",
		HTTPClient:  server.Client(),
	}).CreateSwBlockFrontendPublication(context.Background(), "kube-system", SwBlockFrontendPublicationObject{
		Ref: OperatorObjectRef{
			Name: "demo-pvc-r2-frontend-publication",
		},
		Spec: SwBlockFrontendPublicationSpec{
			VolumeName:                         "demo-pvc",
			VolumeID:                           "pvc-demo",
			PVCName:                            "demo-pvc",
			ReplicaID:                          "r2",
			SourceEligibilityName:              "demo-pvc-r2-ack",
			AckEligibilityKnown:                true,
			AckEligible:                        true,
			FrontendFencedAfterExecution:       true,
			PrimaryUnchanged:                   true,
			DurableFrontierCovered:             true,
			NoCrossVolumeIdentityChange:        true,
			FrontendPublicationDecision:        AuthorityExecutorPublicationDecisionDisabled,
			FrontendPublicationReason:          AuthorityExecutorFrontendPublicationReasonDisabled,
			FrontendPublicationMutationAllowed: false,
		},
	})
	if err != nil {
		t.Fatalf("create frontend publication target: %v", err)
	}
	if request.Method != http.MethodPost {
		t.Fatalf("method=%s", request.Method)
	}
	if request.Path != "/apis/block.seaweedfs.com/v1alpha1/namespaces/kube-system/swblockfrontendpublications" {
		t.Fatalf("path=%s", request.Path)
	}
	if request.ContentType != "application/json" {
		t.Fatalf("content-type=%s", request.ContentType)
	}
	if request.Authorization != "Bearer frontend-target-owner-token" {
		t.Fatalf("authorization=%s", request.Authorization)
	}
	if request.Body["apiVersion"] != SwBlockVolumeAPIVersion || request.Body["kind"] != SwBlockFrontendPublicationKind {
		t.Fatalf("body identity=%+v", request.Body)
	}
	metadata := request.Body["metadata"].(map[string]any)
	if metadata["name"] != "demo-pvc-r2-frontend-publication" || metadata["namespace"] != "kube-system" {
		t.Fatalf("metadata=%+v", metadata)
	}
	spec := request.Body["spec"].(map[string]any)
	for _, want := range []string{
		"volumeName",
		"volumeID",
		"pvcName",
		"replicaID",
		"sourceEligibilityName",
		"ackEligibilityKnown",
		"ackEligible",
		"frontendFencedAfterExecution",
		"primaryUnchanged",
		"durableFrontierCovered",
		"noCrossVolumeIdentityChange",
		"frontendPublicationDecision",
		"frontendPublicationReason",
		"frontendPublicationMutationAllowed",
	} {
		if _, ok := spec[want]; !ok {
			t.Fatalf("spec missing %s: %+v", want, spec)
		}
	}
	for _, forbidden := range []string{"source_eligibility_name", "ack_eligible", "frontend_fenced_after_execution", "frontend_publication_decision", "frontend_publication_mutation_allowed"} {
		if _, ok := spec[forbidden]; ok {
			t.Fatalf("spec leaked %s: %+v", forbidden, spec)
		}
	}
	if spec["frontendPublicationDecision"] != AuthorityExecutorPublicationDecisionDisabled ||
		spec["frontendPublicationReason"] != AuthorityExecutorFrontendPublicationReasonDisabled ||
		spec["frontendPublicationMutationAllowed"] != false {
		t.Fatalf("frontend publication preflight spec=%+v", spec)
	}
	if _, ok := request.Body["status"]; ok {
		t.Fatalf("target create must not include status: %+v", request.Body)
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
