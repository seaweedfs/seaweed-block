package ops

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAuthorityExecutorReconcilerObservesDisabledContractsWithoutMutation(t *testing.T) {
	client := &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "returned"},
		Status: SwBlockVolumeCRDStatus{ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
			ActionType:               ManagedVolumeActionReintegrateReturned,
			ReplicaID:                "r1",
			Decision:                 ReturnedReplicaExecutorContractDisabled,
			Reason:                   ReturnedReplicaExecutorContractReasonExecutorDisabled,
			OwnerExecutor:            "authority_recovery_executor",
			ExecutionEnabled:         false,
			MutationAllowed:          false,
			AllowedMutationClass:     []string{"ack_eligibility"},
			ForbiddenMutationClass:   []string{"frontend_publication", "rebuild_traffic", "failback"},
			TerminalEvidenceRequired: []string{"ack_eligibility_known", "frontend_fenced_after_execution"},
		}}},
	}}}

	result, err := (AuthorityExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.VolumeCount != 1 ||
		result.ContractCount != 1 ||
		result.DisabledContractCount != 1 ||
		result.TerminalEvidenceRequiredCount != 1 ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerFailsClosedOnExecutionEnabledContract(t *testing.T) {
	client := &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "unsafe"},
		Status: SwBlockVolumeCRDStatus{ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
			ActionType:       ManagedVolumeActionReintegrateReturned,
			Decision:         ReturnedReplicaExecutorContractDisabled,
			Reason:           ReturnedReplicaExecutorContractReasonExecutorDisabled,
			OwnerExecutor:    "authority_recovery_executor",
			ExecutionEnabled: true,
			MutationAllowed:  false,
		}}},
	}}}

	result, err := (AuthorityExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected execution-enabled contract to fail closed, result=%+v", result)
	}
	if result.UnsafeExecutionContractCount != 1 || result.MutationAttemptCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerRejectsUnsupportedMutationClass(t *testing.T) {
	result, err := (AuthorityExecutorReconciler{
		Client:               &fakeAuthorityExecutorClient{},
		AllowedMutationClass: "frontend_publication",
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected unsupported mutation class to fail closed, result=%+v", result)
	}
	if result.BlockedReason != "unsupported_mutation_class" || result.MutationAttemptCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerIgnoresDisabledRebuildContractWithoutMutation(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes: []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		eligibilities: []SwBlockReplicaEligibilityObject{{
			Ref: OperatorObjectRef{Namespace: "kube-system", Name: "rebuild-r1"},
			Spec: SwBlockReplicaEligibilitySpec{
				VolumeName: "rebuild",
				VolumeID:   "pvc-rebuild",
				PVCName:    "rebuild-pvc",
				ReplicaID:  "r1",
			},
		}},
	}
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.VolumeCount != 1 ||
		result.ContractCount != 1 ||
		result.DisabledContractCount != 1 ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 ||
		len(client.writes) != 0 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
}

func TestAuthorityExecutorReconcilerFailsClosedOnEnabledRebuildContract(t *testing.T) {
	client := &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{authorityExecutorRebuildVolume(true, false)}}
	result, err := (AuthorityExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected enabled rebuild contract to fail closed, result=%+v", result)
	}
	if result.UnsafeExecutionContractCount != 1 || result.MutationAttemptCount != 0 || len(client.writes) != 0 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
}

func TestAuthorityExecutorReconcilerWritesRebuildPlannedStatus(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:  []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		rebuilds: []SwBlockReplicaRebuildObject{authorityExecutorRebuildTarget()},
	}
	now := time.Date(2026, 6, 23, 19, 0, 0, 0, time.UTC)
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationRebuildTraffic,
		Now:                    func() time.Time { return now },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.MutationAttemptCount != 1 ||
		result.RebuildProgressMutationAttempts != 1 ||
		result.AckEligibilityMutationAttempts != 0 ||
		result.BlockedReason != "" ||
		len(client.writes) != 0 ||
		len(client.rebuildWrites) != 1 {
		t.Fatalf("result=%+v ack_writes=%+v rebuild_writes=%+v", result, client.writes, client.rebuildWrites)
	}
	write := client.rebuildWrites[0]
	if write.ref.Name != "rebuild-r1" {
		t.Fatalf("write ref=%+v", write.ref)
	}
	status := write.status
	if status.State != "planned" ||
		status.ReasonCode != AuthorityExecutorReasonRebuildPlanned ||
		!status.FrontendFencedBeforeRebuild ||
		!status.PrimaryUnchanged ||
		!status.DurableFrontierKnown ||
		status.DurableFrontierLSN != 51 ||
		!status.RequiredFrontierKnown ||
		status.RequiredFrontierLSN != 52 ||
		status.DurableFrontierCaughtUp ||
		status.RebuildTrafficStarted ||
		!status.NoFrontendPublication ||
		!status.NoCrossVolumeIdentityChange ||
		!status.ObservedAt.Equal(now) {
		t.Fatalf("status=%+v", status)
	}
	for _, want := range []string{"no_rebuild_data_movement", "no_frontend_publication", "no_failback"} {
		if !authorityExecutorStringSliceContains(status.NonClaims, want) {
			t.Fatalf("non-claims missing %s: %+v", want, status.NonClaims)
		}
	}
}

func TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:  []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		rebuilds: []SwBlockReplicaRebuildObject{authorityExecutorRuntimeRebuildTarget()},
	}
	runtime := &fakeAuthorityRebuildRuntime{
		result: AuthorityRebuildRuntimeResult{
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			EvidenceRefs:         []string{"runtime-terminal-evidence.txt"},
		},
	}
	now := time.Date(2026, 6, 23, 20, 0, 0, 0, time.UTC)
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		RebuildRuntime:         runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationRebuildTraffic,
		Now:                    func() time.Time { return now },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.MutationAttemptCount != 1 ||
		result.RebuildProgressMutationAttempts != 1 ||
		result.BlockedReason != "" ||
		len(runtime.requests) != 1 ||
		len(client.rebuildWrites) != 2 {
		t.Fatalf("result=%+v runtime=%+v rebuild_writes=%+v", result, runtime.requests, client.rebuildWrites)
	}
	req := runtime.requests[0]
	if req.VolumeName != "rebuild" ||
		req.VolumeID != "pvc-rebuild" ||
		req.PVCName != "rebuild-pvc" ||
		req.ReplicaID != "r1" ||
		req.RuntimeEndpoint != "http://127.0.0.1:23260/rebuild/runtime" ||
		req.TargetDataAddr != "127.0.0.1:19103" ||
		req.SessionID != 1001 ||
		req.Epoch != 7 ||
		req.EndpointVersion != 3 ||
		req.FromLSN != 52 ||
		req.FrontierHintLSN != 52 ||
		req.BasePinLSN != 60 ||
		req.DurableFrontierLSN != 51 ||
		req.RequiredFrontierLSN != 52 ||
		!req.FrontendFenced ||
		req.FrontendPrimaryReady ||
		!req.NoFrontendPublication ||
		!req.NoCrossVolumeMutation {
		t.Fatalf("runtime request=%+v", req)
	}
	running := client.rebuildWrites[0].status
	if running.State != "running" ||
		running.ReasonCode != AuthorityExecutorReasonRebuildRunning ||
		!running.RebuildTrafficStarted ||
		running.DurableFrontierCaughtUp ||
		running.PublicationDecision != AuthorityExecutorPublicationDecisionBlocked ||
		running.PublicationReason != AuthorityExecutorPublicationReasonCaughtUpRequired ||
		running.PublicationMutationAllowed {
		t.Fatalf("running status=%+v", running)
	}
	caughtUp := client.rebuildWrites[1].status
	if caughtUp.State != "caught_up" ||
		caughtUp.ReasonCode != AuthorityExecutorReasonRebuildCaughtUp ||
		!caughtUp.RebuildTrafficStarted ||
		!caughtUp.DurableFrontierCaughtUp ||
		caughtUp.DurableFrontierLSN != 52 ||
		caughtUp.PublicationDecision != AuthorityExecutorPublicationDecisionDisabled ||
		caughtUp.PublicationReason != AuthorityExecutorPublicationReasonPolicyDisabled ||
		caughtUp.PublicationMutationAllowed ||
		!caughtUp.NoFrontendPublication ||
		!caughtUp.NoCrossVolumeIdentityChange ||
		!authorityExecutorStringSliceContains(caughtUp.EvidenceRefs, "runtime-terminal-evidence.txt") {
		t.Fatalf("caught_up status=%+v", caughtUp)
	}
}

func TestAuthorityExecutorReconcilerKeepsRunningWhenRuntimeOnlyStarts(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:  []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		rebuilds: []SwBlockReplicaRebuildObject{authorityExecutorRuntimeRebuildTarget()},
	}
	runtime := &fakeAuthorityRebuildRuntime{
		result: AuthorityRebuildRuntimeResult{
			RuntimeState: "started",
			EvidenceRefs: []string{
				"blockvolume-runtime-started.txt",
			},
		},
	}
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		RebuildRuntime:         runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationRebuildTraffic,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.BlockedReason != "" ||
		result.MutationAttemptCount != 1 ||
		len(runtime.requests) != 1 ||
		len(client.rebuildWrites) != 1 {
		t.Fatalf("result=%+v runtime=%+v rebuild_writes=%+v", result, runtime.requests, client.rebuildWrites)
	}
	running := client.rebuildWrites[0].status
	if running.State != "running" ||
		running.ReasonCode != AuthorityExecutorReasonRebuildRunning ||
		!running.RebuildTrafficStarted ||
		running.DurableFrontierCaughtUp ||
		running.PublicationDecision != AuthorityExecutorPublicationDecisionBlocked ||
		running.PublicationMutationAllowed {
		t.Fatalf("running status=%+v", running)
	}
}

func TestAuthorityExecutorReconcilerPublishesAckEligibilityAfterRebuildCaughtUp(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:       []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		eligibilities: []SwBlockReplicaEligibilityObject{authorityExecutorRebuildEligibilityTarget()},
		rebuilds:      []SwBlockReplicaRebuildObject{authorityExecutorCaughtUpRebuildTarget()},
	}
	now := time.Date(2026, 6, 25, 2, 0, 0, 0, time.UTC)
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
		Now:                    func() time.Time { return now },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.MutationAttemptCount != 1 ||
		result.AckEligibilityMutationAttempts != 1 ||
		result.RebuildProgressMutationAttempts != 0 ||
		result.BlockedReason != "" ||
		len(client.writes) != 1 ||
		len(client.rebuildWrites) != 0 {
		t.Fatalf("result=%+v ack_writes=%+v rebuild_writes=%+v", result, client.writes, client.rebuildWrites)
	}
	write := client.writes[0]
	if write.ref.Name != "rebuild-r1-ack" {
		t.Fatalf("write ref=%+v", write.ref)
	}
	status := write.status
	if status.ReasonCode != AuthorityExecutorReasonAckEligibilityRecorded ||
		!status.AckEligibilityKnown ||
		!status.AckEligible ||
		!status.FrontendFencedAfterExecution ||
		!status.PrimaryUnchanged ||
		!status.DurableFrontierCovered ||
		!status.NoCrossVolumeIdentityChange ||
		!status.ObservedAt.Equal(now) {
		t.Fatalf("status=%+v", status)
	}
	for _, want := range []string{"rebuild-contract.txt", "rebuild.txt", "runtime-terminal-evidence.txt"} {
		if !authorityExecutorStringSliceContains(status.EvidenceRefs, want) {
			t.Fatalf("evidence refs missing %s: %+v", want, status.EvidenceRefs)
		}
	}
	for _, want := range []string{"no_frontend_publication", "no_failback", "no_primary_authority_change", "no_cross_volume_mutation"} {
		if !authorityExecutorStringSliceContains(status.NonClaims, want) {
			t.Fatalf("non-claims missing %s: %+v", want, status.NonClaims)
		}
	}
}

func TestAuthorityExecutorReconcilerHoldsAckEligibilityUntilRebuildCaughtUp(t *testing.T) {
	for name, rebuild := range map[string]SwBlockReplicaRebuildObject{
		"running":        authorityExecutorRunningRebuildTarget(),
		"policy_allowed": authorityExecutorCaughtUpRebuildTargetWithPublicationAllowed(),
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeAuthorityExecutorClient{
				volumes:       []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
				eligibilities: []SwBlockReplicaEligibilityObject{authorityExecutorRebuildEligibilityTarget()},
				rebuilds:      []SwBlockReplicaRebuildObject{rebuild},
			}
			result, err := (AuthorityExecutorReconciler{
				Namespace:              "kube-system",
				Client:                 client,
				ExecutionRequested:     true,
				ExecutionPolicyEnabled: true,
				AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
			}).Reconcile(context.Background())
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if result.BlockedReason != AuthorityExecutorBlockedTerminalEvidence ||
				result.TerminalEvidenceMissingCount != 1 ||
				result.MutationAttemptCount != 0 ||
				result.AckEligibilityMutationAttempts != 0 ||
				len(client.writes) != 0 {
				t.Fatalf("result=%+v writes=%+v", result, client.writes)
			}
		})
	}
}

func TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:  []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		rebuilds: []SwBlockReplicaRebuildObject{authorityExecutorRuntimeRebuildTarget()},
	}
	runtime := &fakeAuthorityRebuildRuntime{
		results: []AuthorityRebuildRuntimeResult{
			{
				RuntimeState: "started",
				EvidenceRefs: []string{
					"blockvolume-runtime-started.txt",
				},
			},
			{
				RuntimeState:         "caught_up",
				DurableFrontierKnown: true,
				DurableFrontierLSN:   52,
				EvidenceRefs: []string{
					"blockvolume-runtime-caught-up.txt",
				},
			},
		},
	}
	reconciler := AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		RebuildRuntime:         runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationRebuildTraffic,
	}
	if _, err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if len(runtime.requests) != 2 || len(client.rebuildWrites) != 3 {
		t.Fatalf("requests=%d writes=%d", len(runtime.requests), len(client.rebuildWrites))
	}
	if got := client.rebuildWrites[0].status; got.State != "running" || got.DurableFrontierCaughtUp {
		t.Fatalf("first running status=%+v", got)
	}
	if got := client.rebuildWrites[1].status; got.State != "running" || got.DurableFrontierCaughtUp {
		t.Fatalf("second running status=%+v", got)
	}
	caughtUp := client.rebuildWrites[2].status
	if caughtUp.State != "caught_up" ||
		caughtUp.ReasonCode != AuthorityExecutorReasonRebuildCaughtUp ||
		!caughtUp.DurableFrontierCaughtUp ||
		caughtUp.DurableFrontierLSN != 52 ||
		caughtUp.PublicationDecision != AuthorityExecutorPublicationDecisionDisabled ||
		caughtUp.PublicationMutationAllowed ||
		!authorityExecutorStringSliceContains(caughtUp.EvidenceRefs, "blockvolume-runtime-caught-up.txt") {
		t.Fatalf("caught_up status=%+v", caughtUp)
	}
}

func TestAuthorityExecutorReconcilerWritesBlockedStatusWhenRebuildRuntimeFails(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:  []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		rebuilds: []SwBlockReplicaRebuildObject{authorityExecutorRuntimeRebuildTarget()},
	}
	runtime := &fakeAuthorityRebuildRuntime{err: errors.New("runtime refused")}
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		RebuildRuntime:         runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationRebuildTraffic,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected runtime failure, result=%+v", result)
	}
	if result.BlockedReason != AuthorityExecutorReasonRebuildRuntimeFailed ||
		result.MutationAttemptCount != 1 ||
		len(runtime.requests) != 1 ||
		len(client.rebuildWrites) != 2 {
		t.Fatalf("result=%+v runtime=%+v rebuild_writes=%+v", result, runtime.requests, client.rebuildWrites)
	}
	blocked := client.rebuildWrites[1].status
	if blocked.State != "blocked" ||
		blocked.ReasonCode != AuthorityExecutorReasonRebuildRuntimeFailed ||
		!blocked.RebuildTrafficStarted ||
		blocked.DurableFrontierCaughtUp {
		t.Fatalf("blocked status=%+v", blocked)
	}
}

func TestAuthorityExecutorReconcilerBlocksWhenRuntimeTargetFactsMissing(t *testing.T) {
	target := authorityExecutorRebuildTarget()
	target.Spec.RuntimeEndpoint = ""
	client := &fakeAuthorityExecutorClient{
		volumes:  []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		rebuilds: []SwBlockReplicaRebuildObject{target},
	}
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		RebuildRuntime:         &fakeAuthorityRebuildRuntime{},
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationRebuildTraffic,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RebuildRuntimeTargetMissingCount != 1 ||
		result.BlockedReason != AuthorityExecutorReasonRebuildRuntimeTargetMissing ||
		len(client.rebuildWrites) != 1 {
		t.Fatalf("result=%+v writes=%+v", result, client.rebuildWrites)
	}
	status := client.rebuildWrites[0].status
	if status.State != "blocked" ||
		status.ReasonCode != AuthorityExecutorReasonRebuildRuntimeTargetMissing ||
		!status.RebuildTrafficStarted {
		t.Fatalf("status=%+v", status)
	}
}

func TestAuthorityExecutorReconcilerRejectsExecutionWhenPolicyDisabled(t *testing.T) {
	result, err := (AuthorityExecutorReconciler{
		Client:             &fakeAuthorityExecutorClient{},
		ExecutionRequested: true,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected disabled policy to fail closed, result=%+v", result)
	}
	if result.BlockedReason != AuthorityExecutorBlockedPolicyDisabled ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerBlocksExecutionWhenAckTargetMissing(t *testing.T) {
	volume := authorityExecutorReadyVolume()
	result, err := (AuthorityExecutorReconciler{
		Client:                 &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{volume}},
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.BlockedReason != AuthorityExecutorBlockedMutationTargetMissing ||
		result.AckEligibilityTargetMissingCount != 1 ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerHoldsWhenTerminalEvidenceMissing(t *testing.T) {
	volume := authorityExecutorReadyVolume()
	volume.Status.ReplicaReintegrations[0].FrontendFenced = false
	client := &fakeAuthorityExecutorClient{
		volumes:       []SwBlockVolumeObject{volume},
		eligibilities: []SwBlockReplicaEligibilityObject{authorityExecutorTarget()},
	}
	result, err := (AuthorityExecutorReconciler{
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.BlockedReason != AuthorityExecutorBlockedTerminalEvidence ||
		result.TerminalEvidenceMissingCount != 1 ||
		result.MutationAttemptCount != 0 ||
		len(client.writes) != 0 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
}

func TestAuthorityExecutorReconcilerWritesAckEligibilityStatusWhenTerminalEvidenceReady(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:       []SwBlockVolumeObject{authorityExecutorReadyVolume()},
		eligibilities: []SwBlockReplicaEligibilityObject{authorityExecutorTarget()},
	}
	now := time.Date(2026, 6, 23, 1, 2, 3, 0, time.UTC)
	result, err := (AuthorityExecutorReconciler{
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
		Now:                    func() time.Time { return now },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.MutationAttemptCount != 1 ||
		result.AckEligibilityMutationAttempts != 1 ||
		result.BlockedReason != "" ||
		len(client.writes) != 1 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
	write := client.writes[0]
	if write.ref.Name != "returned-r1" {
		t.Fatalf("write ref=%+v", write.ref)
	}
	status := write.status
	if !status.AckEligibilityKnown ||
		!status.AckEligible ||
		!status.FrontendFencedAfterExecution ||
		!status.PrimaryUnchanged ||
		!status.DurableFrontierCovered ||
		!status.NoCrossVolumeIdentityChange ||
		status.ReasonCode != AuthorityExecutorReasonAckEligibilityRecorded ||
		!status.ObservedAt.Equal(now) {
		t.Fatalf("status=%+v", status)
	}
	if len(status.Conditions) != 1 || status.Conditions[0].Reason != AuthorityExecutorReasonAckEligibilityRecorded {
		t.Fatalf("conditions=%+v", status.Conditions)
	}
}

type fakeAuthorityExecutorClient struct {
	volumes       []SwBlockVolumeObject
	eligibilities []SwBlockReplicaEligibilityObject
	rebuilds      []SwBlockReplicaRebuildObject
	writes        []fakeReplicaEligibilityWrite
	rebuildWrites []fakeReplicaRebuildWrite
}

type fakeAuthorityRebuildRuntime struct {
	result   AuthorityRebuildRuntimeResult
	results  []AuthorityRebuildRuntimeResult
	err      error
	requests []AuthorityRebuildRuntimeRequest
}

func (f *fakeAuthorityRebuildRuntime) ExecuteRebuild(_ context.Context, req AuthorityRebuildRuntimeRequest) (AuthorityRebuildRuntimeResult, error) {
	f.requests = append(f.requests, req)
	if len(f.results) > 0 {
		result := f.results[0]
		f.results = f.results[1:]
		return result, f.err
	}
	return f.result, f.err
}

type fakeReplicaEligibilityWrite struct {
	ref    OperatorObjectRef
	status SwBlockReplicaEligibilityCRDStatus
}

type fakeReplicaRebuildWrite struct {
	ref    OperatorObjectRef
	status SwBlockReplicaRebuildCRDStatus
}

func (f *fakeAuthorityExecutorClient) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}

func (f *fakeAuthorityExecutorClient) ListSwBlockReplicaEligibilities(context.Context, string) ([]SwBlockReplicaEligibilityObject, error) {
	return append([]SwBlockReplicaEligibilityObject(nil), f.eligibilities...), nil
}

func (f *fakeAuthorityExecutorClient) ListSwBlockReplicaRebuilds(context.Context, string) ([]SwBlockReplicaRebuildObject, error) {
	return append([]SwBlockReplicaRebuildObject(nil), f.rebuilds...), nil
}

func (f *fakeAuthorityExecutorClient) WriteReplicaEligibilityStatus(_ context.Context, ref OperatorObjectRef, status SwBlockReplicaEligibilityCRDStatus) error {
	f.writes = append(f.writes, fakeReplicaEligibilityWrite{ref: ref, status: status})
	return nil
}

func (f *fakeAuthorityExecutorClient) WriteReplicaRebuildStatus(_ context.Context, ref OperatorObjectRef, status SwBlockReplicaRebuildCRDStatus) error {
	f.rebuildWrites = append(f.rebuildWrites, fakeReplicaRebuildWrite{ref: ref, status: status})
	return nil
}

func authorityExecutorReadyVolume() SwBlockVolumeObject {
	return SwBlockVolumeObject{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "returned"},
		Status: SwBlockVolumeCRDStatus{
			VolumeID: "pvc-1",
			PVCName:  "demo",
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
				EvidenceRefs:          []string{"returned.txt"},
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
				AllowedMutationClass:     []string{AuthorityExecutorAllowedMutationAckEligibility},
				ForbiddenMutationClass:   []string{"frontend_publication", "rebuild_traffic", "failback"},
				TerminalEvidenceRequired: returnedReplicaTerminalEvidenceRequired(ManagedVolumeActionReintegrateReturned),
				EvidenceRefs:             []string{"contract.txt"},
			}},
		},
	}
}

func authorityExecutorRebuildVolume(executionEnabled, mutationAllowed bool) SwBlockVolumeObject {
	return SwBlockVolumeObject{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "rebuild"},
		Status: SwBlockVolumeCRDStatus{
			VolumeID: "pvc-rebuild",
			PVCName:  "rebuild-pvc",
			ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
				ReplicaID:             "r1",
				State:                 ReturnedReplicaStateRecovering,
				ReasonCode:            ReasonCandidateFrontierBehind,
				FrontendFenced:        true,
				FrontendPrimaryReady:  false,
				AckEligibilityKnown:   true,
				AckEligible:           false,
				DurableFrontierKnown:  true,
				DurableFrontierLSN:    51,
				RequiredFrontierKnown: true,
				RequiredFrontierLSN:   52,
				EvidenceRefs:          []string{"rebuild.txt"},
			}},
			ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
				ActionType:               ManagedVolumeActionRebuildReturned,
				ReplicaID:                "r1",
				Decision:                 ReturnedReplicaExecutorContractDisabled,
				Reason:                   ReturnedReplicaExecutorContractReasonExecutorDisabled,
				OwnerExecutor:            "authority_recovery_executor",
				ExecutionEnabled:         executionEnabled,
				MutationAllowed:          mutationAllowed,
				PreflightDecision:        ReturnedReplicaExecutorPreflightReady,
				PreflightReason:          ReturnedReplicaExecutorPreflightReasonSatisfied,
				AllowedMutationClass:     []string{"rebuild_traffic"},
				ForbiddenMutationClass:   []string{"ack_eligibility", "frontend_publication", "failback"},
				TerminalEvidenceRequired: returnedReplicaTerminalEvidenceRequired(ManagedVolumeActionRebuildReturned),
				EvidenceRefs:             []string{"rebuild-contract.txt"},
			}},
		},
	}
}

func authorityExecutorTarget() SwBlockReplicaEligibilityObject {
	return SwBlockReplicaEligibilityObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaEligibilityKind,
			Namespace:  "kube-system",
			Name:       "returned-r1",
		},
		Spec: SwBlockReplicaEligibilitySpec{
			VolumeName: "returned",
			VolumeID:   "pvc-1",
			PVCName:    "demo",
			ReplicaID:  "r1",
		},
	}
}

func authorityExecutorRebuildTarget() SwBlockReplicaRebuildObject {
	return SwBlockReplicaRebuildObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaRebuildKind,
			Namespace:  "kube-system",
			Name:       "rebuild-r1",
		},
		Spec: SwBlockReplicaRebuildSpec{
			VolumeName: "rebuild",
			VolumeID:   "pvc-rebuild",
			PVCName:    "rebuild-pvc",
			ReplicaID:  "r1",
		},
	}
}

func authorityExecutorRuntimeRebuildTarget() SwBlockReplicaRebuildObject {
	target := authorityExecutorRebuildTarget()
	target.Spec.RuntimeEndpoint = "http://127.0.0.1:23260/rebuild/runtime"
	target.Spec.TargetDataAddr = "127.0.0.1:19103"
	target.Spec.SessionID = 1001
	target.Spec.Epoch = 7
	target.Spec.EndpointVersion = 3
	target.Spec.FromLSN = 52
	target.Spec.FrontierHintLSN = 52
	target.Spec.BasePinLSN = 60
	return target
}

func authorityExecutorRebuildEligibilityTarget() SwBlockReplicaEligibilityObject {
	return SwBlockReplicaEligibilityObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaEligibilityKind,
			Namespace:  "kube-system",
			Name:       "rebuild-r1-ack",
		},
		Spec: SwBlockReplicaEligibilitySpec{
			VolumeName: "rebuild",
			VolumeID:   "pvc-rebuild",
			PVCName:    "rebuild-pvc",
			ReplicaID:  "r1",
		},
	}
}

func authorityExecutorRunningRebuildTarget() SwBlockReplicaRebuildObject {
	target := authorityExecutorRebuildTarget()
	target.Status = SwBlockReplicaRebuildCRDStatus{
		Executor:                    "authority_recovery_executor",
		State:                       "running",
		ReasonCode:                  AuthorityExecutorReasonRebuildRunning,
		FrontendFencedBeforeRebuild: true,
		PrimaryUnchanged:            true,
		DurableFrontierKnown:        true,
		DurableFrontierLSN:          51,
		RequiredFrontierKnown:       true,
		RequiredFrontierLSN:         52,
		DurableFrontierCaughtUp:     false,
		RebuildTrafficStarted:       true,
		PublicationDecision:         AuthorityExecutorPublicationDecisionBlocked,
		PublicationReason:           AuthorityExecutorPublicationReasonCaughtUpRequired,
		PublicationMutationAllowed:  false,
		NoFrontendPublication:       true,
		NoCrossVolumeIdentityChange: true,
		EvidenceRefs:                []string{"runtime-running-evidence.txt"},
	}
	return target
}

func authorityExecutorCaughtUpRebuildTarget() SwBlockReplicaRebuildObject {
	target := authorityExecutorRebuildTarget()
	target.Status = SwBlockReplicaRebuildCRDStatus{
		Executor:                    "authority_recovery_executor",
		State:                       "caught_up",
		ReasonCode:                  AuthorityExecutorReasonRebuildCaughtUp,
		FrontendFencedBeforeRebuild: true,
		PrimaryUnchanged:            true,
		DurableFrontierKnown:        true,
		DurableFrontierLSN:          52,
		RequiredFrontierKnown:       true,
		RequiredFrontierLSN:         52,
		DurableFrontierCaughtUp:     true,
		RebuildTrafficStarted:       true,
		PublicationDecision:         AuthorityExecutorPublicationDecisionDisabled,
		PublicationReason:           AuthorityExecutorPublicationReasonPolicyDisabled,
		PublicationMutationAllowed:  false,
		NoFrontendPublication:       true,
		NoCrossVolumeIdentityChange: true,
		EvidenceRefs:                []string{"runtime-terminal-evidence.txt"},
	}
	return target
}

func authorityExecutorCaughtUpRebuildTargetWithPublicationAllowed() SwBlockReplicaRebuildObject {
	target := authorityExecutorCaughtUpRebuildTarget()
	target.Status.PublicationMutationAllowed = true
	return target
}
