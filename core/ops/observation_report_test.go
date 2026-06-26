package ops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObservationExplain_IncludesManagedVolumeProjectionAndDryRunActions(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	cluster.Status = ObservationStatusBlocked
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:       "pvc-loopback",
		Namespace:      "default",
		PVCName:        "demo-pvc",
		Status:         ObservationStatusBlocked,
		Reason:         ReasonPublishTargetLoopbackCrossNode,
		PrimaryReplica: "r1",
		PrimaryNode:    "m01",
		PublishTarget:  "127.0.0.1:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "127.0.0.1:3260",
		}},
		Conditions: []ObservationCondition{{
			Type:     "Attach",
			Status:   "false",
			Reason:   ReasonPublishTargetLoopbackCrossNode,
			Severity: "error",
			Message:  "writer scheduled on m02 but publish target is loopback",
		}},
	}}

	text := RenderObservationExplainText(cluster)
	for _, want := range []string{
		"managed_volume pvc-loopback status=blocked reason=publish_target_loopback_cross_node",
		"managed_volume_state kubernetes=bound authority=primary_available",
		"managed_volume_condition Ready status=False reason=publish_target_loopback_cross_node severity=warning",
		"managed_volume_condition Blocked status=True reason=publish_target_loopback_cross_node severity=warning",
		"managed_volume_action safe_k8s.reinstall_external_iscsi mode=dry_run side_effect=safe_k8s executor=installer_or_operator",
		"managed_volume_action_preconditions safe_k8s.reinstall_external_iscsi multiple_kubernetes_nodes,loopback_publish_target,pod_scheduled_on_different_node",
		"managed_volume_action_invariants safe_k8s.reinstall_external_iscsi INV-K8S-NONLOOPBACK-001",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
}

func TestObservationReportSummary_IncludesManagedVolumeStatus(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{healthyObservationVolume()}
	cluster.Cleanup = &CleanupEvidence{
		Status:                 "ok",
		KubernetesResidueCount: 0,
		ISCSIResidueCount:      0,
		MultipathResidueCount:  0,
		ProcessResidueCount:    0,
		HostPathResidueCount:   0,
		FailureCount:           0,
		EvidenceRef:            "cleanup-summary.txt",
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"operator_snapshot=operator-snapshot.json",
		"cleanup_status=ok",
		"k8s_residue_count=0",
		"iscsi_residue_count=0",
		"multipath_residue_count=0",
		"process_residue_count=0",
		"hostpath_residue_count=0",
		"failure_count=0",
		"cleanup_evidence=cleanup-summary.txt",
		"volume=pvc-healthy status=ok pvc=default/mysql-data",
		"managed_volume=pvc-healthy status=ready reason=first_volume_verified",
		"managed_volume_authority=pvc-healthy primary=r1 publish_target=192.168.1.181:3260 epoch=1 endpoint_version=1",
		"managed_volume_condition=Ready status=True reason=first_volume_verified severity=info",
		"managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationReportSummary_IncludesReturnedReplicaProjection(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC))
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-returned",
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			Observed:             true,
			Role:                 "replica",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          false,
		}, {
			ReplicaID: "r2",
			Observed:  true,
			Role:      "primary",
		}},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"managed_volume=pvc-returned status=recovering",
		"managed_volume_returned_replica=pvc-returned replica=r1 state=fenced reason=returned_replica_frontend_fenced",
		"frontend_fenced=true ack_eligibility_known=true ack_eligible=false durable_frontier_known=true durable_lsn=52",
		"managed_volume_executor_preflight=authority.reintegrate_returned_replica target=r1 decision=ready reason=preconditions_satisfied mode=dry_run executor=authority_recovery_executor mutation_allowed=false ack_eligibility_known=true required_lsn=52 durable_lsn=52",
		"managed_volume_executor_contract=authority.reintegrate_returned_replica target=r1 decision=disabled reason=executor_policy_disabled executor=authority_recovery_executor execution_enabled=false mutation_allowed=false allowed_mutation=ack_eligibility terminal_evidence=ack_eligibility_known,ack_eligible_true,frontend_fenced_after_execution,primary_unchanged,durable_frontier_covered,no_cross_volume_identity_change",
		"managed_volume_action=authority.reintegrate_returned_replica mode=dry_run side_effect=authority_mutating executor=authority_recovery_executor decision=allowed",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationReportSummary_IncludesReturnedReplicaFailbackContract(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC))
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-failback",
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			Observed:             true,
			Role:                 "previous_primary",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          true,
			StalePrimaryFenced:   true,
		}, {
			ReplicaID: "r2",
			Observed:  true,
			Role:      "primary",
		}},
		EvidenceRefs: []string{"returned-replica-summary.txt"},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"managed_volume_returned_replica=pvc-failback replica=r1 state=fenced",
		"ack_eligibility_known=true ack_eligible=true",
		"managed_volume_executor_preflight=authority.failback_returned_replica target=r1 decision=ready reason=preconditions_satisfied",
		"managed_volume_executor_contract=authority.failback_returned_replica target=r1 decision=disabled reason=executor_policy_disabled executor=authority_recovery_executor execution_enabled=false mutation_allowed=false allowed_mutation=failback",
		"terminal_evidence=ack_eligible_true,frontend_fenced_before_failback,failback_authority_owner,authority_epoch_advanced,single_primary_after_failback,publish_target_swapped_after_failback,no_cross_volume_identity_change",
		"managed_volume_action=authority.failback_returned_replica mode=dry_run side_effect=authority_mutating executor=authority_recovery_executor decision=rejected",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationReportHTML_IncludesManagedVolumeConditions(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	cluster.Status = ObservationStatusBlocked
	cluster.Cleanup = &CleanupEvidence{
		Status:                 "failed",
		KubernetesResidueCount: 1,
		MultipathResidueCount:  2,
		FailureCount:           2,
		ReasonCodes:            []string{"multipath_residue_present"},
		EvidenceRef:            "cleanup-summary.txt",
	}
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:       "pvc-loopback",
		Namespace:      "default",
		PVCName:        "demo-pvc",
		Status:         ObservationStatusBlocked,
		Reason:         ReasonPublishTargetLoopbackCrossNode,
		PrimaryReplica: "r1",
		PrimaryNode:    "m01",
		PublishTarget:  "127.0.0.1:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "127.0.0.1:3260",
		}},
	}}

	html := RenderObservationReportHTML(cluster)
	for _, want := range []string{
		"Lifecycle Cleanup",
		"Support Evidence",
		"Safe Next Steps",
		"Managed Volumes",
		"Managed Volume Conditions",
		"cleanup-summary.txt",
		"collect-helm-support-bundle.sh",
		"verify-helm-cleanup.sh",
		"pvc-loopback",
		"publish_target_loopback_cross_node",
		"safe_k8s.reinstall_external_iscsi",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("html missing %q:\n%s", want, html)
		}
	}
}

func TestObservationReportSummary_IncludesCleanupVisibilityNextStep(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 5, 19, 15, 0, 0, time.UTC))
	cluster.Cleanup = &CleanupEvidence{
		Status:            "failed",
		ISCSIResidueCount: 1,
		FailureCount:      1,
		ReasonCodes:       []string{"iscsi_node_records_present"},
		EvidenceRef:       "cleanup-summary.txt",
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"cleanup_status=failed",
		"iscsi_residue_count=1",
		"support_bundle_ref=cleanup-summary.txt",
		"safe_next_step=observe.collect_bundle mode=read_only mutation_allowed=false",
		"safe_next_step=observe.verify_cleanup mode=scripted mutation_allowed=false",
		"verify-helm-cleanup.sh",
		"reason=iscsi_node_records_present",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationReportSummary_IncludesSupportBundlePointers(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 5, 18, 45, 0, 0, time.UTC))
	cluster.Status = ObservationStatusBlocked
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-blocked",
		Namespace:         "default",
		PVCName:           "blocked-pvc",
		Status:            ObservationStatusBlocked,
		Reason:            ReasonCSINodeImagePullFailed,
		SupportBundleHint: "support/bundle",
	}}
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:      "pvc-blocked",
		PVCName:       "blocked-pvc",
		ProductStatus: ObservationStatusBlocked,
		ProductReason: ReasonCSINodeImagePullFailed,
		EvidenceRefs:  []string{"support/bundle/replayed-report/summary.txt"},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"support_bundle_ref=support/bundle",
		"support_bundle_ref=support/bundle/replayed-report/summary.txt",
		"safe_next_step=observe.collect_bundle mode=read_only mutation_allowed=false",
		"collect-helm-support-bundle.sh",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationReport_IncludesInstallDrift(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 13, 13, 10, 0, 0, time.UTC))
	cluster.InstallDrift = &InstallDriftEvidence{
		Status:       InstallDriftStatusMismatch,
		ReasonCode:   ReasonInstallDriftMismatch,
		CurrentImage: "sw-block:old",
		DesiredImage: "sw-block:new",
		EvidenceRef:  "install-drift-summary.txt",
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"install_drift_status=mismatch reason=install_drift_mismatch evidence=install-drift-summary.txt",
		"install_drift_image current=sw-block:old desired=sw-block:new",
		"cluster_condition=Blocked status=True reason=install_drift_mismatch severity=warning",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
	html := RenderObservationReportHTML(cluster)
	if !strings.Contains(html, "Install Drift") || !strings.Contains(html, "sw-block:old") || !strings.Contains(html, "sw-block:new") {
		t.Fatalf("html missing install drift:\n%s", html)
	}
}

func TestObservationReportArtifacts_JSONIncludesManagedVolumeProjection(t *testing.T) {
	dir := t.TempDir()
	cluster := NewClusterEvidence(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{healthyObservationVolume()}

	if err := WriteObservationReportArtifacts(dir, cluster); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ObservationReportJSONArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var decoded ClusterEvidence
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode report json: %v\n%s", err, raw)
	}
	if len(decoded.ManagedVolumes) != 1 {
		t.Fatalf("managed_volumes=%d json=%s", len(decoded.ManagedVolumes), raw)
	}
	managed := decoded.ManagedVolumes[0]
	if managed.VolumeID != "pvc-healthy" || managed.Status != ManagedVolumeStatusReady {
		t.Fatalf("managed projection=%+v", managed)
	}
	if len(managed.Actions) == 0 || managed.Actions[0].Mode != ManagedVolumeActionModeReadOnly {
		t.Fatalf("managed actions=%+v", managed.Actions)
	}

	raw, err = os.ReadFile(filepath.Join(dir, ObservationOperatorSnapshotArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot OperatorFoundationSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode operator snapshot json: %v\n%s", err, raw)
	}
	if !snapshot.ReadOnly || snapshot.Mutation.MutationAllowed {
		t.Fatalf("operator snapshot must be read-only: %+v", snapshot)
	}
	if snapshot.Cluster.VolumeCount != 1 || len(snapshot.Volumes) != 1 {
		t.Fatalf("operator snapshot missing volume evidence: %+v", snapshot)
	}
}
