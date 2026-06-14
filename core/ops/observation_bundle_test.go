package ops

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObservationBundle_RendersNodeLossRecoveryStory(t *testing.T) {
	dir := t.TempDir()
	inventoryDir := filepath.Join(dir, "demo", "ops-inventory-reader-verified")
	if err := os.MkdirAll(inventoryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 16, 16, 3, 6, 0, time.UTC),
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-node-loss",
			Namespace:         "default",
			PVCName:           "sw-block-demo-pvc",
			PVName:            "pvc-node-loss",
			ReplicationFactor: 3,
			Replicas: []VolumeInventoryReplicaInput{
				{
					ReplicaID:       "r1",
					ServerID:        "node-loss-r1",
					NodeName:        "m01",
					Observed:        true,
					Protocol:        "iscsi",
					FrontendAddress: "192.168.1.181:3260",
					AuthorityRole:   "unavailable",
					ReplicationRole: "unavailable",
					AckProfile:      PromotionAckProfileSyncQuorum,
					Issues:          []string{"status_endpoint_unreachable=192.168.1.181:23260"},
				},
				{
					ReplicaID:              "r2",
					ServerID:               "node-loss-r2",
					NodeName:               "m02",
					Observed:               true,
					Protocol:               "iscsi",
					FrontendAddress:        "192.168.1.184:3260",
					AuthorityRole:          "primary",
					ReplicationRole:        "none",
					Healthy:                true,
					FrontendPrimaryReady:   true,
					Epoch:                  2,
					EndpointVersion:        1,
					AckProfile:             PromotionAckProfileSyncQuorum,
					ClaimProfile:           PromotionClaimBetaRecovery,
					DurableLatched:         true,
					DurableOperational:     true,
					RequiredFrontierKnown:  true,
					RequiredFrontierLSN:    52,
					CandidateFrontierKnown: true,
					CandidateFrontierLSN:   52,
				},
				{
					ReplicaID:              "r3",
					ServerID:               "node-loss-r3",
					NodeName:               "tp01",
					Observed:               true,
					Protocol:               "iscsi",
					FrontendAddress:        "192.168.1.188:3260",
					AuthorityRole:          "unknown",
					ReplicationRole:        "replica_ready",
					AckProfile:             PromotionAckProfileSyncQuorum,
					ClaimProfile:           PromotionClaimBetaRecovery,
					DurableLatched:         true,
					DurableOperational:     true,
					RequiredFrontierKnown:  true,
					RequiredFrontierLSN:    52,
					CandidateFrontierKnown: true,
					CandidateFrontierLSN:   52,
				},
			},
		}},
	})
	mustWriteInventory(t, inventoryDir, inventory)
	mustWrite(t, filepath.Join(dir, "demo", NodeLossRecoverySummaryArtifact), strings.Join([]string{
		"node_loss_recovery_summary_version=1",
		"result=promoted",
		"ack_profile=sync-quorum",
		"before_primary=r1@m01",
		"failed=r1@m01",
		"promoted=r2@m02",
		"before_frontend=192.168.1.181:3260",
		"after_frontend=192.168.1.184:3260",
		"pod_recreate_used=true",
		"reader_verified=true",
		"data_check_after_node_loss=reader_checksum_passed",
		"old_primary_stale_io_success_count=0",
	}, "\n"))
	mustWrite(t, filepath.Join(dir, "demo", ControlPlaneTimelineArtifact), strings.Join([]string{
		"event=primary_observed replica=r1 volume=pvc-node-loss",
		"event=candidate_evaluated replica=r2 candidate_ready=true reason=promotion_ready volume=pvc-node-loss",
		"event=authority_published from=r1 to=r2 primary=r2 primary_count=1 volume=pvc-node-loss",
		"event=data_check reader_verified=true result=reader_checksum_passed volume=pvc-node-loss",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-node-loss"})
	if err != nil {
		t.Fatal(err)
	}
	text := RenderObservationExplainText(cluster)
	for _, want := range []string{
		"volume pvc-node-loss status=ok rf=3 ack=sync-quorum",
		"primary r2 on m02 frontend=192.168.1.184:3260",
		"CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260",
		"old primary stale I/O success count is 0",
		"timeline:",
		"authority_published severity=info reason=candidate_covers_required_frontier volume=pvc-node-loss replica=r2",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
	jsonl, err := RenderClusterEventsJSONL(cluster.Events)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonl, `"event_type":"authority_published"`) || !strings.Contains(jsonl, `"reason_code":"candidate_covers_required_frontier"`) {
		t.Fatalf("jsonl missing event evidence:\n%s", jsonl)
	}
}

func TestObservationBundle_RendersImagePullBlockedStoryWithoutInventory(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "demo", KubeSystemPodsDeploysArtifact), `NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local`)

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-blocked"})
	if err != nil {
		t.Fatal(err)
	}
	text := RenderClusterEvidenceText(cluster)
	for _, want := range []string{
		"cluster status=blocked volumes=1 nodes=0",
		"volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed",
		"waiting=ImagePullBackOff on node m02 image sw-block-csi:local",
		"next action: import sw-block-csi:local to the blocked node",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
}

func TestObservationBundle_ReplaysInstallDriftSummary(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "demo", ObservationInstallDriftArtifact), strings.Join([]string{
		"chart_name=seaweed-block",
		"current_chart_version=0.3.5",
		"desired_chart_version=0.4.0",
		"current_image=sw-block:old",
		"desired_image=sw-block:new",
		"current_csi_image=sw-block-csi:old",
		"desired_csi_image=sw-block-csi:new",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if cluster.InstallDrift == nil || cluster.InstallDrift.Status != InstallDriftStatusMismatch {
		t.Fatalf("install drift=%+v", cluster.InstallDrift)
	}
	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.InstallDrift == nil || snapshot.Cluster.InstallDrift.ReasonCode != ReasonInstallDriftMismatch {
		t.Fatalf("snapshot install drift=%+v", snapshot.Cluster.InstallDrift)
	}
	assertCondition(t, snapshot.Cluster.Conditions, ConditionBlocked, "True", ReasonInstallDriftMismatch)
	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"install_drift_status=mismatch reason=install_drift_mismatch evidence=",
		"install-drift-summary.txt",
		"install_drift_chart current=0.3.5 desired=0.4.0",
		"install_drift_image current=sw-block:old desired=sw-block:new csi_current=sw-block-csi:old csi_desired=sw-block-csi:new",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationBundle_ManagedVolumeUsesPrimaryFailureArtifactHints(t *testing.T) {
	dir := t.TempDir()
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := NewClusterEvidence(time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-stage2",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		PrimaryReplica:    "r2",
		PublishTarget:     "192.168.1.184:3261",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.184:3261",
		}},
	}}
	raw, err := MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "demo", PrimaryFailureRecoveryArtifact), strings.Join([]string{
		"promoted_replica=r2",
		"data_check_after_failover=mounted_workload_checksum_passed",
		"pod_recreate_used=false",
		"old_primary_stale_io_success_count=0",
		"transparent_failover_claimed=true",
	}, "\n"))

	out, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ManagedVolumes) != 1 {
		t.Fatalf("managed_volumes=%+v", out.ManagedVolumes)
	}
	managed := out.ManagedVolumes[0]
	if managed.Status != ManagedVolumeStatusRecovered || managed.ReasonCode != ReasonTransparentHostPathRecovered {
		t.Fatalf("managed=%+v", managed)
	}
}

func TestObservationBundle_D7PrefersNewestRestartClusterEvidence(t *testing.T) {
	dir := t.TempDir()
	oldCluster := NewClusterEvidence(time.Date(2026, 5, 25, 21, 32, 48, 0, time.UTC))
	oldCluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-restart",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m01",
		PublishTarget:     "192.168.1.181:3260",
		Epoch:             1,
	}}
	newCluster := NewClusterEvidence(time.Date(2026, 5, 25, 21, 33, 37, 0, time.UTC))
	newCluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-restart",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r2",
		PrimaryNode:       "m02",
		PublishTarget:     "192.168.1.184:3260",
		Epoch:             2,
	}}
	writeClusterEvidenceArtifact(t, filepath.Join(dir, "recovery", "setup", "status", ClusterEvidenceArtifact), oldCluster)
	writeClusterEvidenceArtifact(t, filepath.Join(dir, "restart", RestartClusterEvidenceArtifact), newCluster)

	out, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-restart"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Volumes) != 1 {
		t.Fatalf("volumes=%+v", out.Volumes)
	}
	volume := out.Volumes[0]
	if volume.PrimaryReplica != "r2" || volume.PrimaryNode != "m02" || volume.PublishTarget != "192.168.1.184:3260" || volume.Epoch != 2 {
		t.Fatalf("expected post-restart evidence, got %+v", volume)
	}
	summary := RenderObservationReportSummary(out)
	if !strings.Contains(summary, "primary=r2@m02 frontend=192.168.1.184:3260") {
		t.Fatalf("summary used stale primary:\n%s", summary)
	}
}

func TestObservationBundle_SkipsCorruptClusterEvidenceCandidate(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "stale", "status"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "stale", "status", ClusterEvidenceArtifact), "{not-json")
	good := NewClusterEvidence(time.Date(2026, 5, 25, 21, 33, 37, 0, time.UTC))
	good.Volumes = []VolumeEvidence{healthyObservationVolume()}
	writeClusterEvidenceArtifact(t, filepath.Join(dir, "restart", RestartClusterEvidenceArtifact), good)

	out, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Volumes) != 1 || out.Volumes[0].VolumeID != healthyObservationVolume().VolumeID {
		t.Fatalf("unexpected bundle replay output: %+v", out.Volumes)
	}
}

func TestObservationBundle_CarriesCleanupEvidenceIntoReportSurfaces(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{healthyObservationVolume()})
	mustWrite(t, filepath.Join(dir, "demo", ObservationCleanupSummaryArtifact), strings.Join([]string{
		"cleanup_status=failed",
		"k8s_residue_count=1",
		"iscsi_residue_count=2",
		"multipath_residue_count=3",
		"process_residue_count=4",
		"hostpath_residue_count=5",
		"failure_count=2",
		"failed_phase=collect_and_cleanup",
		"reason_codes=kubernetes_sw_block_resources_present,multipath_maps_present",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if cluster.Cleanup == nil {
		t.Fatalf("missing cleanup evidence")
	}
	if cluster.Cleanup.Status != "failed" || cluster.Cleanup.KubernetesResidueCount != 1 || cluster.Cleanup.MultipathResidueCount != 3 {
		t.Fatalf("cleanup evidence=%+v", cluster.Cleanup)
	}
	if len(cluster.Cleanup.ReasonCodes) != 2 || cluster.Cleanup.ReasonCodes[1] != "multipath_maps_present" {
		t.Fatalf("cleanup reason codes=%+v", cluster.Cleanup.ReasonCodes)
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"cleanup_status=failed",
		"k8s_residue_count=1",
		"iscsi_residue_count=2",
		"multipath_residue_count=3",
		"process_residue_count=4",
		"hostpath_residue_count=5",
		"failure_count=2",
		"failed_phase=collect_and_cleanup",
		"cleanup_evidence=",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.Cleanup == nil || snapshot.Cluster.Cleanup.Status != "failed" || snapshot.Cluster.Cleanup.FailedPhase != "collect_and_cleanup" {
		t.Fatalf("operator cleanup evidence=%+v", snapshot.Cluster.Cleanup)
	}
}

func TestObservationBundle_CarriesHostPrereqEvidenceIntoNodeStatus(t *testing.T) {
	dir := t.TempDir()
	cluster := NewClusterEvidence(time.Date(2026, 6, 8, 13, 0, 0, 0, time.UTC))
	cluster.Nodes = []NodeEvidence{{
		NodeName:       "m02",
		KubernetesNode: "m02",
		Ready:          true,
		Schedulable:    true,
	}, {
		NodeName:       "tp01",
		KubernetesNode: "tp01",
		Ready:          true,
		Schedulable:    true,
	}}
	writeClusterEvidenceArtifact(t, filepath.Join(dir, "status", ClusterEvidenceArtifact), cluster)
	mustWrite(t, filepath.Join(dir, "host", ObservationHostPrereqArtifact), strings.Join([]string{
		"node=m02 iscsi_prereq=missing multipath_prereq=ok command_iscsiadm=missing",
		"node=tp01 iscsi_prereq=ok multipath_prereq=missing command_multipath=missing",
	}, "\n"))

	out, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := BuildOperatorFoundationSnapshot(out)
	if len(snapshot.Cluster.Nodes) != 2 {
		t.Fatalf("nodes=%+v", snapshot.Cluster.Nodes)
	}
	if snapshot.Cluster.Nodes[0].Status != ManagedVolumeStatusBlocked ||
		snapshot.Cluster.Nodes[0].ReasonCode != ReasonISCSIPrereqMissing {
		t.Fatalf("m02 node=%+v", snapshot.Cluster.Nodes[0])
	}
	if snapshot.Cluster.Nodes[1].Status != ManagedVolumeStatusBlocked ||
		snapshot.Cluster.Nodes[1].ReasonCode != ReasonMultipathPrereqMissing {
		t.Fatalf("tp01 node=%+v", snapshot.Cluster.Nodes[1])
	}
	summary := RenderObservationReportSummary(out)
	for _, want := range []string{
		"node=m02 k8s=m02 status=blocked reason=iscsi_prereq_missing",
		"node=tp01 k8s=tp01 status=blocked reason=multipath_prereq_missing",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationBundle_CarriesUnsupportedLoopbackAttachIntoManagedStatus(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{{
		VolumeID:          "pvc-loopback",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 1,
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m01",
		PublishTarget:     "127.0.0.1:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "127.0.0.1:3260",
		}},
	}})
	mustWrite(t, filepath.Join(dir, "demo", ObservationLoopbackAttachArtifact), strings.Join([]string{
		"issue=unsupported_cross_node_loopback_attach",
		"app_node=m02",
		"blockvolume_node=m01",
		"frontend=127.0.0.1:3260",
		"volume_id=pvc-loopback",
		"replica_id=r1",
		"reason=loopback frontend requires app pod and blockvolume on the same node",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-loopback"})
	if err != nil {
		t.Fatal(err)
	}
	if cluster.Status != ObservationStatusBlocked {
		t.Fatalf("cluster status=%s", cluster.Status)
	}
	if len(cluster.ManagedVolumes) != 1 {
		t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
	}
	managed := cluster.ManagedVolumes[0]
	if managed.Status != ManagedVolumeStatusBlocked || managed.ReasonCode != ReasonPublishTargetLoopbackCrossNode {
		t.Fatalf("managed=%+v", managed)
	}
	action := findManagedVolumeAction(managed.Actions, ManagedVolumeActionReinstallExternalISCSI)
	if action == nil {
		t.Fatalf("missing external iSCSI dry-run action: %+v", managed.Actions)
	}
	if action.Decision != ManagedVolumeActionDecisionAllowed ||
		action.Mode != ManagedVolumeActionModeDryRun ||
		action.SideEffectClass != ManagedVolumeSideEffectSafeK8S ||
		action.OwnerExecutor != "installer_or_operator" ||
		action.EvidenceRequired != "loopback_cross_node_evidence" {
		t.Fatalf("unexpected external iSCSI action evaluation: %+v", *action)
	}

	contract := ManagedVolumeOperatorContractFromProjection(managed)
	operatorAction := findManagedVolumeOperatorAction(contract.AllowedActions, ManagedVolumeActionReinstallExternalISCSI)
	if operatorAction == nil {
		t.Fatalf("missing operator action: %+v", contract.AllowedActions)
	}
	if operatorAction.Decision != ManagedVolumeActionDecisionAllowed ||
		operatorAction.MutationAllowed ||
		operatorAction.EvidenceRequired != "loopback_cross_node_evidence" {
		t.Fatalf("unexpected operator action contract: %+v", *operatorAction)
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	snapshotAction := findManagedVolumeOperatorAction(snapshot.Volumes[0].AllowedActions, ManagedVolumeActionReinstallExternalISCSI)
	if snapshotAction == nil {
		t.Fatalf("missing snapshot action: %+v", snapshot.Volumes[0].AllowedActions)
	}
	if snapshotAction.Decision != ManagedVolumeActionDecisionAllowed ||
		snapshotAction.MutationAllowed ||
		snapshot.Mutation.MutationAllowed {
		t.Fatalf("unexpected snapshot action/boundary: action=%+v mutation=%+v", *snapshotAction, snapshot.Mutation)
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"volume=pvc-loopback status=blocked pvc=default/demo-pvc primary=r1@m01 frontend=127.0.0.1:3260",
		"managed_volume=pvc-loopback status=blocked reason=publish_target_loopback_cross_node",
		"managed_volume_condition=Ready status=False reason=publish_target_loopback_cross_node severity=warning",
		"managed_volume_action=safe_k8s.reinstall_external_iscsi mode=dry_run side_effect=safe_k8s executor=installer_or_operator decision=allowed",
		"managed_volume_action_evidence_required=safe_k8s.reinstall_external_iscsi loopback_cross_node_evidence",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	explain := RenderObservationExplainText(cluster)
	for _, want := range []string{
		"managed_volume_action safe_k8s.reinstall_external_iscsi mode=dry_run side_effect=safe_k8s executor=installer_or_operator decision=allowed",
		"managed_volume_action_evidence_required safe_k8s.reinstall_external_iscsi loopback_cross_node_evidence",
	} {
		if !strings.Contains(explain, want) {
			t.Fatalf("explain missing %q:\n%s", want, explain)
		}
	}

	server := httptest.NewServer(NewObservationDashboardHandler(cluster))
	defer server.Close()
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"reason_code": "publish_target_loopback_cross_node"`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"decision": "allowed"`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"mutation_allowed": false`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"evidence_required": "loopback_cross_node_evidence"`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationReportTextArtifact, "managed_volume=pvc-loopback status=blocked reason=publish_target_loopback_cross_node")
}

func TestObservationBundle_DeleteSafetyBlocksWithResidue(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{{
		VolumeID:          "pvc-delete",
		Namespace:         "default",
		PVCName:           "delete-pvc",
		PVName:            "pv-delete",
		ReplicationFactor: 1,
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m01",
		PublishTarget:     "192.168.1.181:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "192.168.1.181:3260",
		}},
	}})
	mustWrite(t, filepath.Join(dir, ObservationCleanupSummaryArtifact), strings.Join([]string{
		"cleanup_status=failed",
		"iscsi_residue_count=1",
		"reason_codes=iscsi_node_records_present",
	}, "\n"))
	mustWrite(t, filepath.Join(dir, ObservationDeleteSafetyArtifact), strings.Join([]string{
		"delete_requested=true",
		"finalizer_present=true",
		"volume_id=pvc-delete",
		"pvc_name=delete-pvc",
		"pv_name=pv-delete",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-delete"})
	if err != nil {
		t.Fatal(err)
	}
	managed := managedProjectionForVolume(cluster.ManagedVolumes, "pvc-delete")
	if managed.DeleteSafety == nil {
		t.Fatalf("missing delete safety: %+v", managed)
	}
	if managed.Status != ManagedVolumeStatusBlocked ||
		managed.ReasonCode != "iscsi_node_records_present" ||
		managed.DeleteSafety.State != DeleteSafetyStateBlocked ||
		managed.DeleteSafety.Decision != ManagedVolumeActionDecisionRejected ||
		managed.DeleteSafety.FinalizerReleaseAllowed {
		t.Fatalf("managed=%+v delete=%+v", managed, managed.DeleteSafety)
	}
	if !hasManagedVolumeAction(managed.Actions, ManagedVolumeActionVerifyCleanup) {
		t.Fatalf("missing verify cleanup action: %+v", managed.Actions)
	}
	if condition := findObservationCondition(managed.Conditions, ConditionCleanupRequired); condition == nil ||
		condition.Status != "True" ||
		condition.Reason != "iscsi_node_records_present" {
		t.Fatalf("cleanup condition=%+v", condition)
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"managed_volume_delete_safety=pvc-delete state=blocked decision=rejected reason=iscsi_node_records_present release_allowed=false action=safe_k8s.release_swblockvolume_finalizer",
		"managed_volume_delete_safety_safe_next_action=pvc-delete observe.verify_cleanup",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
	explain := RenderObservationExplainText(cluster)
	if !strings.Contains(explain, "managed_volume_delete_safety state=blocked decision=rejected reason=iscsi_node_records_present release_allowed=false action=safe_k8s.release_swblockvolume_finalizer") {
		t.Fatalf("explain missing delete safety:\n%s", explain)
	}
	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Volumes[0].Status.DeleteSafety == nil ||
		snapshot.Volumes[0].Status.DeleteSafety.State != DeleteSafetyStateBlocked {
		t.Fatalf("snapshot delete safety=%+v", snapshot.Volumes[0].Status.DeleteSafety)
	}
	server := httptest.NewServer(NewObservationDashboardHandler(cluster))
	defer server.Close()
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"delete_safety": {`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"state": "blocked"`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"reason": "iscsi_node_records_present"`)
}

func TestObservationBundle_DeleteSafetyUnknownWithoutCleanupEvidence(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{{
		VolumeID:          "pvc-missing-cleanup",
		Namespace:         "default",
		PVCName:           "missing-cleanup-pvc",
		PVName:            "pv-missing-cleanup",
		ReplicationFactor: 1,
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m01",
		PublishTarget:     "192.168.1.181:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "192.168.1.181:3260",
		}},
	}})
	mustWrite(t, filepath.Join(dir, ObservationDeleteSafetyArtifact), strings.Join([]string{
		"delete_requested=true",
		"finalizer_present=true",
		"volume_id=pvc-missing-cleanup",
		"pvc_name=missing-cleanup-pvc",
		"pv_name=pv-missing-cleanup",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-missing-cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	managed := managedProjectionForVolume(cluster.ManagedVolumes, "pvc-missing-cleanup")
	if managed.DeleteSafety == nil ||
		managed.DeleteSafety.State != DeleteSafetyStateRequested ||
		managed.DeleteSafety.Decision != ManagedVolumeActionDecisionUnknown ||
		managed.DeleteSafety.Reason != ReasonCleanupEvidenceMissing ||
		managed.DeleteSafety.FinalizerReleaseAllowed {
		t.Fatalf("managed=%+v delete=%+v", managed, managed.DeleteSafety)
	}
	if managed.Status != ManagedVolumeStatusReady {
		t.Fatalf("missing cleanup evidence must not falsify data-plane readiness: %+v", managed)
	}

	summary := RenderObservationReportSummary(cluster)
	if !strings.Contains(summary, "managed_volume_delete_safety=pvc-missing-cleanup state=requested decision=unknown reason=cleanup_evidence_missing release_allowed=false action=safe_k8s.release_swblockvolume_finalizer") {
		t.Fatalf("summary missing unknown delete safety:\n%s", summary)
	}
	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Volumes[0].Status.DeleteSafety == nil ||
		snapshot.Volumes[0].Status.DeleteSafety.Decision != ManagedVolumeActionDecisionUnknown ||
		snapshot.Volumes[0].Status.DeleteSafety.State != DeleteSafetyStateRequested {
		t.Fatalf("snapshot delete safety=%+v", snapshot.Volumes[0].Status.DeleteSafety)
	}
	server := httptest.NewServer(NewObservationDashboardHandler(cluster))
	defer server.Close()
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"decision": "unknown"`)
	assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"reason": "cleanup_evidence_missing"`)
}

func TestObservationBundle_DeleteSafetyReleasableWithCleanCleanupEvidence(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{{
		VolumeID:          "pvc-clean-delete",
		Namespace:         "default",
		PVCName:           "clean-pvc",
		PVName:            "pv-clean",
		ReplicationFactor: 1,
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m01",
		PublishTarget:     "192.168.1.181:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "192.168.1.181:3260",
		}},
	}})
	mustWrite(t, filepath.Join(dir, ObservationCleanupSummaryArtifact), "cleanup_status=ok")
	mustWrite(t, filepath.Join(dir, ObservationDeleteSafetyArtifact), strings.Join([]string{
		"delete_requested=true",
		"finalizer_present=true",
		"volume_id=pvc-clean-delete",
		"pvc_name=clean-pvc",
		"pv_name=pv-clean",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir, VolumeID: "pvc-clean-delete"})
	if err != nil {
		t.Fatal(err)
	}
	managed := managedProjectionForVolume(cluster.ManagedVolumes, "pvc-clean-delete")
	if managed.DeleteSafety == nil ||
		managed.DeleteSafety.State != DeleteSafetyStateReleasable ||
		managed.DeleteSafety.Decision != ManagedVolumeActionDecisionAllowed ||
		!managed.DeleteSafety.FinalizerReleaseAllowed {
		t.Fatalf("managed=%+v delete=%+v", managed, managed.DeleteSafety)
	}
	if managed.Status == ManagedVolumeStatusBlocked {
		t.Fatalf("clean delete evidence must not block managed volume: %+v", managed)
	}
	if condition := findObservationCondition(managed.Conditions, ConditionCleanupRequired); condition == nil ||
		condition.Status != "False" ||
		condition.Reason != ReasonCleanupVerified {
		t.Fatalf("cleanup condition=%+v", condition)
	}
	summary := RenderObservationReportSummary(cluster)
	if !strings.Contains(summary, "managed_volume_delete_safety=pvc-clean-delete state=releasable decision=allowed reason=finalizer_releasable release_allowed=true action=safe_k8s.release_swblockvolume_finalizer") {
		t.Fatalf("summary missing releasable delete safety:\n%s", summary)
	}
	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Volumes[0].Status.DeleteSafety == nil ||
		!snapshot.Volumes[0].Status.DeleteSafety.FinalizerReleaseAllowed {
		t.Fatalf("snapshot delete safety=%+v", snapshot.Volumes[0].Status.DeleteSafety)
	}
}

func TestObservationBundle_LoopbackAttachTargetsNamedVolume(t *testing.T) {
	dir := t.TempDir()
	writeProductClusterEvidence(t, dir, []VolumeEvidence{
		{
			VolumeID:       "pvc-a",
			Namespace:      "default",
			PVCName:        "pvc-a",
			Status:         ObservationStatusOK,
			PrimaryReplica: "r1",
			PrimaryNode:    "m01",
			PublishTarget:  "192.168.1.181:3260",
		},
		{
			VolumeID:       "pvc-b",
			Namespace:      "default",
			PVCName:        "pvc-b",
			Status:         ObservationStatusOK,
			PrimaryReplica: "r1",
			PrimaryNode:    "m01",
			PublishTarget:  "127.0.0.1:3260",
		},
	})
	mustWrite(t, filepath.Join(dir, "demo", ObservationLoopbackAttachArtifact), strings.Join([]string{
		"issue=unsupported_cross_node_loopback_attach",
		"app_node=m02",
		"blockvolume_node=m01",
		"frontend=127.0.0.1:3260",
		"volume_id=pvc-b",
		"replica_id=r1",
	}, "\n"))

	cluster, err := BuildObservationFromBundle(ObservationBundleOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(cluster.ManagedVolumes) != 2 {
		t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
	}
	statusByVolume := map[string]string{}
	reasonByVolume := map[string]string{}
	for _, managed := range cluster.ManagedVolumes {
		statusByVolume[managed.VolumeID] = managed.Status
		reasonByVolume[managed.VolumeID] = managed.ReasonCode
	}
	if statusByVolume["pvc-a"] != ManagedVolumeStatusReady || reasonByVolume["pvc-a"] != ReasonFirstVolumeVerified {
		t.Fatalf("pvc-a status=%s reason=%s", statusByVolume["pvc-a"], reasonByVolume["pvc-a"])
	}
	if statusByVolume["pvc-b"] != ManagedVolumeStatusBlocked || reasonByVolume["pvc-b"] != ReasonPublishTargetLoopbackCrossNode {
		t.Fatalf("pvc-b status=%s reason=%s", statusByVolume["pvc-b"], reasonByVolume["pvc-b"])
	}
}

func TestManagedVolumeFactsFromEvidence_AllowsSameNodeLoopback(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFactsFromEvidence(VolumeEvidence{
		VolumeID:       "pvc-loopback-ok",
		Namespace:      "default",
		PVCName:        "demo-pvc",
		Status:         ObservationStatusOK,
		PrimaryReplica: "r1",
		PrimaryNode:    "m02",
		PublishTarget:  "127.0.0.1:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m02",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "127.0.0.1:3260",
		}},
	}, ManagedVolumeArtifactHints{}))

	if projection.Status != ManagedVolumeStatusReady || projection.ReasonCode != ReasonFirstVolumeVerified {
		t.Fatalf("same-node loopback projection=%+v", projection)
	}
}

func TestObservationBundle_D6ReplayGate_FirstVolumeBlockedAndRecovery(t *testing.T) {
	cases := []struct {
		name       string
		build      func(t *testing.T, dir string)
		wantStatus string
		wantReason string
		wantEvent  string
	}{
		{
			name: "first-volume",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{healthyObservationVolume()})
			},
			wantStatus: ManagedVolumeStatusReady,
			wantReason: ReasonFirstVolumeVerified,
			wantEvent:  "Normal",
		},
		{
			name: "blocked-image-pull",
			build: func(t *testing.T, dir string) {
				mustWrite(t, filepath.Join(dir, "demo", KubeSystemPodsDeploysArtifact), `NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local`)
			},
			wantStatus: ManagedVolumeStatusBlocked,
			wantReason: ReasonCSINodeImagePullFailed,
			wantEvent:  "Warning",
		},
		{
			name: "status-endpoint-unreachable",
			build: func(t *testing.T, dir string) {
				writeStatusEndpointUnreachableInventory(t, dir)
			},
			wantStatus: ManagedVolumeStatusUnknown,
			wantReason: ReasonStatusEndpointUnreachable,
			wantEvent:  "Warning",
		},
		{
			name: "stage2-recovery",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{{
					VolumeID:          "pvc-stage2",
					Namespace:         "default",
					PVCName:           "demo-pvc",
					ReplicationFactor: 3,
					PrimaryReplica:    "r2",
					PublishTarget:     "192.168.1.184:3261",
					Replicas: []ReplicaEvidence{{
						ReplicaID:      "r2",
						KubernetesNode: "m02",
						Role:           "primary",
						Observed:       true,
						FrontendAddr:   "192.168.1.184:3261",
					}},
				}})
				mustWrite(t, filepath.Join(dir, "demo", PrimaryFailureRecoveryArtifact), strings.Join([]string{
					"promoted_replica=r2",
					"data_check_after_failover=mounted_workload_checksum_passed",
					"pod_recreate_used=false",
					"old_primary_stale_io_success_count=0",
					"transparent_failover_claimed=true",
				}, "\n"))
			},
			wantStatus: ManagedVolumeStatusRecovered,
			wantReason: ReasonTransparentHostPathRecovered,
			wantEvent:  "Normal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			opts := ObservationBundleOptions{Dir: dir}
			if tc.name == "blocked-image-pull" {
				opts.VolumeID = "pvc-blocked"
			}
			cluster, err := BuildObservationFromBundle(opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(cluster.ManagedVolumes) != 1 {
				t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
			}
			managed := cluster.ManagedVolumes[0]
			if managed.Status != tc.wantStatus || managed.ReasonCode != tc.wantReason {
				t.Fatalf("managed=%+v", managed)
			}
			reportDir := filepath.Join(dir, "report")
			if err := WriteObservationReportArtifacts(reportDir, cluster); err != nil {
				t.Fatal(err)
			}
			for _, artifact := range []string{
				ObservationReportHTMLArtifact,
				ObservationReportJSONArtifact,
				ObservationReportJSONLArtifact,
				ObservationOperatorSnapshotArtifact,
				ObservationReportTextArtifact,
			} {
				if _, err := os.Stat(filepath.Join(reportDir, artifact)); err != nil {
					t.Fatalf("missing report artifact %s: %v", artifact, err)
				}
			}
			explain := RenderObservationExplainText(cluster)
			if !strings.Contains(explain, "managed_volume_condition") {
				t.Fatalf("explain missing condition:\n%s", explain)
			}
			contract := ManagedVolumeOperatorContractFromProjection(managed)
			if len(contract.Events) == 0 || contract.Events[0].Type != tc.wantEvent {
				t.Fatalf("contract events=%+v", contract.Events)
			}
			for _, action := range contract.AllowedActions {
				if action.MutationAllowed {
					t.Fatalf("mutation allowed in replay contract: %+v", action)
				}
			}
		})
	}
}

func TestObservationBundle_DashboardReplayGate_FirstVolumeBlockedAndRecovery(t *testing.T) {
	cases := []struct {
		name       string
		build      func(t *testing.T, dir string)
		wantHTML   string
		wantJSON   string
		wantText   string
		wantStatus string
	}{
		{
			name: "first-volume",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{healthyObservationVolume()})
			},
			wantHTML:   "Managed Volumes",
			wantJSON:   `"reason_code": "first_volume_verified"`,
			wantText:   "managed_volume=pvc-healthy status=ready reason=first_volume_verified",
			wantStatus: ManagedVolumeStatusReady,
		},
		{
			name: "blocked-image-pull",
			build: func(t *testing.T, dir string) {
				mustWrite(t, filepath.Join(dir, "demo", KubeSystemPodsDeploysArtifact), `NAME READY STATUS RESTARTS AGE IP NODE
pod/sw-block-csi-node-n948t 0/2 Init:ErrImagePull 0 2m3s 192.168.1.184 m02
deployment.apps/sw-block-csi-controller 1/1 1 1 2m3s block-csi sw-block-csi:local`)
			},
			wantHTML:   ReasonCSINodeImagePullFailed,
			wantJSON:   `"reason_code": "csi_node_image_pull_failed"`,
			wantText:   "managed_volume=pvc-blocked status=blocked reason=csi_node_image_pull_failed",
			wantStatus: ManagedVolumeStatusBlocked,
		},
		{
			name: "status-endpoint-unreachable",
			build: func(t *testing.T, dir string) {
				writeStatusEndpointUnreachableInventory(t, dir)
			},
			wantHTML:   ReasonStatusEndpointUnreachable,
			wantJSON:   `"reason_code": "status_endpoint_unreachable"`,
			wantText:   "managed_volume=pvc-unreachable status=unknown reason=status_endpoint_unreachable",
			wantStatus: ManagedVolumeStatusUnknown,
		},
		{
			name: "stage2-recovery",
			build: func(t *testing.T, dir string) {
				writeProductClusterEvidence(t, dir, []VolumeEvidence{{
					VolumeID:          "pvc-stage2",
					Namespace:         "default",
					PVCName:           "demo-pvc",
					ReplicationFactor: 3,
					PrimaryReplica:    "r2",
					PublishTarget:     "192.168.1.184:3261",
					Replicas: []ReplicaEvidence{{
						ReplicaID:      "r2",
						KubernetesNode: "m02",
						Role:           "primary",
						Observed:       true,
						FrontendAddr:   "192.168.1.184:3261",
					}},
				}})
				mustWrite(t, filepath.Join(dir, "demo", PrimaryFailureRecoveryArtifact), strings.Join([]string{
					"promoted_replica=r2",
					"data_check_after_failover=mounted_workload_checksum_passed",
					"pod_recreate_used=false",
					"old_primary_stale_io_success_count=0",
					"transparent_failover_claimed=true",
				}, "\n"))
			},
			wantHTML:   ReasonTransparentHostPathRecovered,
			wantJSON:   `"reason_code": "transparent_host_path_recovered"`,
			wantText:   "managed_volume=pvc-stage2 status=recovered reason=transparent_host_path_recovered",
			wantStatus: ManagedVolumeStatusRecovered,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			opts := ObservationBundleOptions{Dir: dir}
			if tc.name == "blocked-image-pull" {
				opts.VolumeID = "pvc-blocked"
			}
			cluster, err := BuildObservationFromBundle(opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(cluster.ManagedVolumes) != 1 || cluster.ManagedVolumes[0].Status != tc.wantStatus {
				t.Fatalf("managed_volumes=%+v", cluster.ManagedVolumes)
			}

			server := httptest.NewServer(NewObservationDashboardHandler(cluster))
			defer server.Close()
			assertDashboardEndpointContains(t, server.URL+"/", tc.wantHTML)
			assertDashboardEndpointContains(t, server.URL+"/"+ObservationReportJSONArtifact, tc.wantJSON)
			assertDashboardEndpointContains(t, server.URL+"/"+ObservationOperatorSnapshotArtifact, `"read_only": true`)
			assertDashboardEndpointContains(t, server.URL+"/"+ObservationReportTextArtifact, tc.wantText)

			resp, err := http.Post(server.URL+"/", "application/json", strings.NewReader(`{"action":"repair"}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusMethodNotAllowed || !strings.Contains(string(body), "read-only dashboard") {
				t.Fatalf("mutation response status=%d body=%s", resp.StatusCode, body)
			}
		})
	}
}

func assertDashboardEndpointContains(t *testing.T, url, want string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", url, resp.StatusCode, body)
	}
	if !strings.Contains(string(body), want) {
		t.Fatalf("%s missing %q:\n%s", url, want, body)
	}
}

func writeProductClusterEvidence(t *testing.T, dir string, volumes []VolumeEvidence) {
	t.Helper()
	productDir := filepath.Join(dir, "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cluster := NewClusterEvidence(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = volumes
	raw, err := MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ClusterEvidenceArtifact), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeClusterEvidenceArtifact(t *testing.T, path string, cluster ClusterEvidence) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalObservationJSON(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustWriteInventory(t *testing.T, dir string, inventory VolumeInventory) {
	t.Helper()
	raw, err := MarshalObservationJSON(inventory)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, VolumeInventoryArtifact), string(raw))
}

func writeStatusEndpointUnreachableInventory(t *testing.T, dir string) {
	t.Helper()
	inventoryDir := filepath.Join(dir, "demo", "ops-inventory-status-unreachable")
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-unreachable",
			Namespace:         "default",
			PVCName:           "demo-pvc",
			PVName:            "pvc-unreachable",
			ReplicationFactor: 1,
			Replicas: []VolumeInventoryReplicaInput{{
				ReplicaID:            "r1",
				ServerID:             "node-r1",
				NodeName:             "m01",
				Protocol:             "iscsi",
				FrontendAddress:      "192.168.1.181:3260",
				StatusAddress:        "192.168.1.181:23260",
				Observed:             true,
				AuthorityRole:        "primary",
				Healthy:              true,
				FrontendPrimaryReady: true,
				ReplicationRole:      "none",
				Issues:               []string{"status_endpoint_unreachable=192.168.1.181:23260"},
			}},
		}},
	})
	mustWriteInventory(t, inventoryDir, inventory)
}

func findManagedVolumeOperatorAction(actions []ManagedVolumeOperatorAction, actionType string) *ManagedVolumeOperatorAction {
	for i := range actions {
		if actions[i].Type == actionType {
			return &actions[i]
		}
	}
	return nil
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
