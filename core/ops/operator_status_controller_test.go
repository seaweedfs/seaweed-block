package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestOperatorStatusReconcilerWritesStatusOnlyProjection(t *testing.T) {
	capturedAt := time.Date(2026, 6, 2, 22, 30, 0, 0, time.UTC)
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    capturedAt,
		Status:        ObservationStatusOK,
		Nodes: []NodeEvidence{{
			NodeName: "m02",
			Ready:    true,
		}},
		InstallDrift: &InstallDriftEvidence{
			Status:          InstallDriftStatusMismatch,
			ReasonCode:      ReasonInstallDriftMismatch,
			CurrentImage:    "sw-block:old",
			DesiredImage:    "sw-block:new",
			CurrentCSIImage: "sw-block-csi:old",
			DesiredCSIImage: "sw-block-csi:new",
			EvidenceRef:     "install-drift-summary.txt",
		},
		ManagedVolumes: []ManagedVolumeProjection{
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID: "pvc-ready",
				PVCName:  "demo-pvc",
				PVC:      &PVCFact{Phase: "Bound"},
				Authority: &AuthorityFact{
					PrimaryReplica: "r1",
					PublishTarget:  "192.168.1.184:3260",
				},
				Replicas: []ReplicaFact{{
					ReplicaID:      "r1",
					KubernetesNode: "m02",
					Role:           "primary",
					Observed:       true,
				}},
				CSIStages: []CSIStageFact{{NodeName: "m02", Target: "192.168.1.184:3260"}},
				Workload:  &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
			}),
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID:      "pvc-blocked",
				PVCName:       "blocked-pvc",
				ProductStatus: ObservationStatusBlocked,
				ProductReason: ReasonCSINodeImagePullFailed,
				EvidenceRefs:  []string{"diagnostics/events.txt"},
			}),
		},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}

	result, err := (OperatorStatusReconciler{
		Namespace:   "kube-system",
		ClusterName: "sw-block",
		Source:      source,
		Writer:      writer,
		EventSink:   events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.ClusterRef.Kind != SwBlockClusterKind || result.ClusterRef.Name != "sw-block" {
		t.Fatalf("cluster ref=%+v", result.ClusterRef)
	}
	if len(result.VolumeRefs) != 2 || len(writer.volumes) != 2 {
		t.Fatalf("volume refs=%+v writes=%+v", result.VolumeRefs, writer.volumes)
	}
	if writer.cluster.NodeCount != 1 || writer.cluster.VolumeCount != 2 || writer.cluster.ReadyVolumeCount != 1 || writer.cluster.BlockedVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if writer.cluster.MutationAllowed {
		t.Fatalf("cluster status must not allow mutation: %+v", writer.cluster)
	}
	if writer.cluster.InstallDrift == nil ||
		writer.cluster.InstallDrift.Status != InstallDriftStatusMismatch ||
		writer.cluster.InstallDrift.ReasonCode != ReasonInstallDriftMismatch ||
		writer.cluster.InstallDrift.CurrentImage != "sw-block:old" ||
		writer.cluster.InstallDrift.DesiredImage != "sw-block:new" ||
		writer.cluster.InstallDrift.CurrentCSIImage != "sw-block-csi:old" ||
		writer.cluster.InstallDrift.DesiredCSIImage != "sw-block-csi:new" {
		t.Fatalf("install drift status=%+v", writer.cluster.InstallDrift)
	}
	if len(writer.cluster.Nodes) != 1 || writer.cluster.Nodes[0].Name != "m02" {
		t.Fatalf("cluster nodes=%+v", writer.cluster.Nodes)
	}
	if got := writer.volumes[0].status.Status; got != ManagedVolumeStatusReady {
		t.Fatalf("ready volume status=%s", got)
	}
	if got := writer.volumes[1].status.ReasonCode; got != ReasonCSINodeImagePullFailed {
		t.Fatalf("blocked volume reason=%s", got)
	}
	for _, write := range writer.volumes {
		for _, action := range write.status.AllowedActions {
			if action.MutationAllowed {
				t.Fatalf("mutating action leaked into status: %+v", action)
			}
		}
	}
	rawStatus, err := json.Marshal(writer.volumes[0].status)
	if err != nil {
		t.Fatalf("marshal volume status: %v", err)
	}
	if !strings.Contains(string(rawStatus), `"mutationAllowed":false`) {
		t.Fatalf("CRD volume status must use camelCase mutationAllowed: %s", string(rawStatus))
	}
	if strings.Contains(string(rawStatus), "mutation_allowed") {
		t.Fatalf("CRD volume status must not use operator-snapshot snake_case fields: %s", string(rawStatus))
	}
	if events.countByReason(ReasonFirstVolumeVerified) == 0 {
		t.Fatalf("missing ready event: %+v", events.events)
	}
	if events.countByReason(ReasonCSINodeImagePullFailed) == 0 {
		t.Fatalf("missing blocked event: %+v", events.events)
	}
	for _, event := range events.events {
		if event.Reason == ReasonCSINodeImagePullFailed {
			if event.Type != "Warning" || event.InvolvedObject.Kind != SwBlockVolumeKind || event.InvolvedObject.Name != "blocked-pvc" {
				t.Fatalf("blocked event shape=%+v", event)
			}
		}
	}
	if result.EventCount != len(events.events) {
		t.Fatalf("event count=%d events=%d", result.EventCount, len(events.events))
	}
}

func TestOperatorStatusReconcilerWritesReturnedReplicaExecutorPreflight(t *testing.T) {
	capturedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    capturedAt,
		Status:        ObservationStatusRecovering,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID: "pvc-returned",
			PVCName:  "returned-pvc",
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
				StalePrimaryFenced:   true,
			}, {
				ReplicaID:            "r2",
				Observed:             true,
				Role:                 "primary",
				DurableFrontierKnown: true,
				DurableFrontierLSN:   52,
			}},
			EvidenceRefs: []string{"returned-replica-summary.txt"},
		})},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Namespace: "kube-system",
		Source:    source,
		Writer:    writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	status := writer.volumes[0].status
	if len(status.ExecutorPreflights) != 1 {
		t.Fatalf("executor preflights=%+v", status.ExecutorPreflights)
	}
	preflight := status.ExecutorPreflights[0]
	if preflight.ActionType != ManagedVolumeActionReintegrateReturned ||
		preflight.Decision != ReturnedReplicaExecutorPreflightReady ||
		preflight.Reason != ReturnedReplicaExecutorPreflightReasonSatisfied ||
		preflight.MutationAllowed {
		t.Fatalf("executor preflight=%+v", preflight)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	for _, want := range []string{`"executorPreflights"`, `"actionType"`, `"mutationAllowed"`, `"durableFrontierLsn"`, `"forbiddenMutationClass"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("CRD status missing %s: %s", want, string(raw))
		}
	}
	for _, forbidden := range []string{"executor_preflights", "action_type", "mutation_allowed", "durable_frontier_lsn"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("CRD status leaked snake_case %s: %s", forbidden, string(raw))
		}
	}
}

func TestOperatorStatusReconcilerProjectsNodeReadiness(t *testing.T) {
	heartbeat := time.Date(2026, 6, 5, 16, 0, 0, 0, time.UTC)
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 5, 16, 1, 0, 0, time.UTC),
		Status:        ObservationStatusOK,
		Nodes: []NodeEvidence{{
			NodeName:        "node-loss-r1",
			KubernetesNode:  "m01",
			InternalIP:      "192.168.1.181",
			Schedulable:     true,
			Ready:           true,
			LastHeartbeatAt: heartbeat,
			ReplicaCount:    2,
			RequiredImages:  []string{"sw-block:local", "sw-block-csi:local"},
			Conditions: []ObservationCondition{{
				Type:     ConditionReady,
				Status:   "True",
				Reason:   ReasonNodeReady,
				Severity: "info",
				Message:  "node is ready for Seaweed Block",
			}},
		}},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Source: source,
		Writer: writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writer.cluster.NodeCount != 1 || len(writer.cluster.Nodes) != 1 {
		t.Fatalf("cluster node status=%+v", writer.cluster)
	}
	node := writer.cluster.Nodes[0]
	if node.Name != "node-loss-r1" || node.KubernetesNode != "m01" || node.InternalIP != "192.168.1.181" {
		t.Fatalf("node identity=%+v", node)
	}
	if !node.Schedulable || !node.Ready || node.Status != ManagedVolumeStatusReady || node.ReasonCode != ReasonNodeReady {
		t.Fatalf("node readiness=%+v", node)
	}
	if !node.LastHeartbeatAt.Equal(heartbeat) || node.ReplicaCount != 2 || len(node.RequiredImages) != 2 {
		t.Fatalf("node details=%+v", node)
	}
	assertCondition(t, node.Conditions, ConditionReady, "True", ReasonNodeReady)
	rawStatus, err := json.Marshal(writer.cluster)
	if err != nil {
		t.Fatalf("marshal cluster status: %v", err)
	}
	raw := string(rawStatus)
	for _, want := range []string{`"kubernetesNode":"m01"`, `"internalIP":"192.168.1.181"`, `"lastHeartbeatAt":"2026-06-05T16:00:00Z"`, `"requiredImages":["sw-block:local","sw-block-csi:local"]`} {
		if !strings.Contains(raw, want) {
			t.Fatalf("cluster node status missing %s: %s", want, raw)
		}
	}
	for _, forbidden := range []string{"kubernetes_node", "internal_ip", "last_heartbeat_at", "required_images"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("CRD node status must not use observation snake_case field %q: %s", forbidden, raw)
		}
	}
}

func TestOperatorStatusReconcilerProjectsImageMissingNodeAsBlocked(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 5, 16, 5, 0, 0, time.UTC),
		Status:        ObservationStatusOK,
		Nodes: []NodeEvidence{{
			NodeName:       "m02",
			KubernetesNode: "m02",
			InternalIP:     "192.168.1.184",
			Schedulable:    true,
			Ready:          true,
			RequiredImages: []string{"ghcr.io/seaweedfs/seaweed-block:sha-test"},
			MissingImages:  []string{"ghcr.io/seaweedfs/seaweed-block:sha-test"},
			Conditions: []ObservationCondition{{
				Type:     ConditionBlocked,
				Status:   "True",
				Reason:   ReasonImageMissingOnNode,
				Severity: "warning",
				Message:  "required image is missing on node",
			}},
		}},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Source: source,
		Writer: writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(writer.cluster.Nodes) != 1 {
		t.Fatalf("cluster nodes=%+v", writer.cluster.Nodes)
	}
	node := writer.cluster.Nodes[0]
	if node.Status != ManagedVolumeStatusBlocked || node.ReasonCode != ReasonImageMissingOnNode {
		t.Fatalf("node status=%+v", node)
	}
	if len(node.MissingImages) != 1 || node.MissingImages[0] != "ghcr.io/seaweedfs/seaweed-block:sha-test" {
		t.Fatalf("missing images=%+v", node.MissingImages)
	}
	assertCondition(t, node.Conditions, ConditionReady, "False", ReasonImageMissingOnNode)
	assertCondition(t, node.Conditions, ConditionBlocked, "True", ReasonImageMissingOnNode)
	if writer.cluster.ReadyVolumeCount != 0 {
		t.Fatalf("node blocker must not synthesize ready volumes: %+v", writer.cluster)
	}
}

func TestPhase40D2NodeReadinessReplacesStaleReadyCondition(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC),
		Status:        ObservationStatusOK,
		Nodes: []NodeEvidence{{
			NodeName:    "m02",
			Schedulable: true,
			Ready:       true,
			MissingImages: []string{
				"sw-block-csi:local",
			},
			Conditions: []ObservationCondition{
				{Type: ConditionReady, Status: "True", Reason: ReasonNodeReady, Severity: "info"},
				{Type: ConditionReady, Status: "Unknown", Reason: ReasonNodeNotReady, Severity: "warning"},
			},
		}},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Source: source,
		Writer: writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(writer.cluster.Nodes) != 1 {
		t.Fatalf("cluster nodes=%+v", writer.cluster.Nodes)
	}
	node := writer.cluster.Nodes[0]
	assertCondition(t, node.Conditions, ConditionReady, "False", ReasonImageMissingOnNode)
	if got := countConditionsByType(node.Conditions, ConditionReady); got != 1 {
		t.Fatalf("Ready conditions=%d want 1: %+v", got, node.Conditions)
	}
}

func TestOperatorStatusReconcilerStaleEvidenceProjectsUnknownAndWarningEvent(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 2, 23, 0, 0, 0, time.UTC),
		Status:        ObservationStatusUnavailable,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:            "pvc-stale",
			PVCName:             "stale_pvc",
			EvidenceStale:       true,
			EvidenceStaleReason: ReasonEvidenceStale,
			EvidenceRefs:        []string{"product/unreachable.txt"},
		})},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}

	_, err := (OperatorStatusReconciler{
		Source:    source,
		Writer:    writer,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writer.cluster.StaleVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	volume := writer.volumes[0]
	if volume.ref.Name != "stale-pvc" {
		t.Fatalf("volume name=%q", volume.ref.Name)
	}
	if volume.status.Status != ManagedVolumeStatusUnknown || volume.status.ReasonCode != ReasonEvidenceStale {
		t.Fatalf("volume status=%+v", volume.status)
	}
	foundUnknownReady := false
	for _, condition := range volume.status.Conditions {
		if condition.Type == ConditionReady && condition.Status == "Unknown" && condition.Reason == ReasonEvidenceStale {
			foundUnknownReady = true
		}
	}
	if !foundUnknownReady {
		t.Fatalf("missing Ready=Unknown condition: %+v", volume.status.Conditions)
	}
	if events.countByReason(ReasonEvidenceStale) == 0 {
		t.Fatalf("missing EvidenceStale event: %+v", events.events)
	}
	for _, event := range events.events {
		if event.Reason == ReasonEvidenceStale && event.Type != "Warning" {
			t.Fatalf("EvidenceStale event type=%s want Warning", event.Type)
		}
	}
}

func TestOperatorStatusReconcilerStatusEndpointUnreachableIsUnknownNotBlocked(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC),
		Status:        ObservationStatusUnavailable,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:      "pvc-unreachable",
			PVCName:       "status-unreachable-pvc",
			ProductStatus: ObservationStatusUnavailable,
			ProductReason: ReasonStatusEndpointUnreachable,
			EvidenceRefs:  []string{"diagnostics/status-endpoint-unreachable.txt"},
		})},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}

	_, err := (OperatorStatusReconciler{
		Source:    source,
		Writer:    writer,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writer.cluster.StaleVolumeCount != 1 || writer.cluster.BlockedVolumeCount != 0 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	volume := writer.volumes[0]
	if volume.status.Status != ManagedVolumeStatusUnknown || volume.status.ReasonCode != ReasonStatusEndpointUnreachable {
		t.Fatalf("volume status=%+v", volume.status)
	}
	assertCondition(t, volume.status.Conditions, ConditionReady, "Unknown", ReasonStatusEndpointUnreachable)
	assertCondition(t, volume.status.Conditions, ConditionEvidenceStale, "True", ReasonStatusEndpointUnreachable)
	if condition := findObservationCondition(volume.status.Conditions, ConditionBlocked); condition != nil && condition.Status == "True" {
		t.Fatalf("pure unreachable status must not become Blocked=True: %+v", volume.status.Conditions)
	}
	if events.countByReason(ReasonStatusEndpointUnreachable) == 0 {
		t.Fatalf("missing status_endpoint_unreachable event: %+v", events.events)
	}
	for _, event := range events.events {
		if event.Reason == ReasonStatusEndpointUnreachable && event.Type != "Warning" {
			t.Fatalf("status endpoint event type=%s want Warning", event.Type)
		}
	}
}

func TestOperatorStatusReconcilerWALIntegrityFaultIsNeverReady(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 4, 2, 15, 0, 0, time.UTC),
		Status:        ObservationStatusBlocked,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:      "pvc-corrupt",
			PVCName:       "wal-corrupt-pvc",
			ProductStatus: ObservationStatusBlocked,
			ProductReason: ReasonWALIntegrityFault,
			EvidenceRefs:  []string{"status/report/cluster-evidence.json", "blockvolume.log"},
		})},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}

	_, err := (OperatorStatusReconciler{
		Source:    source,
		Writer:    writer,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writer.cluster.ReadyVolumeCount != 0 || writer.cluster.BlockedVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	volume := writer.volumes[0]
	if volume.status.Status == ManagedVolumeStatusReady {
		t.Fatalf("wal integrity fault must never be Ready: %+v", volume.status)
	}
	if volume.status.ReasonCode != ReasonWALIntegrityFault {
		t.Fatalf("volume status=%+v", volume.status)
	}
	assertCondition(t, volume.status.Conditions, ConditionReady, "False", ReasonWALIntegrityFault)
	assertCondition(t, volume.status.Conditions, ConditionBlocked, "True", ReasonWALIntegrityFault)
	if events.countByReason(ReasonWALIntegrityFault) == 0 {
		t.Fatalf("missing wal_integrity_fault event: %+v", events.events)
	}
}

func TestOperatorStatusReconcilerCleanupStatusUsesCRDFieldNames(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC),
		Status:        ObservationStatusOK,
		Cleanup: &CleanupEvidence{
			Status:                 "failed",
			KubernetesResidueCount: 1,
			ISCSIResidueCount:      2,
			MultipathResidueCount:  3,
			ProcessResidueCount:    4,
			HostPathResidueCount:   5,
			FailureCount:           6,
			FailedPhase:            "verify",
			ReasonCodes:            []string{"iscsi_node_records_present"},
			EvidenceRef:            "cleanup-summary.txt",
		},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Source: source,
		Writer: writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writer.cluster.Cleanup == nil || writer.cluster.Cleanup.ISCSIResidueCount != 2 {
		t.Fatalf("cleanup status=%+v", writer.cluster.Cleanup)
	}
	assertCondition(t, writer.cluster.Conditions, ConditionCleanupRequired, "True", "iscsi_node_records_present")
	foundCleanupStep := false
	for _, step := range writer.cluster.SafeNextSteps {
		if step.Type != ManagedVolumeActionVerifyCleanup {
			continue
		}
		foundCleanupStep = true
		if step.Mode != ManagedVolumeActionModeScripted || step.MutationAllowed {
			t.Fatalf("cleanup next step=%+v", step)
		}
		if !strings.Contains(step.Command, "verify-helm-cleanup.sh") || step.ReasonCode != "iscsi_node_records_present" {
			t.Fatalf("cleanup next step=%+v", step)
		}
	}
	if !foundCleanupStep {
		t.Fatalf("missing cleanup safe next step: %+v", writer.cluster.SafeNextSteps)
	}
	rawStatus, err := json.Marshal(writer.cluster)
	if err != nil {
		t.Fatalf("marshal cluster status: %v", err)
	}
	raw := string(rawStatus)
	for _, want := range []string{`"k8sResidueCount":1`, `"iscsiResidueCount":2`, `"hostPathResidueCount":5`, `"evidenceRef":"cleanup-summary.txt"`} {
		if !strings.Contains(raw, want) {
			t.Fatalf("cluster cleanup status missing %s: %s", want, raw)
		}
	}
	for _, forbidden := range []string{"k8s_residue_count", "iscsi_residue_count", "hostpath_residue_count", "evidence_ref"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("CRD cleanup status must not use report snake_case field %q: %s", forbidden, raw)
		}
	}
}

func TestOperatorStatusReconcilerProjectsCleanCleanupCondition(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 5, 12, 15, 0, 0, time.UTC),
		Status:        ObservationStatusOK,
		Cleanup: &CleanupEvidence{
			Status:      ObservationStatusOK,
			EvidenceRef: "cleanup-summary.txt",
		},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Source: source,
		Writer: writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	assertCondition(t, writer.cluster.Conditions, ConditionCleanupRequired, "False", ReasonCleanupVerified)
	if len(writer.cluster.SafeNextSteps) != 0 {
		t.Fatalf("clean cleanup evidence must not suggest next steps: %+v", writer.cluster.SafeNextSteps)
	}
}

func TestOperatorStatusReconcilerProjectsSupportBundlePointers(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 5, 18, 0, 0, 0, time.UTC),
		Status:        ObservationStatusBlocked,
		Volumes: []VolumeEvidence{{
			VolumeID:          "pvc-blocked",
			Namespace:         "default",
			PVCName:           "blocked-pvc",
			Status:            ObservationStatusBlocked,
			Reason:            ReasonCSINodeImagePullFailed,
			SupportBundleHint: "support/bundle",
			Replicas: []ReplicaEvidence{{
				ReplicaID:         "r1",
				SupportBundlePath: "support/bundle/volumes/pvc-blocked/r1",
			}},
		}},
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:      "pvc-blocked",
			PVCName:       "blocked-pvc",
			ProductStatus: ObservationStatusBlocked,
			ProductReason: ReasonCSINodeImagePullFailed,
			EvidenceRefs:  []string{"support/bundle/replayed-report/summary.txt"},
		})},
	}}
	writer := &fakeOperatorStatusWriter{}

	_, err := (OperatorStatusReconciler{
		Source: source,
		Writer: writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, want := range []string{
		"support/bundle",
		"support/bundle/volumes/pvc-blocked/r1",
	} {
		if !stringSliceContains(writer.cluster.SupportBundleRefs, want) {
			t.Fatalf("cluster support refs missing %q: %+v", want, writer.cluster.SupportBundleRefs)
		}
	}
	if len(writer.cluster.SafeNextSteps) == 0 {
		t.Fatalf("missing safe next steps: %+v", writer.cluster)
	}
	step := writer.cluster.SafeNextSteps[0]
	if step.Type != ManagedVolumeActionCollectBundle || step.Mode != ManagedVolumeActionModeReadOnly || step.MutationAllowed {
		t.Fatalf("support next step=%+v", step)
	}
	if step.Command == "" || !strings.Contains(step.Command, "collect-helm-support-bundle.sh") {
		t.Fatalf("support next step command=%+v", step)
	}
}

func TestOperatorStatusReconcilerEventFailureDoesNotBlockLaterStatusWrites(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 4, 1, 30, 0, 0, time.UTC),
		ManagedVolumes: []ManagedVolumeProjection{
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID:      "pvc-blocked",
				PVCName:       "blocked-pvc",
				ProductStatus: ObservationStatusBlocked,
				ProductReason: ReasonCSINodeImagePullFailed,
				EvidenceRefs:  []string{"blocked/events.txt"},
			}),
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID: "pvc-ready",
				PVCName:  "ready-pvc",
				PVC:      &PVCFact{Phase: "Bound"},
				Authority: &AuthorityFact{
					PrimaryReplica: "r1",
					PublishTarget:  "192.168.1.184:3260",
				},
				Replicas: []ReplicaFact{{
					ReplicaID:      "r1",
					KubernetesNode: "m02",
					Role:           "primary",
					Observed:       true,
				}},
				CSIStages: []CSIStageFact{{NodeName: "m02", Target: "192.168.1.184:3260"}},
				Workload:  &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
			}),
		},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &failingOperatorEventSink{}

	result, err := (OperatorStatusReconciler{
		Namespace: "kube-system",
		Source:    source,
		Writer:    writer,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("event failure must not abort reconcile: %v", err)
	}
	if len(writer.volumes) != 2 {
		t.Fatalf("status writes=%+v", writer.volumes)
	}
	if writer.volumes[0].status.Status != ManagedVolumeStatusBlocked || writer.volumes[1].status.Status != ManagedVolumeStatusReady {
		t.Fatalf("volume statuses=%+v", writer.volumes)
	}
	if events.count == 0 {
		t.Fatalf("expected event attempts")
	}
	if result.EventCount != 0 {
		t.Fatalf("successful event count=%d want 0", result.EventCount)
	}
}

func TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 10, 12, 10, 0, 0, time.UTC),
		Status:        ObservationStatusBlocked,
		ManagedVolumes: []ManagedVolumeProjection{{
			VolumeID:   "pvc-held",
			PVCName:    "held-pvc",
			Status:     ManagedVolumeStatusBlocked,
			ReasonCode: "iscsi_node_records_present",
			Conditions: []ObservationCondition{{
				Type:     ConditionCleanupRequired,
				Status:   "True",
				Reason:   "iscsi_node_records_present",
				Severity: "warning",
			}},
			DeleteSafety: &SwBlockVolumeDeleteSafetyDecision{
				ActionType:              SwBlockVolumeDeleteActionReleaseFinalizer,
				Decision:                ManagedVolumeActionDecisionRejected,
				State:                   DeleteSafetyStateBlocked,
				Reason:                  "iscsi_node_records_present",
				FinalizerReleaseAllowed: false,
				SafeNextAction:          ManagedVolumeActionVerifyCleanup,
				EvidenceRefs:            []string{"cleanup-summary.txt"},
			},
		}},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}
	result, err := (OperatorStatusReconciler{
		Namespace:   "kube-system",
		ClusterName: "sw-block",
		Source:      source,
		Writer:      writer,
		EventSink:   events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 {
		t.Fatalf("finalizer patches=%d", result.FinalizerPatchCount)
	}
	if len(writer.volumes) != 1 || writer.volumes[0].status.DeleteSafety == nil {
		t.Fatalf("volume status writes=%+v", writer.volumes)
	}
	if writer.volumes[0].status.DeleteSafety.State != DeleteSafetyStateBlocked ||
		writer.volumes[0].status.DeleteSafety.Decision != ManagedVolumeActionDecisionRejected {
		t.Fatalf("delete safety status=%+v", writer.volumes[0].status.DeleteSafety)
	}
	action := findCRDAction(writer.volumes[0].status.AllowedActions, SwBlockVolumeDeleteActionReleaseFinalizer)
	if action == nil ||
		action.Mode != ManagedVolumeActionModeDryRun ||
		action.OwnerExecutor != "lifecycle_owner" ||
		action.MutationAllowed ||
		action.Decision != ManagedVolumeActionDecisionRejected {
		t.Fatalf("lifecycle-owner dry-run action=%+v", action)
	}
	if events.countByReason(ReasonDeleteFinalizerAdded) != 0 ||
		events.countByReason(ReasonDeleteFinalizerReleased) != 0 {
		t.Fatalf("operator-status must not emit finalizer mutation events: %+v", events.events)
	}
}

func findCRDAction(actions []SwBlockVolumeCRDAction, typ string) *SwBlockVolumeCRDAction {
	for i := range actions {
		if actions[i].Type == typ {
			return &actions[i]
		}
	}
	return nil
}

func TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC),
		Status:        ObservationStatusBlocked,
		ManagedVolumes: []ManagedVolumeProjection{
			{
				VolumeID:   "pvc-a",
				PVCName:    "delete-a",
				Status:     ManagedVolumeStatusBlocked,
				ReasonCode: "iscsi_node_records_present",
				Conditions: []ObservationCondition{{
					Type:     ConditionCleanupRequired,
					Status:   "True",
					Reason:   "iscsi_node_records_present",
					Severity: "warning",
				}},
				DeleteSafety: &SwBlockVolumeDeleteSafetyDecision{
					ActionType:              SwBlockVolumeDeleteActionReleaseFinalizer,
					Decision:                ManagedVolumeActionDecisionRejected,
					State:                   DeleteSafetyStateBlocked,
					Reason:                  "iscsi_node_records_present",
					FinalizerReleaseAllowed: false,
					SafeNextAction:          ManagedVolumeActionVerifyCleanup,
					EvidenceRefs:            []string{"cleanup-a.txt"},
				},
			},
			{
				VolumeID:   "pvc-b",
				PVCName:    "healthy-b",
				Status:     ManagedVolumeStatusReady,
				ReasonCode: ReasonFirstVolumeVerified,
				Conditions: []ObservationCondition{{
					Type:     ConditionReady,
					Status:   "True",
					Reason:   ReasonFirstVolumeVerified,
					Severity: "info",
				}},
			},
			{
				VolumeID:   "pvc-c",
				PVCName:    "clean-c",
				Status:     ManagedVolumeStatusReady,
				ReasonCode: ReasonFirstVolumeVerified,
				DeleteSafety: &SwBlockVolumeDeleteSafetyDecision{
					ActionType:              SwBlockVolumeDeleteActionReleaseFinalizer,
					Decision:                ManagedVolumeActionDecisionAllowed,
					State:                   DeleteSafetyStateReleasable,
					Reason:                  ReasonDeleteFinalizerReleasable,
					FinalizerReleaseAllowed: true,
					EvidenceRefs:            []string{"cleanup-c.txt"},
				},
			},
			{
				VolumeID:   "pvc-d",
				PVCName:    "stale-d",
				Status:     ManagedVolumeStatusReady,
				ReasonCode: ReasonFirstVolumeVerified,
				DeleteSafety: &SwBlockVolumeDeleteSafetyDecision{
					ActionType:              SwBlockVolumeDeleteActionReleaseFinalizer,
					Decision:                ManagedVolumeActionDecisionUnknown,
					State:                   DeleteSafetyStateRequested,
					Reason:                  ReasonCleanupEvidenceStale,
					FinalizerReleaseAllowed: false,
					MissingFacts:            []string{"cleanup.freshness"},
					EvidenceRefs:            []string{"cleanup-d.txt"},
				},
			},
		},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}
	result, err := (OperatorStatusReconciler{
		Namespace:   "kube-system",
		ClusterName: "sw-block",
		Source:      source,
		Writer:      writer,
		EventSink:   events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 {
		t.Fatalf("finalizer patches=%d", result.FinalizerPatchCount)
	}
	if len(writer.volumes) != 4 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	byName := map[string]SwBlockVolumeCRDStatus{}
	for _, record := range writer.volumes {
		byName[record.ref.Name] = record.status
	}
	if got := byName["delete-a"]; got.Status != ManagedVolumeStatusBlocked ||
		got.ReasonCode != "iscsi_node_records_present" ||
		got.DeleteSafety == nil ||
		got.DeleteSafety.State != DeleteSafetyStateBlocked {
		t.Fatalf("volume A status=%+v", got)
	}
	if got := byName["healthy-b"]; got.Status != ManagedVolumeStatusReady ||
		got.ReasonCode != ReasonFirstVolumeVerified ||
		got.DeleteSafety != nil {
		t.Fatalf("volume B contaminated status=%+v", got)
	}
	if got := byName["clean-c"]; got.Status != ManagedVolumeStatusReady ||
		got.ReasonCode != ReasonFirstVolumeVerified ||
		got.DeleteSafety == nil ||
		got.DeleteSafety.State != DeleteSafetyStateReleasable ||
		got.DeleteSafety.Decision != ManagedVolumeActionDecisionAllowed {
		t.Fatalf("volume C status=%+v", got)
	}
	if got := byName["stale-d"]; got.Status != ManagedVolumeStatusReady ||
		got.ReasonCode != ReasonFirstVolumeVerified ||
		got.DeleteSafety == nil ||
		got.DeleteSafety.State != DeleteSafetyStateRequested ||
		got.DeleteSafety.Decision != ManagedVolumeActionDecisionUnknown ||
		got.DeleteSafety.Reason != ReasonCleanupEvidenceStale {
		t.Fatalf("volume D status=%+v", got)
	}
	for name, wantDecision := range map[string]string{
		"delete-a": ManagedVolumeActionDecisionRejected,
		"clean-c":  ManagedVolumeActionDecisionAllowed,
		"stale-d":  ManagedVolumeActionDecisionUnknown,
	} {
		action := findCRDAction(byName[name].AllowedActions, SwBlockVolumeDeleteActionReleaseFinalizer)
		if action == nil ||
			action.OwnerExecutor != "lifecycle_owner" ||
			action.Mode != ManagedVolumeActionModeDryRun ||
			action.MutationAllowed ||
			action.Decision != wantDecision {
			t.Fatalf("volume %s lifecycle-owner action=%+v want decision=%s", name, action, wantDecision)
		}
	}
	for _, event := range events.events {
		if event.Reason == ReasonDeleteFinalizerAdded || event.Reason == ReasonDeleteFinalizerReleased {
			t.Fatalf("finalizer mutation event emitted: %+v", event)
		}
	}
}

func TestPhase44OperatorStatusReconcilerProjectsLiveDeletingVolumeHold(t *testing.T) {
	deletingAt := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    deletingAt,
		Status:        ObservationStatusOK,
		ManagedVolumes: []ManagedVolumeProjection{{
			VolumeID:   "pvc-live",
			PVCName:    "live-pvc",
			Status:     ManagedVolumeStatusReady,
			ReasonCode: ReasonFirstVolumeVerified,
			Conditions: []ObservationCondition{{
				Type:     ConditionReady,
				Status:   "True",
				Reason:   ReasonFirstVolumeVerified,
				Severity: "info",
			}},
		}},
	}}
	writer := &fakeOperatorStatusWriter{}
	result, err := (OperatorStatusReconciler{
		Namespace: "kube-system",
		Source:    source,
		Writer:    writer,
		Volumes: fakeOperatorSwBlockVolumeSource{volumes: []SwBlockVolumeObject{{
			Ref: OperatorObjectRef{
				Namespace: "kube-system",
				Name:      "live-pvc",
			},
			Finalizers:        []string{SwBlockVolumeFinalizerName},
			DeletionTimestamp: &deletingAt,
			Spec:              SwBlockVolumeSpec{PVCName: "live-pvc"},
			Status: SwBlockVolumeCRDStatus{
				VolumeID: "pvc-live",
				PVCName:  "live-pvc",
			},
		}}},
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 {
		t.Fatalf("operator-status must not patch finalizers: %d", result.FinalizerPatchCount)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	got := writer.volumes[0].status
	if got.Status != ManagedVolumeStatusUnknown || got.ReasonCode != ReasonCleanupEvidenceMissing {
		t.Fatalf("deleting volume with missing evidence must not stay ready: %+v", got)
	}
	if got.DeleteSafety == nil ||
		got.DeleteSafety.Decision != ManagedVolumeActionDecisionUnknown ||
		got.DeleteSafety.State != DeleteSafetyStateRequested ||
		got.DeleteSafety.Reason != ReasonCleanupEvidenceMissing {
		t.Fatalf("delete safety=%+v", got.DeleteSafety)
	}
	action := findCRDAction(got.AllowedActions, SwBlockVolumeDeleteActionReleaseFinalizer)
	if action == nil ||
		action.Decision != ManagedVolumeActionDecisionUnknown ||
		action.MutationAllowed ||
		action.Mode != ManagedVolumeActionModeDryRun {
		t.Fatalf("release action=%+v", action)
	}
}

func TestPhase44OperatorStatusReconcilerProjectsLiveDeletingVolumeRelease(t *testing.T) {
	observedAt := time.Date(2026, 6, 16, 10, 5, 0, 0, time.UTC)
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    observedAt,
		Status:        ObservationStatusOK,
		Cleanup: &CleanupEvidence{
			Status:      ObservationStatusOK,
			EvidenceRef: "cleanup-summary.txt",
			ObservedAt:  observedAt,
		},
		ManagedVolumes: []ManagedVolumeProjection{{
			VolumeID:   "pvc-clean",
			PVCName:    "clean-pvc",
			Status:     ManagedVolumeStatusReady,
			ReasonCode: ReasonFirstVolumeVerified,
		}},
	}}
	writer := &fakeOperatorStatusWriter{}
	_, err := (OperatorStatusReconciler{
		Namespace: "kube-system",
		Source:    source,
		Writer:    writer,
		Volumes: fakeOperatorSwBlockVolumeSource{volumes: []SwBlockVolumeObject{{
			Ref: OperatorObjectRef{
				Namespace: "kube-system",
				Name:      "clean-pvc",
			},
			Finalizers:        []string{"example.com/foreign", SwBlockVolumeFinalizerName},
			DeletionTimestamp: &observedAt,
			Spec:              SwBlockVolumeSpec{PVCName: "clean-pvc"},
			Status: SwBlockVolumeCRDStatus{
				VolumeID: "pvc-clean",
				PVCName:  "clean-pvc",
			},
		}}},
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := writer.volumes[0].status
	if got.DeleteSafety == nil ||
		!got.DeleteSafety.FinalizerReleaseAllowed ||
		got.DeleteSafety.Decision != ManagedVolumeActionDecisionAllowed ||
		got.DeleteSafety.State != DeleteSafetyStateReleasable {
		t.Fatalf("delete safety=%+v", got.DeleteSafety)
	}
	action := findCRDAction(got.AllowedActions, SwBlockVolumeDeleteActionReleaseFinalizer)
	if action == nil || action.Decision != ManagedVolumeActionDecisionAllowed || action.MutationAllowed {
		t.Fatalf("release action=%+v", action)
	}
}

func TestPhase44OperatorStatusReconcilerSkipsDeletedCRDuringStatusPatch(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 16, 10, 15, 0, 0, time.UTC),
		Status:        ObservationStatusOK,
		ManagedVolumes: []ManagedVolumeProjection{
			{
				VolumeID:   "pvc-gone",
				PVCName:    "gone-pvc",
				Status:     ManagedVolumeStatusReady,
				ReasonCode: ReasonFirstVolumeVerified,
			},
			{
				VolumeID:   "pvc-live",
				PVCName:    "live-pvc",
				Status:     ManagedVolumeStatusReady,
				ReasonCode: ReasonFirstVolumeVerified,
			},
		},
	}}
	writer := &notFoundOnceOperatorStatusWriter{missingName: "gone-pvc"}
	result, err := (OperatorStatusReconciler{
		Namespace: "kube-system",
		Source:    source,
		Writer:    writer,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("404 on deleted volume CR must not abort reconcile: %v", err)
	}
	if len(result.VolumeRefs) != 1 || result.VolumeRefs[0].Name != "live-pvc" {
		t.Fatalf("volume refs=%+v", result.VolumeRefs)
	}
	if len(writer.volumes) != 1 || writer.volumes[0].ref.Name != "live-pvc" {
		t.Fatalf("written volumes=%+v", writer.volumes)
	}
}

func TestPhase40D2VolumeStatusClearsStaleDeleteSafety(t *testing.T) {
	status := SwBlockVolumeCRDStatus{
		VolumeID:     "pvc-ready",
		PVCName:      "ready-pvc",
		Status:       ManagedVolumeStatusReady,
		ReasonCode:   ReasonFirstVolumeVerified,
		DeleteSafety: nil,
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	if !strings.Contains(string(raw), `"deleteSafety":null`) {
		t.Fatalf("status patch must clear stale deleteSafety with null: %s", string(raw))
	}
}

func TestSwBlockVolumeObjectNameIsDNSLabelLike(t *testing.T) {
	cases := map[string]string{
		"Demo_PVC":      "demo-pvc",
		"pvc:1234.5678": "pvc-1234-5678",
		"---":           "unknown-volume",
		"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnop": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk",
	}
	for in, want := range cases {
		got := SwBlockVolumeObjectName(ManagedVolumeOperatorStatus{PVCName: in})
		if got != want {
			t.Fatalf("name %q=%q want %q", in, got, want)
		}
	}
}

type fakeOperatorStatusSource struct {
	cluster ClusterEvidence
}

func (f fakeOperatorStatusSource) ClusterEvidence(context.Context) (ClusterEvidence, error) {
	return f.cluster, nil
}

type fakeOperatorSwBlockVolumeSource struct {
	volumes []SwBlockVolumeObject
}

func (f fakeOperatorSwBlockVolumeSource) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}

type fakeOperatorStatusWriter struct {
	cluster OperatorStatusWriterClusterRecord
	volumes []OperatorStatusWriterVolumeRecord
}

type OperatorStatusWriterClusterRecord = SwBlockClusterCRDStatus

type OperatorStatusWriterVolumeRecord struct {
	ref    OperatorObjectRef
	status SwBlockVolumeCRDStatus
}

func (f *fakeOperatorStatusWriter) WriteClusterStatus(_ context.Context, _ OperatorObjectRef, status SwBlockClusterCRDStatus) error {
	f.cluster = status
	return nil
}

func (f *fakeOperatorStatusWriter) WriteVolumeStatus(_ context.Context, ref OperatorObjectRef, status SwBlockVolumeCRDStatus) error {
	f.volumes = append(f.volumes, OperatorStatusWriterVolumeRecord{ref: ref, status: status})
	return nil
}

type notFoundOnceOperatorStatusWriter struct {
	fakeOperatorStatusWriter
	missingName string
}

func (f *notFoundOnceOperatorStatusWriter) WriteVolumeStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockVolumeCRDStatus) error {
	if ref.Name == f.missingName {
		return fmt.Errorf("patch swblockvolumes/%s status failed: http 404 not found", ref.Name)
	}
	return f.fakeOperatorStatusWriter.WriteVolumeStatus(ctx, ref, status)
}

type fakeOperatorEventSink struct {
	events []OperatorKubernetesEvent
}

func (f *fakeOperatorEventSink) EmitEvent(_ context.Context, event OperatorKubernetesEvent) error {
	f.events = append(f.events, event)
	return nil
}

type failingOperatorEventSink struct {
	count int
}

func (f *failingOperatorEventSink) EmitEvent(context.Context, OperatorKubernetesEvent) error {
	f.count++
	return errOperatorEventSinkFailed{}
}

type errOperatorEventSinkFailed struct{}

func (errOperatorEventSinkFailed) Error() string {
	return "event sink failed"
}

func (f *fakeOperatorEventSink) countByReason(reason string) int {
	count := 0
	for _, event := range f.events {
		if event.Reason == reason {
			count++
		}
	}
	return count
}

func assertCondition(t *testing.T, conditions []ObservationCondition, typ, status, reason string) {
	t.Helper()
	condition := findObservationCondition(conditions, typ)
	if condition == nil {
		t.Fatalf("missing %s condition: %+v", typ, conditions)
	}
	if condition.Status != status || condition.Reason != reason {
		t.Fatalf("%s condition=%+v want status=%s reason=%s", typ, *condition, status, reason)
	}
}

func countConditionsByType(conditions []ObservationCondition, typ string) int {
	count := 0
	for _, condition := range conditions {
		if condition.Type == typ {
			count++
		}
	}
	return count
}
