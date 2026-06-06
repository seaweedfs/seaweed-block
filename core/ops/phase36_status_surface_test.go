package ops

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPhase36D5NodeStatusSurfacesAgree(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 5, 20, 0, 0, 0, time.UTC))
	cluster.Nodes = []NodeEvidence{{
		NodeName:       "m02",
		KubernetesNode: "m02",
		InternalIP:     "192.168.1.184",
		Schedulable:    true,
		Ready:          true,
		RequiredImages: []string{"sw-block:local"},
		MissingImages:  []string{"sw-block-csi:local"},
	}}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"node=m02 k8s=m02 status=blocked reason=image_missing_on_node ready=true schedulable=true missing_images=sw-block-csi:local",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.NodeCount != 1 || len(snapshot.Cluster.Nodes) != 1 {
		t.Fatalf("snapshot nodes=%+v", snapshot.Cluster)
	}
	node := snapshot.Cluster.Nodes[0]
	if node.Status != ManagedVolumeStatusBlocked || node.ReasonCode != ReasonImageMissingOnNode {
		t.Fatalf("node status=%+v", node)
	}

	dashboard := phase36DashboardSnapshot(t, cluster)
	if dashboard.Cluster.Nodes[0].Status != ManagedVolumeStatusBlocked ||
		dashboard.Cluster.Nodes[0].ReasonCode != ReasonImageMissingOnNode {
		t.Fatalf("dashboard node=%+v", dashboard.Cluster.Nodes[0])
	}
}

func TestPhase36D5CleanupRequiredSurfacesAgree(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 5, 20, 5, 0, 0, time.UTC))
	cluster.Cleanup = &CleanupEvidence{
		Status:            "failed",
		ISCSIResidueCount: 1,
		FailureCount:      1,
		EvidenceRef:       "cleanup-summary.txt",
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"cleanup_status=failed",
		"iscsi_residue_count=1",
		"cluster_condition=CleanupRequired status=True reason=cleanup_required severity=warning",
		"safe_next_step=observe.verify_cleanup mode=scripted mutation_allowed=false",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.Cleanup == nil || snapshot.Cluster.Cleanup.ISCSIResidueCount != 1 {
		t.Fatalf("snapshot cleanup=%+v", snapshot.Cluster.Cleanup)
	}
	if !phase36HasClusterCondition(snapshot, ConditionCleanupRequired, "True", ReasonCleanupRequired) {
		t.Fatalf("snapshot conditions=%+v", snapshot.Cluster.Conditions)
	}
	verifyStep := false
	for _, step := range snapshot.Cluster.SafeNextSteps {
		if step.Type == ManagedVolumeActionVerifyCleanup {
			verifyStep = true
			if step.Mode != ManagedVolumeActionModeScripted || step.MutationAllowed {
				t.Fatalf("cleanup step=%+v", step)
			}
		}
	}
	if !verifyStep {
		t.Fatalf("missing cleanup step: %+v", snapshot.Cluster.SafeNextSteps)
	}

	dashboard := phase36DashboardSnapshot(t, cluster)
	if dashboard.Cluster.Cleanup == nil || dashboard.Cluster.Cleanup.ISCSIResidueCount != 1 {
		t.Fatalf("dashboard cleanup=%+v", dashboard.Cluster.Cleanup)
	}
	if !phase36HasClusterCondition(dashboard, ConditionCleanupRequired, "True", ReasonCleanupRequired) {
		t.Fatalf("dashboard conditions=%+v", dashboard.Cluster.Conditions)
	}
}

func TestPhase36D5StaleEvidenceSurfacesDoNotClaimReady(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 5, 20, 10, 0, 0, time.UTC))
	cluster.Status = ObservationStatusUnavailable
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:      "pvc-unreachable",
		PVCName:       "status-unreachable-pvc",
		ProductStatus: ObservationStatusUnavailable,
		ProductReason: ReasonStatusEndpointUnreachable,
		EvidenceRefs:  []string{"diagnostics/status-endpoint-unreachable.txt"},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"managed_volume=pvc-unreachable status=unknown reason=status_endpoint_unreachable",
		"managed_volume_condition=Ready status=Unknown reason=status_endpoint_unreachable severity=warning",
		"managed_volume_condition=EvidenceStale status=True reason=status_endpoint_unreachable severity=warning",
		"cluster_condition=EvidenceStale status=True reason=evidence_stale severity=warning",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
	if strings.Contains(summary, "managed_volume_condition=Ready status=True") {
		t.Fatalf("stale summary must not claim Ready=True:\n%s", summary)
	}

	explain := RenderObservationExplainText(cluster)
	if !strings.Contains(explain, "managed_volume pvc-unreachable status=unknown reason=status_endpoint_unreachable") ||
		!strings.Contains(explain, "managed_volume_condition EvidenceStale status=True reason=status_endpoint_unreachable") {
		t.Fatalf("explain missing stale reason:\n%s", explain)
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.ReadyVolumeCount != 0 || snapshot.Cluster.StaleVolumeCount != 1 {
		t.Fatalf("snapshot cluster=%+v", snapshot.Cluster)
	}
	volume := snapshot.Volumes[0]
	if volume.Status.Status != ManagedVolumeStatusUnknown || volume.Status.ReasonCode != ReasonStatusEndpointUnreachable {
		t.Fatalf("snapshot volume=%+v", volume.Status)
	}
	if operatorContractHasCondition(volume, ConditionReady, "True", "") {
		t.Fatalf("snapshot stale volume must not be Ready=True: %+v", volume.Status.Conditions)
	}

	dashboard := phase36DashboardSnapshot(t, cluster)
	if dashboard.Cluster.ReadyVolumeCount != 0 || dashboard.Cluster.StaleVolumeCount != 1 {
		t.Fatalf("dashboard cluster=%+v", dashboard.Cluster)
	}
	if dashboard.Volumes[0].Status.Status != ManagedVolumeStatusUnknown ||
		dashboard.Volumes[0].Status.ReasonCode != ReasonStatusEndpointUnreachable {
		t.Fatalf("dashboard volume=%+v", dashboard.Volumes[0].Status)
	}
}

func phase36DashboardSnapshot(t *testing.T, cluster ClusterEvidence) OperatorFoundationSnapshot {
	t.Helper()
	recorder := httptest.NewRecorder()
	NewObservationDashboardHandler(cluster).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/operator-snapshot.json", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("dashboard code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot OperatorFoundationSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode dashboard snapshot: %v\n%s", err, recorder.Body.String())
	}
	if !snapshot.ReadOnly || snapshot.Mutation.MutationAllowed {
		t.Fatalf("dashboard mutation boundary=%+v", snapshot.Mutation)
	}
	return snapshot
}

func phase36HasClusterCondition(snapshot OperatorFoundationSnapshot, typ, status, reason string) bool {
	for _, condition := range snapshot.Cluster.Conditions {
		if condition.Type == typ && condition.Status == status && condition.Reason == reason {
			return true
		}
	}
	return false
}
