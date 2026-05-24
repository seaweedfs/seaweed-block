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

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"operator_snapshot=operator-snapshot.json",
		"volume=pvc-healthy status=ok pvc=default/mysql-data",
		"managed_volume=pvc-healthy status=ready reason=first_volume_verified",
		"managed_volume_condition=Ready status=True reason=first_volume_verified severity=info",
		"managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestObservationReportHTML_IncludesManagedVolumeConditions(t *testing.T) {
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
	}}

	html := RenderObservationReportHTML(cluster)
	for _, want := range []string{
		"Managed Volumes",
		"Managed Volume Conditions",
		"pvc-loopback",
		"publish_target_loopback_cross_node",
		"safe_k8s.reinstall_external_iscsi",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("html missing %q:\n%s", want, html)
		}
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
