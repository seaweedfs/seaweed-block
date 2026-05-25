package ops

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPhase32D3ReadyStatusSurfacesAgree(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 25, 21, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-ready",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		PVName:            "pvc-ready",
		ReplicationFactor: 1,
		AckProfile:        "best-effort",
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m02",
		PublishTarget:     "127.0.0.1:3260",
		Epoch:             1,
		EndpointVersion:   1,
	}}
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-ready",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		PVName:            "pvc-ready",
		ReplicationFactor: 1,
		AckProfile:        "best-effort",
		PVC:               &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r1",
			PublishTarget:   "127.0.0.1:3260",
			Epoch:           1,
			EndpointVersion: 1,
		},
		CSIStages:    []CSIStageFact{{NodeName: "m02", Target: "127.0.0.1:3260"}},
		Workload:     &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
		EvidenceRefs: []string{"writer.log", "reader.log"},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"managed_volume=pvc-ready status=ready reason=first_volume_verified",
		"managed_volume_condition=Ready status=True reason=first_volume_verified severity=info",
		"read_only=true",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	html := RenderObservationReportHTML(cluster)
	for _, want := range []string{
		"pvc-ready",
		"first_volume_verified",
		"Ready=True/first_volume_verified",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("html missing %q", want)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if !snapshot.ReadOnly || snapshot.Mutation.MutationAllowed {
		t.Fatalf("snapshot mutation boundary=%+v", snapshot.Mutation)
	}
	if snapshot.Cluster.VolumeCount != 1 || snapshot.Cluster.ReadyVolumeCount != 1 || snapshot.Cluster.BlockedVolumeCount != 0 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	volume := snapshot.Volumes[0]
	if volume.Status.Status != ManagedVolumeStatusReady || volume.Status.ReasonCode != ReasonFirstVolumeVerified {
		t.Fatalf("volume status=%+v", volume.Status)
	}
	if !operatorContractHasCondition(volume, ConditionReady, "True", ReasonFirstVolumeVerified) {
		t.Fatalf("conditions=%+v", volume.Status.Conditions)
	}

	recorder := httptest.NewRecorder()
	NewObservationDashboardHandler(cluster).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/operator-snapshot.json", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"reason_code": "first_volume_verified"`) {
		t.Fatalf("dashboard operator snapshot code=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestPhase32D4BlockedStatusSurfacesAgree(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 25, 21, 10, 0, 0, time.UTC))
	cluster.Status = ObservationStatusBlocked
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-blocked",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		Status:            ObservationStatusBlocked,
		Reason:            ReasonCSINodeImagePullFailed,
		ReplicationFactor: 1,
	}}
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:  "pvc-blocked",
		Namespace: "default",
		PVCName:   "demo-pvc",
		PVC:       &PVCFact{Phase: "Bound"},
		KubernetesNodes: []KubernetesNodeFact{{
			NodeName:     "m02",
			Ready:        true,
			Schedulable:  true,
			CSINodeReady: false,
			Reason:       ReasonCSINodeImagePullFailed,
			Message:      "CSI node image pull failed",
		}},
		EvidenceRefs: []string{"blocked-bundle/demo/kube-system-pods-deploys.txt", "blocked-bundle/explain.txt"},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"managed_volume=pvc-blocked status=blocked reason=csi_node_image_pull_failed",
		"managed_volume_condition=Ready status=False reason=csi_node_image_pull_failed severity=warning",
		"managed_volume_condition=Blocked status=True reason=csi_node_image_pull_failed severity=warning",
		"managed_volume_action=safe_k8s.import_csi_image mode=dry_run side_effect=safe_k8s executor=installer_or_operator",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	html := RenderObservationReportHTML(cluster)
	for _, want := range []string{
		"pvc-blocked",
		"csi_node_image_pull_failed",
		"Blocked=True/csi_node_image_pull_failed",
		"safe_k8s.import_csi_image(dry_run)",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("html missing %q", want)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.VolumeCount != 1 || snapshot.Cluster.ReadyVolumeCount != 0 || snapshot.Cluster.BlockedVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	volume := snapshot.Volumes[0]
	if volume.Status.Status != ManagedVolumeStatusBlocked || volume.Status.ReasonCode != ReasonCSINodeImagePullFailed {
		t.Fatalf("volume status=%+v", volume.Status)
	}
	if !operatorContractHasCondition(volume, ConditionReady, "False", ReasonCSINodeImagePullFailed) ||
		!operatorContractHasCondition(volume, ConditionBlocked, "True", ReasonCSINodeImagePullFailed) {
		t.Fatalf("conditions=%+v", volume.Status.Conditions)
	}
	for _, action := range volume.AllowedActions {
		if action.MutationAllowed {
			t.Fatalf("blocked surface exposed mutating action: %+v", action)
		}
	}

	recorder := httptest.NewRecorder()
	NewObservationDashboardHandler(cluster).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/operator-snapshot.json", nil))
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK ||
		!strings.Contains(body, `"reason_code": "csi_node_image_pull_failed"`) ||
		!strings.Contains(body, `"mutation_allowed": false`) {
		t.Fatalf("dashboard operator snapshot code=%d body=%s", recorder.Code, body)
	}
}

func TestPhase32D5RestartPromotionStatusSurfacesAgree(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 25, 22, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-restart",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		PVName:            "pvc-restart",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		Status:            ObservationStatusOK,
		Reason:            "authority_persisted",
		PrimaryReplica:    "r2",
		PrimaryNode:       "m02",
		PublishTarget:     "192.168.1.184:3260",
		Epoch:             2,
		EndpointVersion:   1,
	}}
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          "pvc-restart",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		PVName:            "pvc-restart",
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		PVC:               &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			PublishTarget:         "192.168.1.184:3260",
			Epoch:                 2,
			EndpointVersion:       1,
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:          "r1",
			KubernetesNode:     "m01",
			Role:               "unavailable",
			Observed:           false,
			StalePrimaryFenced: true,
		}, {
			ReplicaID:            "r2",
			KubernetesNode:       "m02",
			Role:                 "primary",
			Observed:             true,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendAddr:         "192.168.1.184:3260",
		}, {
			ReplicaID:            "r3",
			KubernetesNode:       "tp01",
			Role:                 "replica",
			Observed:             true,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
		}},
		CSIStages: []CSIStageFact{{
			NodeName:        "m02",
			Target:          "192.168.1.184:3260",
			Epoch:           2,
			EndpointVersion: 1,
			Reattach:        true,
		}},
		Workload:     &WorkloadCheckFact{ReaderVerified: true},
		EvidenceRefs: []string{"restart-promotion-summary.txt", "reader-after-restart.log"},
	})}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"volume=pvc-restart status=ok pvc=default/demo-pvc primary=r2@m02 frontend=192.168.1.184:3260 rf=3 ack=sync-quorum",
		"managed_volume=pvc-restart status=recovered reason=csi_reattach_recovered",
		"managed_volume_condition=Ready status=True reason=csi_reattach_recovered severity=info",
		"managed_volume_condition=Recovered status=True reason=csi_reattach_recovered severity=info",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.VolumeCount != 1 || snapshot.Cluster.ReadyVolumeCount != 1 || snapshot.Cluster.BlockedVolumeCount != 0 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	volume := snapshot.Volumes[0]
	if volume.Status.Status != ManagedVolumeStatusRecovered || volume.Status.ReasonCode != ReasonCSIReattachRecovered {
		t.Fatalf("volume status=%+v", volume.Status)
	}
	if !operatorContractHasCondition(volume, ConditionRecovered, "True", ReasonCSIReattachRecovered) {
		t.Fatalf("conditions=%+v", volume.Status.Conditions)
	}
}

func TestPhase32D6MultiVolumeStatusSurfacesKeepVolumesIndependent(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 25, 22, 15, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{
		phase32VolumeEvidence("pvc-a", "app-a", "r1", "m01", "192.168.1.181:3260"),
		phase32VolumeEvidence("pvc-b", "app-b", "r2", "m02", "192.168.1.184:3260"),
		phase32VolumeEvidence("pvc-c", "app-c", "r3", "tp01", "192.168.1.188:3260"),
	}
	cluster.ManagedVolumes = []ManagedVolumeProjection{
		phase32ReadyManagedProjection("pvc-a", "app-a", "r1", "192.168.1.181:3260"),
		phase32ReadyManagedProjection("pvc-b", "app-b", "r2", "192.168.1.184:3260"),
		phase32ReadyManagedProjection("pvc-c", "app-c", "r3", "192.168.1.188:3260"),
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.VolumeCount != 3 || snapshot.Cluster.ReadyVolumeCount != 3 || snapshot.Cluster.BlockedVolumeCount != 0 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	seenVolumes := map[string]bool{}
	seenTargets := map[string]bool{}
	for _, volume := range snapshot.Volumes {
		if seenVolumes[volume.Status.VolumeID] {
			t.Fatalf("duplicate volume in snapshot: %+v", snapshot.Volumes)
		}
		seenVolumes[volume.Status.VolumeID] = true
		if volume.Status.Status != ManagedVolumeStatusReady || volume.Status.ReasonCode != ReasonFirstVolumeVerified {
			t.Fatalf("bad managed volume=%+v", volume.Status)
		}
		if !operatorContractHasCondition(volume, ConditionReady, "True", ReasonFirstVolumeVerified) {
			t.Fatalf("conditions=%+v", volume.Status.Conditions)
		}
	}
	for _, volume := range cluster.Volumes {
		if seenTargets[volume.PublishTarget] {
			t.Fatalf("duplicate publish target: %+v", cluster.Volumes)
		}
		seenTargets[volume.PublishTarget] = true
	}

	summary := RenderObservationReportSummary(cluster)
	for _, want := range []string{
		"volume=pvc-a status=ok pvc=default/app-a primary=r1@m01 frontend=192.168.1.181:3260 rf=3 ack=sync-quorum",
		"volume=pvc-b status=ok pvc=default/app-b primary=r2@m02 frontend=192.168.1.184:3260 rf=3 ack=sync-quorum",
		"volume=pvc-c status=ok pvc=default/app-c primary=r3@tp01 frontend=192.168.1.188:3260 rf=3 ack=sync-quorum",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func operatorContractHasCondition(contract ManagedVolumeOperatorContract, conditionType, status, reason string) bool {
	for _, condition := range contract.Status.Conditions {
		if condition.Type == conditionType && condition.Status == status && condition.Reason == reason {
			return true
		}
	}
	return false
}

func phase32VolumeEvidence(volumeID, pvc, primary, node, frontend string) VolumeEvidence {
	return VolumeEvidence{
		VolumeID:          volumeID,
		Namespace:         "default",
		PVCName:           pvc,
		PVName:            volumeID,
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		Status:            ObservationStatusOK,
		PrimaryReplica:    primary,
		PrimaryNode:       node,
		PublishTarget:     frontend,
		Epoch:             1,
		EndpointVersion:   1,
	}
}

func phase32ReadyManagedProjection(volumeID, pvc, primary, frontend string) ManagedVolumeProjection {
	return ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:          volumeID,
		Namespace:         "default",
		PVCName:           pvc,
		PVName:            volumeID,
		ReplicationFactor: 3,
		AckProfile:        "sync-quorum",
		PVC:               &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:  primary,
			PublishTarget:   frontend,
			Epoch:           1,
			EndpointVersion: 1,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            primary,
			Role:                 "primary",
			Observed:             true,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   44,
			FrontendAddr:         frontend,
		}},
		CSIStages:    []CSIStageFact{{Target: frontend}},
		Workload:     &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
		EvidenceRefs: []string{volumeID + "/writer.log", volumeID + "/reader.log"},
	})
}
