package ops

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestObservation_RenderHealthyVolumeTextAndJSON(t *testing.T) {
	volume := healthyObservationVolume()

	text := RenderVolumeEvidenceText(volume)
	for _, want := range []string{
		"volume pvc-healthy status=ok rf=3 ack=sync-quorum",
		"primary r1 on m01 frontend=192.168.1.181:3260",
		"r2 m02 unknown replica_ready durable_lsn=44 candidate_ready=true",
		"next action: none",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}

	raw, err := MarshalObservationJSON(volume)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	var decoded VolumeEvidence
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode json: %v\n%s", err, raw)
	}
	if decoded.Status != ObservationStatusOK || decoded.Replicas[1].CandidateReadyReason != PromotionReasonReady {
		t.Fatalf("decoded evidence lost stable fields: %+v", decoded)
	}
}

func TestObservation_RenderRecoveringNodeLossStory(t *testing.T) {
	volume := VolumeEvidence{
		VolumeID:          "pvc-recovering",
		Namespace:         "default",
		PVCName:           "mysql-data",
		ReplicationFactor: 3,
		AckProfile:        PromotionAckProfileSyncQuorum,
		DesiredReplicas:   3,
		ObservedReplicas:  3,
		Status:            ObservationStatusRecovering,
		Reason:            ReasonPrimaryNodeLost,
		PrimaryReplica:    "r2",
		PrimaryNode:       "m02",
		PublishTarget:     "192.168.1.184:3260",
		Epoch:             2,
		EndpointVersion:   1,
		Replicas: []ReplicaEvidence{
			{
				ReplicaID:          "r1",
				KubernetesNode:     "m01",
				Role:               "unavailable",
				ReplicationRole:    "unavailable",
				StalePrimaryFenced: true,
				Conditions: []ObservationCondition{{
					Type:     "StalePrimary",
					Status:   "true",
					Reason:   ReasonStalePrimaryFenced,
					Severity: "info",
					Message:  "old primary endpoint is not ready",
				}},
			},
			{
				ReplicaID:            "r2",
				KubernetesNode:       "m02",
				Role:                 "primary",
				ReplicationRole:      "none",
				DurableFrontierKnown: true,
				DurableFrontierLSN:   52,
			},
		},
		Conditions: []ObservationCondition{{
			Type:     "Recovery",
			Status:   "true",
			Reason:   ReasonPrimaryNodeLost,
			Severity: "info",
			Message:  "CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260",
		}},
		NextActions: []string{"wait for app pod readiness, then collect support bundle if stuck"},
	}

	text := RenderVolumeEvidenceText(volume)
	for _, want := range []string{
		"volume pvc-recovering status=recovering rf=3 ack=sync-quorum reason=primary_node_lost",
		"primary r2 on m02 frontend=192.168.1.184:3260",
		"r1 m01 unavailable unavailable stale_primary_fenced=true",
		"condition Recovery severity=info reason=primary_node_lost CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260",
		"next action: wait for app pod readiness, then collect support bundle if stuck",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
}

func TestObservation_RenderBlockedImagePullStory(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 16, 16, 3, 6, 0, time.UTC))
	cluster.Status = ObservationStatusBlocked
	cluster.Nodes = []NodeEvidence{{
		NodeName:       "m02",
		KubernetesNode: "m02",
		InternalIP:     "192.168.1.184",
		Schedulable:    true,
		Ready:          true,
		RequiredImages: []string{"sw-block:local", "sw-block-csi:local"},
		MissingImages:  []string{"sw-block-csi:local"},
		Conditions: []ObservationCondition{{
			Type:     "ImageInventory",
			Status:   "false",
			Reason:   ReasonImageMissingOnNode,
			Severity: "error",
			Message:  "node m02 missing image sw-block-csi:local",
		}},
	}}
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:          "pvc-blocked",
		Namespace:         "default",
		PVCName:           "mysql-data",
		ReplicationFactor: 3,
		Status:            ObservationStatusBlocked,
		Reason:            ReasonCSINodeImagePullFailed,
		Conditions: []ObservationCondition{{
			Type:     "Attach",
			Status:   "false",
			Reason:   ReasonCSINodeImagePullFailed,
			Severity: "error",
			Message:  "pod kube-system/sw-block-csi-node-abc waiting=ImagePullBackOff",
		}},
		NextActions: []string{"import the image to m02 or use a registry reachable by all nodes"},
	}}

	text := RenderClusterEvidenceText(cluster)
	for _, want := range []string{
		"cluster status=blocked volumes=1 nodes=1",
		"volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed",
		"condition Attach severity=error reason=csi_node_image_pull_failed pod kube-system/sw-block-csi-node-abc waiting=ImagePullBackOff",
		"next action: import the image to m02 or use a registry reachable by all nodes",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}
}

func TestObservation_RenderEventsJSONLStableOrder(t *testing.T) {
	events := []ClusterEvent{
		{
			EventID:   "2",
			EventTime: time.Date(2026, 5, 16, 16, 3, 8, 0, time.UTC),
			Type:      "authority_published",
			Severity:  "info",
			VolumeID:  "pvc-a",
			ReplicaID: "r2",
			Reason:    ReasonCandidateCoversRequiredFrontier,
			Message:   "r2 promoted after required frontier was covered",
		},
		{
			EventID:   "1",
			EventTime: time.Date(2026, 5, 16, 16, 3, 6, 0, time.UTC),
			Type:      "promotion_candidate_evaluated",
			Severity:  "info",
			VolumeID:  "pvc-a",
			ReplicaID: "r2",
			Reason:    PromotionReasonReady,
			Message:   "r2 candidate_ready=true",
		},
	}

	jsonl, err := RenderClusterEventsJSONL(events)
	if err != nil {
		t.Fatalf("render jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(jsonl), "\n")
	if len(lines) != 2 {
		t.Fatalf("jsonl lines=%d:\n%s", len(lines), jsonl)
	}
	if !strings.Contains(lines[0], `"event_id":"1"`) || !strings.Contains(lines[1], `"event_id":"2"`) {
		t.Fatalf("events not sorted by time:\n%s", jsonl)
	}
	if !strings.Contains(lines[1], `"reason_code":"candidate_covers_required_frontier"`) {
		t.Fatalf("missing stable reason code:\n%s", jsonl)
	}
}

func healthyObservationVolume() VolumeEvidence {
	return VolumeEvidence{
		VolumeID:          "pvc-healthy",
		Namespace:         "default",
		PVCName:           "mysql-data",
		ReplicationFactor: 3,
		AckProfile:        PromotionAckProfileSyncQuorum,
		DesiredReplicas:   3,
		ObservedReplicas:  3,
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PrimaryNode:       "m01",
		PublishTarget:     "192.168.1.181:3260",
		Epoch:             1,
		EndpointVersion:   1,
		Replicas: []ReplicaEvidence{
			{
				ReplicaID:            "r1",
				KubernetesNode:       "m01",
				Role:                 "primary",
				ReplicationRole:      "none",
				DurableFrontierKnown: true,
				DurableFrontierLSN:   44,
			},
			{
				ReplicaID:            "r2",
				KubernetesNode:       "m02",
				Role:                 "unknown",
				ReplicationRole:      "replica_ready",
				DurableFrontierKnown: true,
				DurableFrontierLSN:   44,
				CandidateReady:       true,
				CandidateReadyReason: PromotionReasonReady,
			},
			{
				ReplicaID:            "r3",
				KubernetesNode:       "tp01",
				Role:                 "unknown",
				ReplicationRole:      "replica_ready",
				DurableFrontierKnown: true,
				DurableFrontierLSN:   44,
				CandidateReady:       true,
				CandidateReadyReason: PromotionReasonReady,
			},
		},
		NextActions:       []string{"none"},
		SupportBundleHint: "sw-block ops bundle volume pvc-healthy --out /tmp/sw-block-pvc-healthy",
	}
}
