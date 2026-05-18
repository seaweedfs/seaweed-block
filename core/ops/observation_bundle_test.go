package ops

import (
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

func mustWriteInventory(t *testing.T, dir string, inventory VolumeInventory) {
	t.Helper()
	raw, err := MarshalObservationJSON(inventory)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, VolumeInventoryArtifact), string(raw))
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
