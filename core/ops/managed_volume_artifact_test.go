package ops

import "testing"

func TestManagedVolumeFactsFromEvidence_NodeLossSummary(t *testing.T) {
	facts := ManagedVolumeFactsFromEvidence(VolumeEvidence{
		VolumeID:          "pvc-node-loss",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		AckProfile:        PromotionAckProfileSyncQuorum,
		PrimaryReplica:    "r2",
		PrimaryNode:       "m02",
		PublishTarget:     "192.168.1.184:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:          "r1",
			KubernetesNode:     "m01",
			Role:               "unavailable",
			StalePrimaryFenced: true,
		}, {
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Role:           "primary",
			Observed:       true,
			FrontendAddr:   "192.168.1.184:3260",
		}},
	}, ManagedVolumeArtifactHints{
		NodeLoss: map[string]string{
			"result":                     "promoted",
			"promoted":                   "r2@m02",
			"before_primary":             "r1@m01",
			"after_frontend":             "192.168.1.184:3260",
			"reader_verified":            "true",
			"pod_recreate_used":          "true",
			"data_check_after_node_loss": "reader_checksum_passed",
		},
	})

	projection := ProjectManagedVolume(facts)
	if projection.Status != ManagedVolumeStatusRecovered {
		t.Fatalf("status=%s reason=%s facts=%+v", projection.Status, projection.ReasonCode, facts)
	}
	if projection.ReasonCode != ReasonCSIReattachRecovered {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if len(facts.CSIStages) != 1 || !facts.CSIStages[0].Reattach {
		t.Fatalf("csi facts=%+v", facts.CSIStages)
	}
}

func TestManagedVolumeFactsFromEvidence_Stage2TransparentSummary(t *testing.T) {
	facts := ManagedVolumeFactsFromEvidence(VolumeEvidence{
		VolumeID:          "pvc-stage2",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		ReplicationFactor: 3,
		AckProfile:        PromotionAckProfileSyncQuorum,
		PrimaryReplica:    "r2",
		PublishTarget:     "192.168.1.184:3261",
	}, ManagedVolumeArtifactHints{
		PrimaryFailure: map[string]string{
			"promoted_replica":                   "r2",
			"data_check_after_failover":          "mounted_workload_checksum_passed",
			"pod_recreate_used":                  "false",
			"old_primary_stale_io_success_count": "0",
			"transparent_failover_claimed":       "true",
		},
	})

	projection := ProjectManagedVolume(facts)
	if projection.Status != ManagedVolumeStatusRecovered {
		t.Fatalf("status=%s reason=%s facts=%+v", projection.Status, projection.ReasonCode, facts)
	}
	if projection.ReasonCode != ReasonTransparentHostPathRecovered {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if len(facts.HostPaths) != 1 || !facts.HostPaths[0].MultipathReady || !facts.HostPaths[0].StaleFenced {
		t.Fatalf("host facts=%+v", facts.HostPaths)
	}
}
