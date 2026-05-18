package ops

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
)

func TestBuildVolumeInventory_MultiVolumeRFShapes(t *testing.T) {
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Host: "m02", Scenario: "inventory"},
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-a",
				Namespace:         "default",
				PVCName:           "app-a",
				PVName:            "pv-a",
				ReplicationFactor: 1,
				SupportBundle:     "volumes/pvc-a",
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", "primary"),
				},
			},
			{
				VolumeID:          "pvc-b",
				Namespace:         "default",
				PVCName:           "app-b",
				PVName:            "pv-b",
				ReplicationFactor: 2,
				SupportBundle:     "volumes/pvc-b",
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", "primary"),
					healthyInventoryReplica("r2", "s2", "node-b", "replica"),
				},
			},
			{
				VolumeID:          "pvc-c",
				Namespace:         "default",
				PVCName:           "app-c",
				PVName:            "pv-c",
				ReplicationFactor: 3,
				SupportBundle:     "volumes/pvc-c",
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", "primary"),
					healthyInventoryReplica("r2", "s2", "node-b", "replica"),
					healthyInventoryReplica("r3", "s3", "node-c", "replica"),
				},
			},
		},
	})

	if inventory.SchemaVersion != VolumeInventorySchemaVersion {
		t.Fatalf("schema_version=%q", inventory.SchemaVersion)
	}
	if len(inventory.Volumes) != 3 {
		t.Fatalf("volumes=%d", len(inventory.Volumes))
	}
	for _, volume := range inventory.Volumes {
		if volume.Status != "ok" {
			t.Fatalf("volume %s status=%s issues=%v", volume.VolumeID, volume.Status, volume.Issues)
		}
		if volume.DesiredReplicas != volume.ReplicationFactor {
			t.Fatalf("volume %s desired=%d rf=%d", volume.VolumeID, volume.DesiredReplicas, volume.ReplicationFactor)
		}
		if volume.ObservedReplicas != volume.ReplicationFactor {
			t.Fatalf("volume %s observed=%d rf=%d", volume.VolumeID, volume.ObservedReplicas, volume.ReplicationFactor)
		}
		if volume.PrimaryReplicaID != "r1" {
			t.Fatalf("volume %s primary=%q", volume.VolumeID, volume.PrimaryReplicaID)
		}
		if len(volume.Protocols) != 1 || volume.Protocols[0] != "iscsi" {
			t.Fatalf("volume %s protocols=%v", volume.VolumeID, volume.Protocols)
		}
	}

	raw, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"schema_version":"1.0"`,
		`"replication_factor":2`,
		`"desired_replicas":3`,
		`"replicas"`,
		`"support_bundle":"volumes/pvc-c"`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("json missing %s:\n%s", want, raw)
		}
	}
}

func TestNodeLoss_BuildVolumeInventory_EmitsTopologyEligibilityMarkers(t *testing.T) {
	r1 := healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary)
	r1.FrontendAddress = "10.0.0.11:3260"
	r2 := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRoleUnknown)
	r2.FrontendAddress = "10.0.0.12:3260"
	r3 := healthyInventoryReplica("r3", "s3", "node-c", hostvolume.AuthorityRoleUnknown)
	r3.FrontendAddress = "storage-c.example:3260"

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "node-loss-eligibility"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-node-loss",
			Namespace:         "default",
			PVCName:           "app-node-loss",
			PVName:            "pvc-node-loss",
			ReplicationFactor: 3,
			Replicas:          []VolumeInventoryReplicaInput{r1, r2, r3},
		}},
	})

	volume := inventory.Volumes[0]
	if !volume.ReplicasOnDistinctNodes || !volume.FrontendsNonLoopback {
		t.Fatalf("eligibility markers distinct=%t non_loopback=%t", volume.ReplicasOnDistinctNodes, volume.FrontendsNonLoopback)
	}
	raw, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"replicas_on_distinct_nodes":true`,
		`"frontends_non_loopback":true`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("json missing %s:\n%s", want, raw)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	if !strings.Contains(summary, "eligibility: volume=pvc-node-loss replicas_on_distinct_nodes=true frontends_non_loopback=true") {
		t.Fatalf("summary missing node-loss eligibility line:\n%s", summary)
	}
}

func TestNodeLoss_BuildVolumeInventory_EligibilityMarkersFailClosedWithoutHealthImpact(t *testing.T) {
	r1 := healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary)
	r2 := healthyInventoryReplica("r2", "s2", "node-a", hostvolume.AuthorityRoleUnknown)
	r2.FrontendAddress = "127.0.0.1:3261"

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "node-loss-ineligible"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-ineligible",
			Namespace:         "default",
			PVCName:           "app-ineligible",
			PVName:            "pvc-ineligible",
			ReplicationFactor: 2,
			Replicas:          []VolumeInventoryReplicaInput{r1, r2},
		}},
	})

	volume := inventory.Volumes[0]
	if volume.ReplicasOnDistinctNodes || volume.FrontendsNonLoopback {
		t.Fatalf("eligibility markers distinct=%t non_loopback=%t want both false", volume.ReplicasOnDistinctNodes, volume.FrontendsNonLoopback)
	}
	if volume.Status != "ok" {
		t.Fatalf("eligibility markers must not change health status: status=%s issues=%v", volume.Status, volume.Issues)
	}
	summary := RenderVolumeInventorySummary(inventory)
	if !strings.Contains(summary, "eligibility: volume=pvc-ineligible replicas_on_distinct_nodes=false frontends_non_loopback=false") {
		t.Fatalf("summary missing ineligible marker line:\n%s", summary)
	}
}

func TestBuildVolumeInventory_EmptyClusterIsTrustworthyOK(t *testing.T) {
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Host: "m02", Scenario: "empty"},
		ProductRevision: "product-rev",
	})

	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitOK {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
	if inventory.Status != "ok" || len(inventory.Volumes) != 0 {
		t.Fatalf("inventory status=%s volumes=%d", inventory.Status, len(inventory.Volumes))
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"inventory_status: ok",
		"volumes: total=0 ok=0 unhealthy=0 invalid=0",
		"issues: none",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_RF2CandidateNamesPromotionReadinessBlocker(t *testing.T) {
	candidate := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRoleUnknown)
	candidate.AckProfile = PromotionAckProfileBestEffort
	candidate.RequiredFrontierKnown = true
	candidate.RequiredFrontierLSN = 90
	candidate.CandidateFrontierKnown = true
	candidate.CandidateFrontierLSN = 90

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "promotion-blocker"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-rf2",
			Namespace:         "default",
			PVCName:           "app-rf2",
			PVName:            "pvc-rf2",
			ReplicationFactor: 2,
			Replicas: []VolumeInventoryReplicaInput{
				healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary),
				candidate,
			},
		}},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("RF=2 best-effort candidate should block recovery claim: status=%s issues=%v", volume.Status, volume.Issues)
	}
	want := "candidate_not_promotion_ready=r2 reason=replication_ack_profile_unmet ack_profile=best-effort"
	if !containsString(volume.Issues, want) {
		t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
	}
	replica := volume.Replicas[1]
	if replica.PromotionReadiness.CandidateReady {
		t.Fatalf("candidate should not be promotion-ready: %+v", replica.PromotionReadiness)
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"promotion: volume=pvc-rf2 replica=r2 candidate_ready=false reason=replication_ack_profile_unmet claim_profile=beta-recovery ack_profile=best-effort",
		"- volume pvc-rf2 candidate_not_promotion_ready=r2 reason=replication_ack_profile_unmet ack_profile=best-effort",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_RF2BestEffortControlledDemoCanBePromotionReady(t *testing.T) {
	candidate := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRoleUnknown)
	candidate.ClaimProfile = PromotionClaimControlledBestEffortDemo
	candidate.AckProfile = PromotionAckProfileBestEffort
	candidate.RequiredFrontierKnown = true
	candidate.RequiredFrontierLSN = 90
	candidate.CandidateFrontierKnown = true
	candidate.CandidateFrontierLSN = 90

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "best-effort-demo"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-rf2-best-effort",
			Namespace:         "default",
			PVCName:           "app-rf2-best-effort",
			PVName:            "pvc-rf2-best-effort",
			ReplicationFactor: 2,
			Replicas: []VolumeInventoryReplicaInput{
				healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary),
				candidate,
			},
		}},
	})

	replica := inventory.Volumes[0].Replicas[1]
	if !replica.PromotionReadiness.CandidateReady {
		t.Fatalf("controlled best-effort demo candidate should be promotion-ready: %+v", replica.PromotionReadiness)
	}
	if replica.PromotionReadiness.ClaimProfile != PromotionClaimControlledBestEffortDemo {
		t.Fatalf("claim_profile=%q", replica.PromotionReadiness.ClaimProfile)
	}
	summary := RenderVolumeInventorySummary(inventory)
	if !strings.Contains(summary, "promotion: volume=pvc-rf2-best-effort replica=r2 candidate_ready=true reason=promotion_ready claim_profile=controlled-best-effort-demo ack_profile=best-effort") {
		t.Fatalf("summary missing controlled best-effort profile:\n%s", summary)
	}
}

func TestBuildVolumeInventory_RF2CandidateRequiresKnownFrontier(t *testing.T) {
	candidate := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRoleUnknown)
	candidate.AckProfile = PromotionAckProfileSyncQuorum
	candidate.RequiredFrontierKnown = false
	candidate.CandidateFrontierKnown = true
	candidate.CandidateFrontierLSN = 90

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "frontier-blocker"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "pvc-rf2-frontier",
			Namespace:         "default",
			PVCName:           "app-rf2-frontier",
			PVName:            "pvc-rf2-frontier",
			ReplicationFactor: 2,
			Replicas: []VolumeInventoryReplicaInput{
				healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary),
				candidate,
			},
		}},
	})

	volume := inventory.Volumes[0]
	want := "candidate_not_promotion_ready=r2 reason=required_frontier_missing ack_profile=sync-quorum"
	if !containsString(volume.Issues, want) {
		t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
	}
	if volume.Replicas[1].PromotionReadiness.Reason != PromotionReasonRequiredFrontierMissing {
		t.Fatalf("promotion reason=%q", volume.Replicas[1].PromotionReadiness.Reason)
	}
}

func TestBuildVolumeInventory_MissingReplicaIsUnhealthyNotCollapsed(t *testing.T) {
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-rf2",
				Namespace:         "default",
				PVCName:           "app-rf2",
				ReplicationFactor: 2,
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", "primary"),
					{ReplicaID: "r2", ServerID: "s2", NodeName: "node-b", Observed: false},
				},
			},
		},
	})

	if len(inventory.Volumes) != 1 {
		t.Fatalf("volumes=%d", len(inventory.Volumes))
	}
	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("status=%s issues=%v", volume.Status, volume.Issues)
	}
	if volume.DesiredReplicas != 2 || volume.ObservedReplicas != 1 {
		t.Fatalf("replica counts desired=%d observed=%d", volume.DesiredReplicas, volume.ObservedReplicas)
	}
	for _, want := range []string{
		"observed_replicas=1 desired_replicas=2",
		"replica_slot_missing=r2",
		"replica r2 missing",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	if len(volume.Replicas) != 2 {
		t.Fatalf("replicas=%d", len(volume.Replicas))
	}
	if volume.Replicas[1].Status != "missing" || !containsString(volume.Replicas[1].Issues, "missing") {
		t.Fatalf("missing replica not explicit: %+v", volume.Replicas[1])
	}

	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"inventory_status: unhealthy",
		"volumes: total=1 ok=0 unhealthy=1 invalid=0",
		"volume: id=pvc-rf2 namespace=default pvc=app-rf2 pv=unavailable rf=2 desired=2 observed=1 primary=r1 status=unhealthy",
		"replica: volume=pvc-rf2 replica=r2 server=s2 node=node-b observed=false status=missing lifecycle_owner=unavailable owner_ref=unavailable",
		"- volume pvc-rf2 observed_replicas=1 desired_replicas=2",
		"- volume pvc-rf2 replica_slot_missing=r2",
		"- volume pvc-rf2 replica r2 missing",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_DegradedReplicaExplainsHealthyButUnready(t *testing.T) {
	replica := healthyInventoryReplica("r1", "s1", "node-a", "primary")
	replica.Epoch = 0
	replica.EndpointVersion = 0
	replica.Issues = append(replica.Issues, "ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0")
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-ready-pod",
				Namespace:         "default",
				PVCName:           "app-ready-pod",
				ReplicationFactor: 1,
				Replicas:          []VolumeInventoryReplicaInput{replica},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" || volume.Replicas[0].Status != "unhealthy" || !volume.Replicas[0].Healthy {
		t.Fatalf("unexpected status shape: volume=%+v replica=%+v", volume, volume.Replicas[0])
	}
	for _, want := range []string{
		"replica_degraded=r1 status=unhealthy",
		"replica r1 ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	for _, got := range volume.Issues {
		if strings.Contains(got, "replica_unhealthy") {
			t.Fatalf("ambiguous issue survived: %v", volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"replica: volume=pvc-ready-pod replica=r1 server=s1 node=node-a observed=true status=unhealthy lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/app-r1 role=primary replication=none healthy=true epoch=0 endpoint_version=0",
		"- volume pvc-ready-pod replica_degraded=r1 status=unhealthy",
		"- volume pvc-ready-pod replica r1 ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_SameNodeAttachEvidenceIsVisible(t *testing.T) {
	replica := healthyInventoryReplica("r1", "m02", "m02", "primary")
	replica.SupportBundle = "volumes/pvc-a/r1"
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-a",
				Namespace:         "default",
				PVCName:           "sw-block-demo-pvc",
				PVName:            "pvc-a",
				ReplicationFactor: 1,
				SupportBundle:     "volumes/pvc-a",
				Replicas:          []VolumeInventoryReplicaInput{replica},
			},
		},
	})

	if inventory.Status != "ok" {
		t.Fatalf("status=%s issues=%v", inventory.Status, VolumeInventoryIssues(inventory))
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-a namespace=default pvc=sw-block-demo-pvc pv=pvc-a rf=1 desired=1 observed=1 primary=r1 status=ok protocols=iscsi replicas=1",
		"replica: volume=pvc-a replica=r1 server=m02 node=m02 observed=true status=ok",
		"frontend=127.0.0.1:3260 status_addr=127.0.0.1:23260 support_bundle=volumes/pvc-a/r1",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_FailoverTimelineIsSelfExplanatory(t *testing.T) {
	r1 := healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRoleSuperseded)
	r1.Healthy = false
	r1.FrontendPrimaryReady = false
	r1.Epoch = 1
	r1.EndpointVersion = 1
	r1.Issues = append(r1.Issues, "stale_primary_fenced=true superseded_by=r2 epoch=2 endpoint_version=1")
	r2 := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRolePrimary)
	r2.Epoch = 2
	r2.EndpointVersion = 1
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "mounted-failover"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-failover",
				Namespace:         "default",
				PVCName:           "app-failover",
				PVName:            "pvc-failover",
				ReplicationFactor: 2,
				Replicas:          []VolumeInventoryReplicaInput{r1, r2},
				FailoverTimeline: []VolumeInventoryFailoverEvent{
					{Phase: "before_failure", ReplicaID: "r1", Role: "primary", Epoch: 1, EndpointVersion: 1, Status: "serving"},
					{Phase: "failure_injected", ReplicaID: "r1", Role: "old_primary", Epoch: 1, EndpointVersion: 1, Status: "stopped", Reason: "primary-blockvolume-controlled-stop"},
					{Phase: "after_failover", ReplicaID: "r2", Role: "primary", Epoch: 2, EndpointVersion: 1, Status: "serving"},
					{Phase: "old_primary_fenced", ReplicaID: "r1", Role: "old_primary", Epoch: 1, EndpointVersion: 1, Status: "fenced", Reason: "superseded_by=r2"},
				},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.PrimaryReplicaID != "r2" {
		t.Fatalf("primary=%q want r2", volume.PrimaryReplicaID)
	}
	if len(volume.FailoverTimeline) != 4 {
		t.Fatalf("timeline=%+v", volume.FailoverTimeline)
	}
	if !containsString(volume.Issues, "replica r1 stale_primary_fenced=true superseded_by=r2 epoch=2 endpoint_version=1") {
		t.Fatalf("stale primary issue missing: %v", volume.Issues)
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-failover namespace=default pvc=app-failover pv=pvc-failover rf=2 desired=2 observed=2 primary=r2 status=unhealthy",
		"replica: volume=pvc-failover replica=r1 server=s1 node=node-a observed=true status=unhealthy lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/app-r1 role=superseded replication=replica_ready healthy=false epoch=1 endpoint_version=1",
		"replica: volume=pvc-failover replica=r2 server=s2 node=node-b observed=true status=ok lifecycle_owner=pvc-owner-ref owner_ref=PersistentVolumeClaim/default/app-r2 role=primary replication=none healthy=true epoch=2 endpoint_version=1",
		"failover: volume=pvc-failover phase=before_failure replica=r1 role=primary epoch=1 endpoint_version=1 status=serving reason=-",
		"failover: volume=pvc-failover phase=failure_injected replica=r1 role=old_primary epoch=1 endpoint_version=1 status=stopped reason=primary-blockvolume-controlled-stop",
		"failover: volume=pvc-failover phase=after_failover replica=r2 role=primary epoch=2 endpoint_version=1 status=serving reason=-",
		"failover: volume=pvc-failover phase=old_primary_fenced replica=r1 role=old_primary epoch=1 endpoint_version=1 status=fenced reason=superseded_by=r2",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	raw, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"failover_timeline"`,
		`"phase":"after_failover"`,
		`"replica_id":"r2"`,
		`"epoch":2`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("json missing %s:\n%s", want, raw)
		}
	}
}

func TestBuildVolumeInventory_StalePrimaryFrontendReadyBlocksRecoveryClaim(t *testing.T) {
	oldPrimary := healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRoleSuperseded)
	oldPrimary.Healthy = true
	oldPrimary.FrontendPrimaryReady = true
	oldPrimary.Epoch = 1
	oldPrimary.EndpointVersion = 1
	newPrimary := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRolePrimary)
	newPrimary.Epoch = 2
	newPrimary.EndpointVersion = 1

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "mounted-failover-negative"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-stale-primary",
				Namespace:         "default",
				PVCName:           "app-stale-primary",
				PVName:            "pvc-stale-primary",
				ReplicationFactor: 2,
				Replicas:          []VolumeInventoryReplicaInput{oldPrimary, newPrimary},
				FailoverTimeline: []VolumeInventoryFailoverEvent{
					{Phase: "before_failure", ReplicaID: "r1", Role: "primary", Epoch: 1, EndpointVersion: 1, Status: "serving"},
					{Phase: "after_failover", ReplicaID: "r2", Role: "primary", Epoch: 2, EndpointVersion: 1, Status: "serving"},
				},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("stale primary with frontend ready must not look recovered: status=%s issues=%v", volume.Status, volume.Issues)
	}
	for _, want := range []string{
		"replica_degraded=r1 status=unhealthy",
		"replica r1 stale_primary_frontend_ready=true role=superseded epoch=1 endpoint_version=1",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"inventory_status: unhealthy",
		"replica: volume=pvc-stale-primary replica=r1 server=s1 node=node-a observed=true status=unhealthy",
		"- volume pvc-stale-primary replica r1 stale_primary_frontend_ready=true role=superseded epoch=1 endpoint_version=1",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_NonPrimaryFrontendReadyBlocksRecoveryClaim(t *testing.T) {
	candidate := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRoleUnknown)
	candidate.Healthy = true
	candidate.FrontendPrimaryReady = true
	candidate.ReplicationRole = hostvolume.ReplicationRoleReady
	candidate.Epoch = 2
	candidate.EndpointVersion = 1

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "mounted-failover-negative"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-non-primary-ready",
				Namespace:         "default",
				PVCName:           "app-non-primary-ready",
				PVName:            "pvc-non-primary-ready",
				ReplicationFactor: 2,
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary),
					candidate,
				},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("non-primary frontend-ready replica must be unhealthy: status=%s issues=%v", volume.Status, volume.Issues)
	}
	want := "replica r2 non-primary authority_role=unknown frontend_primary_ready=true"
	if !containsString(volume.Issues, want) {
		t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
	}
}

func TestBuildVolumeInventory_PrimaryWithReplicaRoleIsNotEligiblePrimary(t *testing.T) {
	candidate := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRolePrimary)
	candidate.ReplicationRole = hostvolume.ReplicationRoleNotReady
	candidate.Epoch = 2
	candidate.EndpointVersion = 1

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "mounted-failover-negative"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-not-ready-candidate",
				Namespace:         "default",
				PVCName:           "app-not-ready-candidate",
				PVName:            "pvc-not-ready-candidate",
				ReplicationFactor: 2,
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRoleSuperseded),
					candidate,
				},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("primary candidate with replication_role=not_ready must not look recovered: status=%s issues=%v", volume.Status, volume.Issues)
	}
	for _, want := range []string{
		"replica_degraded=r2 status=unhealthy",
		"replica r2 primary replication_role=not_ready want none",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-not-ready-candidate namespace=default pvc=app-not-ready-candidate pv=pvc-not-ready-candidate rf=2 desired=2 observed=2 primary=r2 status=unhealthy",
		"replica: volume=pvc-not-ready-candidate replica=r2 server=s2 node=node-b observed=true status=unhealthy",
		"- volume pvc-not-ready-candidate replica r2 primary replication_role=not_ready want none",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_NonPrimaryWithPrimaryReplicationRoleIsUnsafe(t *testing.T) {
	oldPrimary := healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRoleSuperseded)
	oldPrimary.Healthy = false
	oldPrimary.FrontendPrimaryReady = false
	oldPrimary.ReplicationRole = hostvolume.ReplicationRoleNone
	oldPrimary.Epoch = 1
	oldPrimary.EndpointVersion = 1

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "mounted-failover-negative"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-demoted-primary-role",
				Namespace:         "default",
				PVCName:           "app-demoted-primary-role",
				PVName:            "pvc-demoted-primary-role",
				ReplicationFactor: 2,
				Replicas: []VolumeInventoryReplicaInput{
					oldPrimary,
					healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRolePrimary),
				},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("non-primary replication_role=none must be unhealthy: status=%s issues=%v", volume.Status, volume.Issues)
	}
	want := "replica r1 non-primary authority_role=superseded replication_role=none"
	if !containsString(volume.Issues, want) {
		t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
	}
}

func TestBuildVolumeInventory_ConflictingPrimariesAreUnsafe(t *testing.T) {
	r1 := healthyInventoryReplica("r1", "s1", "node-a", hostvolume.AuthorityRolePrimary)
	r1.Epoch = 1
	r1.EndpointVersion = 1
	r2 := healthyInventoryReplica("r2", "s2", "node-b", hostvolume.AuthorityRolePrimary)
	r2.Epoch = 2
	r2.EndpointVersion = 1

	inventory := BuildVolumeInventory(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Scenario: "mounted-failover"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{
			{
				VolumeID:          "pvc-split-brain",
				Namespace:         "default",
				PVCName:           "app-split-brain",
				PVName:            "pvc-split-brain",
				ReplicationFactor: 2,
				Replicas:          []VolumeInventoryReplicaInput{r1, r2},
			},
		},
	})

	volume := inventory.Volumes[0]
	if volume.Status != "unhealthy" {
		t.Fatalf("status=%s issues=%v", volume.Status, volume.Issues)
	}
	if !containsString(volume.Issues, "conflicting_primary_replicas=r1,r2") {
		t.Fatalf("conflict issue missing: %v", volume.Issues)
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-split-brain namespace=default pvc=app-split-brain pv=pvc-split-brain rf=2 desired=2 observed=2 primary=r2 status=unhealthy",
		"replica: volume=pvc-split-brain replica=r1 server=s1 node=node-a observed=true status=ok",
		"replica: volume=pvc-split-brain replica=r2 server=s2 node=node-b observed=true status=ok",
		"- volume pvc-split-brain conflicting_primary_replicas=r1,r2",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestBuildVolumeInventory_InvalidIdentityAndCollectionErrors(t *testing.T) {
	inventory := BuildVolumeInventory(VolumeInventoryInput{
		Source:          ReportSource{},
		ProductRevision: "",
		CollectionErrors: []string{
			"kubernetes list pvc: forbidden",
		},
		Volumes: []VolumeInventoryVolumeInput{
			{
				ReplicationFactor: 1,
				Replicas: []VolumeInventoryReplicaInput{
					healthyInventoryReplica("r1", "s1", "node-a", "primary"),
				},
			},
		},
	})

	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitInvalid {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
	for _, want := range []string{
		"invalid: product_revision unavailable",
		"collection_error: kubernetes list pvc: forbidden",
		"invalid: volume unavailable volume_id unavailable",
	} {
		if !containsString(VolumeInventoryIssues(inventory), want) {
			t.Fatalf("inventory issues missing %q: %v", want, VolumeInventoryIssues(inventory))
		}
	}
}

func healthyInventoryReplica(replicaID, serverID, nodeName, role string) VolumeInventoryReplicaInput {
	replicationRole := hostvolume.ReplicationRoleReady
	healthy := false
	primaryReady := false
	if role == "primary" {
		replicationRole = hostvolume.ReplicationRoleNone
		healthy = true
		primaryReady = true
	}
	return VolumeInventoryReplicaInput{
		ReplicaID:              replicaID,
		ServerID:               serverID,
		NodeName:               nodeName,
		GeneratedDeployment:    "sw-blockvolume-" + replicaID,
		LifecycleOwner:         "pvc-owner-ref",
		OwnerReference:         "PersistentVolumeClaim/default/app-" + replicaID,
		Protocol:               "iscsi",
		FrontendAddress:        "127.0.0.1:3260",
		StatusAddress:          "127.0.0.1:23260",
		DataAddr:               "127.0.0.1:19000",
		CtrlAddr:               "127.0.0.1:19001",
		Observed:               true,
		AuthorityRole:          role,
		Healthy:                healthy,
		FrontendPrimaryReady:   primaryReady,
		ReplicationRole:        replicationRole,
		Epoch:                  7,
		EndpointVersion:        2,
		AckProfile:             PromotionAckProfileSyncQuorum,
		DurableLatched:         true,
		DurableOperational:     true,
		RequiredFrontierLSN:    90,
		RequiredFrontierKnown:  true,
		CandidateFrontierLSN:   90,
		CandidateFrontierKnown: true,
	}
}
