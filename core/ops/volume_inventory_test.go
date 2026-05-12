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
		"replica: volume=pvc-rf2 replica=r2 server=s2 node=node-b observed=false status=missing",
		"- volume pvc-rf2 observed_replicas=1 desired_replicas=2",
		"- volume pvc-rf2 replica_slot_missing=r2",
		"- volume pvc-rf2 replica r2 missing",
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
		ReplicaID:            replicaID,
		ServerID:             serverID,
		NodeName:             nodeName,
		GeneratedDeployment:  "sw-blockvolume-" + replicaID,
		Protocol:             "iscsi",
		FrontendAddress:      "127.0.0.1:3260",
		StatusAddress:        "127.0.0.1:23260",
		DataAddr:             "127.0.0.1:19000",
		CtrlAddr:             "127.0.0.1:19001",
		Observed:             true,
		AuthorityRole:        role,
		Healthy:              healthy,
		FrontendPrimaryReady: primaryReady,
		ReplicationRole:      replicationRole,
		Epoch:                7,
		EndpointVersion:      2,
	}
}
