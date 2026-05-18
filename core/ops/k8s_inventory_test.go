package ops

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestKubernetesInventoryCollector_MapsTwoPVCsToDeployments(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          pvcListJSON,
			"kubectl get pv -o json":                                      pvListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": deploymentListJSON,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if inventory.Status != "ok" {
		t.Fatalf("status=%s issues=%v", inventory.Status, VolumeInventoryIssues(inventory))
	}
	if len(inventory.Volumes) != 2 {
		t.Fatalf("volumes=%d", len(inventory.Volumes))
	}
	for _, volume := range inventory.Volumes {
		if volume.Namespace != "default" || volume.PVCName == Unavailable || volume.PVName == Unavailable {
			t.Fatalf("bad k8s identity: %+v", volume)
		}
		if volume.ReplicationFactor != 1 || volume.DesiredReplicas != 1 || volume.ObservedReplicas != 1 {
			t.Fatalf("bad replica counts: %+v", volume)
		}
		if len(volume.Replicas) != 1 {
			t.Fatalf("replicas=%d for %s", len(volume.Replicas), volume.VolumeID)
		}
		replica := volume.Replicas[0]
		if replica.GeneratedDeployment == Unavailable || replica.ServerID != "m02" || replica.Protocol != "iscsi" {
			t.Fatalf("bad replica identity: %+v", replica)
		}
		if replica.LifecycleOwner != "pvc-owner-ref" || !strings.HasPrefix(replica.OwnerReference, "PersistentVolumeClaim/default/app-") {
			t.Fatalf("bad lifecycle ownership: %+v", replica)
		}
		if !strings.HasPrefix(replica.FrontendAddress, "127.0.0.1:") ||
			!strings.HasPrefix(replica.StatusAddress, "127.0.0.1:") {
			t.Fatalf("bad endpoints: %+v", replica)
		}
	}
}

func TestKubernetesInventoryCollector_OrphanPVCIsActionable(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          pvcListJSON,
			"kubectl get pv -o json":                                      pvListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
	for _, volume := range inventory.Volumes {
		if len(volume.Replicas) != 0 {
			t.Fatalf("expected orphan pvc without replicas: %+v", volume)
		}
		if !containsString(volume.Issues, "generated_deployment_missing") {
			t.Fatalf("volume issues=%v", volume.Issues)
		}
	}
}

func TestKubernetesInventoryCollector_RF2PVCWithoutPlacementKeepsDesiredReplicaCount(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          singlePVCListJSON,
			"kubectl get pv -o json":                                      singleRF2PVListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
	if len(inventory.Volumes) != 1 {
		t.Fatalf("volumes=%d", len(inventory.Volumes))
	}
	volume := inventory.Volumes[0]
	if volume.ReplicationFactor != 2 || volume.DesiredReplicas != 2 || volume.ObservedReplicas != 0 {
		t.Fatalf("replica counts rf=%d desired=%d observed=%d", volume.ReplicationFactor, volume.DesiredReplicas, volume.ObservedReplicas)
	}
	for _, want := range []string{
		"generated_deployment_missing",
		"observed_replicas=0 desired_replicas=2",
		"replica_slot_missing=unknown",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-a namespace=default pvc=app-a pv=pvc-a rf=2 desired=2 observed=0 primary=unavailable status=unhealthy protocols= replicas=0",
		"- volume pvc-a generated_deployment_missing",
		"- volume pvc-a observed_replicas=0 desired_replicas=2",
		"- volume pvc-a replica_slot_missing=unknown",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestKubernetesInventoryCollector_UsesPVHandleBeforeReplicaFallback(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json": `{"items":[{"metadata":{"name":"app-a","namespace":"default","uid":"uid-a"},"spec":{"volumeName":"pv-claim-name","storageClassName":"sw-block-dynamic"},"status":{"phase":"Bound"}}]}`,
			"kubectl get pv -o json":             `{"items":[{"metadata":{"name":"pv-claim-name"},"spec":{"claimRef":{"namespace":"default","name":"app-a","uid":"uid-a"},"csi":{"driver":"block.csi.seaweedfs.com","volumeHandle":"pvc-remapped"}}}]}`,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[
				{"metadata":{"name":"sw-blockvolume-pvc-remapped-r1","namespace":"default","labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-remapped","sw-block.seaweedfs.com/replica":"r1"}},"spec":{"template":{"spec":{"containers":[{"name":"blockvolume","args":["--server-id=m01","--volume-id=pvc-remapped","--replica-id=r1","--iscsi-listen=10.0.0.1:3260"]}]}}},"status":{"replicas":1,"readyReplicas":1}},
				{"metadata":{"name":"sw-blockvolume-pvc-remapped-r2","namespace":"default","labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-remapped","sw-block.seaweedfs.com/replica":"r2"}},"spec":{"template":{"spec":{"containers":[{"name":"blockvolume","args":["--server-id=m02","--volume-id=pvc-remapped","--replica-id=r2","--iscsi-listen=10.0.0.2:3260"]}]}}},"status":{"replicas":1,"readyReplicas":1}}
			]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(inventory.Volumes) != 1 {
		t.Fatalf("volumes=%d want 1: %+v", len(inventory.Volumes), inventory.Volumes)
	}
	volume := inventory.Volumes[0]
	if volume.VolumeID != "pvc-remapped" || volume.ReplicationFactor != 2 || volume.ObservedReplicas != 2 {
		t.Fatalf("volume=%+v", volume)
	}
}

func TestKubernetesInventoryCollector_OrphanDeploymentIsActionable(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          `{"items":[]}`,
			"kubectl get pv -o json":                                      `{"items":[]}`,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": orphanDeploymentListJSON,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
	if len(inventory.Volumes) != 1 {
		t.Fatalf("volumes=%d want orphan deployment row", len(inventory.Volumes))
	}
	volume := inventory.Volumes[0]
	for _, want := range []string{
		"orphan-blockvolume-deploy=sw-blockvolume-pvc-orphan-r1",
		"heartbeat-without-placement=m02 state=unadmitted-by-master reason=no-matching-pvc-or-pv",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-orphan namespace=default pvc=unavailable pv=unavailable rf=1 desired=1 observed=1",
		"- volume pvc-orphan orphan-blockvolume-deploy=sw-blockvolume-pvc-orphan-r1",
		"- volume pvc-orphan heartbeat-without-placement=m02 state=unadmitted-by-master reason=no-matching-pvc-or-pv",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestKubernetesInventoryCollector_LocalProcessWithoutPlacementIsActionable(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          `{"items":[]}`,
			"kubectl get pv -o json":                                      `{"items":[]}`,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": `{"items":[]}`,
			"ps -eo args": "blockvolume --master=127.0.0.1:9333 --server-id=sx --volume-id=pvc-unplaced --replica-id=r1 --data-addr=127.0.0.1:19101 --ctrl-addr=127.0.0.1:19102 --status-addr=127.0.0.1:23260 --iscsi-listen=127.0.0.1:3260\n",
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
	if len(inventory.Volumes) != 1 {
		t.Fatalf("volumes=%d want local process residue row", len(inventory.Volumes))
	}
	volume := inventory.Volumes[0]
	for _, want := range []string{
		"blockvolume-process-without-placement=sx",
		"heartbeat-without-placement=sx state=unadmitted-by-master reason=local-process-without-pvc-or-pv",
		"replica r1 local_process_without_kubernetes_placement",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"volume: id=pvc-unplaced namespace=default pvc=unavailable pv=unavailable rf=1 desired=1 observed=1",
		"replica: volume=pvc-unplaced replica=r1 server=sx node=sx observed=true status=unhealthy lifecycle_owner=unavailable owner_ref=unavailable",
		"- volume pvc-unplaced heartbeat-without-placement=sx state=unadmitted-by-master reason=local-process-without-pvc-or-pv",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestKubernetesInventoryCollector_AttachesReplicaStatusBundles(t *testing.T) {
	masterAddr, closeMaster := startOpsFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("volume"); got != "pvc-a" {
			t.Fatalf("volume query=%q want pvc-a", got)
		}
		switch r.URL.Path {
		case "/status":
			writeLiveJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:        "pvc-a",
					ReplicaID:       "r1",
					Epoch:           7,
					EndpointVersion: 2,
					Healthy:         true,
				},
				FrontendPrimaryReady: true,
				AuthorityRole:        hostvolume.AuthorityRolePrimary,
				ReplicationRole:      hostvolume.ReplicationRoleNone,
			})
		case "/status/peers":
			writeLiveJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{Peers: []replication.ReplicaPeerStatus{}})
		case "/status/durable":
			writeLiveJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "pvc-a", ReplicaID: "r1", Latched: true, Operational: true}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()
	dir := t.TempDir()

	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:        "default",
		MasterAddr:       masterAddr,
		StatusBundleRoot: dir,
		ProductRevision:  "product-rev",
		RunnerRevision:   "runner-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          singlePVCListJSON,
			"kubectl get pv -o json":                                      singlePVListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": fmt.Sprintf(singleDeploymentListJSONTemplate, statusServer.URL),
			"iscsiadm -m session":                                         "iscsiadm: No active sessions.\n",
			"nvme list-subsys -o json":                                    `{"Subsystems":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if inventory.Status != "ok" {
		t.Fatalf("status=%s issues=%v", inventory.Status, VolumeInventoryIssues(inventory))
	}
	replica := inventory.Volumes[0].Replicas[0]
	if replica.SupportBundle != "volumes/pvc-a/r1" {
		t.Fatalf("support bundle=%q", replica.SupportBundle)
	}
	if replica.Epoch != 7 || replica.EndpointVersion != 2 {
		t.Fatalf("replica status was not refreshed from nested report: epoch=%d ev=%d", replica.Epoch, replica.EndpointVersion)
	}
	for _, name := range []string{VolumeStatusReportArtifact, VolumeStatusSummaryArtifact, OpsStatusBundleArtifact} {
		if _, err := os.Stat(filepath.Join(dir, "volumes", "pvc-a", "r1", name)); err != nil {
			t.Fatalf("missing status artifact %s: %v", name, err)
		}
	}
}

func TestKubernetesInventoryCollector_RF2ReadyReplicaStillNamesMissingFrontier(t *testing.T) {
	masterAddr, closeMaster := startOpsFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			writeLiveJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:  "pvc-a",
					ReplicaID: "r2",
					Healthy:   true,
				},
				AuthorityRole:        hostvolume.AuthorityRoleUnknown,
				FrontendPrimaryReady: false,
				ReplicationRole:      hostvolume.ReplicationRoleReady,
			})
		case "/status/peers":
			writeLiveJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{})
		case "/status/durable":
			writeLiveJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "pvc-a", ReplicaID: "r2", Latched: true, Operational: true, FrontierKnown: true, DurableLSN: 53, RetainedLSN: 1, HeadLSN: 53}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	deploymentJSON := fmt.Sprintf(`{"items":[
  {"metadata":{"name":"sw-blockvolume-pvc-a-r2","namespace":"default","labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-a","sw-block.seaweedfs.com/replica":"r2"},"ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-a","uid":"uid-a"}]},
   "status":{"replicas":1,"readyReplicas":1},
   "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02-r2","--volume-id=pvc-a","--replica-id=r2","--data-addr=127.0.0.1:19103","--ctrl-addr=127.0.0.1:19104","--status-addr=%s","--replication-ack=sync-quorum","--iscsi-listen=127.0.0.1:3261","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-a"]}]}}}}
]}`, statusServer.URL)
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:        "default",
		MasterAddr:       masterAddr,
		StatusBundleRoot: t.TempDir(),
		ProductRevision:  "product-rev",
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          singlePVCListJSON,
			"kubectl get pv -o json":                                      singleRF2PVListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": deploymentJSON,
			"iscsiadm -m session":                                         "iscsiadm: No active sessions.\n",
			"nvme list-subsys -o json":                                    `{"Subsystems":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	volume := inventory.Volumes[0]
	if volume.ReplicationFactor != 2 || volume.DesiredReplicas != 2 {
		t.Fatalf("rf/desired not preserved: %+v", volume)
	}
	replica := volume.Replicas[0]
	if replica.PromotionReadiness.CandidateReady {
		t.Fatalf("live collector must not infer readiness without frontier evidence: %+v", replica.PromotionReadiness)
	}
	if replica.PromotionReadiness.Reason != PromotionReasonRequiredFrontierMissing {
		t.Fatalf("reason=%q want %q", replica.PromotionReadiness.Reason, PromotionReasonRequiredFrontierMissing)
	}
	if !replica.PromotionReadiness.CandidateFrontierKnown || replica.PromotionReadiness.CandidateFrontierLSN != 53 {
		t.Fatalf("candidate frontier not propagated from durable status: %+v", replica.PromotionReadiness)
	}
	for _, want := range []string{
		"candidate_not_promotion_ready=r2 reason=required_frontier_missing ack_profile=sync-quorum",
		"observed_replicas=1 desired_replicas=2",
	} {
		if !containsString(volume.Issues, want) {
			t.Fatalf("volume issues missing %q: %v", want, volume.Issues)
		}
	}
	summary := RenderVolumeInventorySummary(inventory)
	if !strings.Contains(summary, "promotion: volume=pvc-a replica=r2 candidate_ready=false reason=required_frontier_missing claim_profile=beta-recovery ack_profile=sync-quorum required_frontier_known=false required_frontier_lsn=0 candidate_frontier_known=true candidate_frontier_lsn=53") {
		t.Fatalf("summary missing promotion blocker:\n%s", summary)
	}
}

func TestKubernetesInventoryCollector_RequiredFrontierMakesCoveredCandidateReady(t *testing.T) {
	masterAddr, closeMaster := startOpsFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			writeLiveJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:  "pvc-a",
					ReplicaID: "r2",
					Healthy:   true,
				},
				AuthorityRole:        hostvolume.AuthorityRoleUnknown,
				FrontendPrimaryReady: false,
				ReplicationRole:      hostvolume.ReplicationRoleReady,
			})
		case "/status/peers":
			writeLiveJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{})
		case "/status/durable":
			writeLiveJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "pvc-a", ReplicaID: "r2", Latched: true, Operational: true, FrontierKnown: true, DurableLSN: 53, RetainedLSN: 1, HeadLSN: 53}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	deploymentJSON := fmt.Sprintf(`{"items":[
  {"metadata":{"name":"sw-blockvolume-pvc-a-r2","namespace":"default","labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-a","sw-block.seaweedfs.com/replica":"r2"},"ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-a","uid":"uid-a"}]},
   "status":{"replicas":1,"readyReplicas":1},
   "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02-r2","--volume-id=pvc-a","--replica-id=r2","--data-addr=127.0.0.1:19103","--ctrl-addr=127.0.0.1:19104","--status-addr=%s","--replication-ack=sync-quorum","--iscsi-listen=127.0.0.1:3261","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-a"]}]}}}}
]}`, statusServer.URL)
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:         "default",
		MasterAddr:        masterAddr,
		StatusBundleRoot:  t.TempDir(),
		ProductRevision:   "product-rev",
		RequiredFrontiers: map[string]uint64{"pvc-a": 53},
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          singlePVCListJSON,
			"kubectl get pv -o json":                                      singleRF2PVListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": deploymentJSON,
			"iscsiadm -m session":                                         "iscsiadm: No active sessions.\n",
			"nvme list-subsys -o json":                                    `{"Subsystems":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	replica := inventory.Volumes[0].Replicas[0]
	if !replica.PromotionReadiness.CandidateReady {
		t.Fatalf("candidate should be ready when required and candidate frontiers match: %+v", replica.PromotionReadiness)
	}
	if !replica.PromotionReadiness.RequiredFrontierKnown || replica.PromotionReadiness.RequiredFrontierLSN != 53 {
		t.Fatalf("required frontier not propagated: %+v", replica.PromotionReadiness)
	}
	summary := RenderVolumeInventorySummary(inventory)
	if !strings.Contains(summary, "promotion: volume=pvc-a replica=r2 candidate_ready=true reason=promotion_ready claim_profile=beta-recovery ack_profile=sync-quorum required_frontier_known=true required_frontier_lsn=53 candidate_frontier_known=true candidate_frontier_lsn=53 frontier_covered=true") {
		t.Fatalf("summary missing covered promotion evidence:\n%s", summary)
	}
	if strings.Contains(summary, "candidate_not_promotion_ready=r2") {
		t.Fatalf("summary should not include candidate-not-ready issue when frontier is covered:\n%s", summary)
	}
}

func TestKubernetesInventoryCollector_Stage2ClaimRejectsBestEffortCandidate(t *testing.T) {
	masterAddr, closeMaster := startOpsFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			writeLiveJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:  "pvc-a",
					ReplicaID: "r2",
					Healthy:   true,
				},
				AuthorityRole:        hostvolume.AuthorityRoleUnknown,
				FrontendPrimaryReady: false,
				ReplicationRole:      hostvolume.ReplicationRoleReady,
			})
		case "/status/peers":
			writeLiveJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{})
		case "/status/durable":
			writeLiveJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{Volumes: []durable.VolumeStatus{{VolumeID: "pvc-a", ReplicaID: "r2", Latched: true, Operational: true, FrontierKnown: true, DurableLSN: 53, RetainedLSN: 1, HeadLSN: 53}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	deploymentJSON := fmt.Sprintf(`{"items":[
  {"metadata":{"name":"sw-blockvolume-pvc-a-r2","namespace":"default","labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-a","sw-block.seaweedfs.com/replica":"r2"},"ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-a","uid":"uid-a"}]},
   "status":{"replicas":1,"readyReplicas":1},
   "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02-r2","--volume-id=pvc-a","--replica-id=r2","--data-addr=127.0.0.1:19103","--ctrl-addr=127.0.0.1:19104","--status-addr=%s","--replication-ack=best-effort","--iscsi-listen=127.0.0.1:3261","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-a"]}]}}}}
]}`, statusServer.URL)
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:         "default",
		MasterAddr:        masterAddr,
		StatusBundleRoot:  t.TempDir(),
		ProductRevision:   "product-rev",
		ClaimProfile:      PromotionClaimStage2ISCSIALUAMultipath,
		RequiredFrontiers: map[string]uint64{"pvc-a": 53},
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          singlePVCListJSON,
			"kubectl get pv -o json":                                      singleRF2PVListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": deploymentJSON,
			"iscsiadm -m session":                                         "iscsiadm: No active sessions.\n",
			"nvme list-subsys -o json":                                    `{"Subsystems":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	replica := inventory.Volumes[0].Replicas[0]
	if replica.PromotionReadiness.CandidateReady {
		t.Fatalf("stage2 claim must not accept best-effort candidate: %+v", replica.PromotionReadiness)
	}
	if replica.PromotionReadiness.Reason != PromotionReasonReplicationAckProfileBad {
		t.Fatalf("reason=%q want %q", replica.PromotionReadiness.Reason, PromotionReasonReplicationAckProfileBad)
	}
	summary := RenderVolumeInventorySummary(inventory)
	for _, want := range []string{
		"promotion: volume=pvc-a replica=r2 candidate_ready=false reason=replication_ack_profile_unmet claim_profile=stage2-iscsi-alua-multipath ack_profile=best-effort",
		"- volume pvc-a candidate_not_promotion_ready=r2 reason=replication_ack_profile_unmet ack_profile=best-effort",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
}

func TestKubernetesInventoryCollector_PreservesReplicaFrontendWhenMasterReportsPrimaryFrontend(t *testing.T) {
	masterAddr, closeMaster := startOpsFakeMaster(t)
	defer closeMaster()
	statusServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			writeLiveJSON(t, w, hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:  "pvc-a",
					ReplicaID: "r2",
				},
				AuthorityRole:   hostvolume.AuthorityRoleUnknown,
				ReplicationRole: hostvolume.ReplicationRoleNotReady,
			})
		case "/status/peers":
			writeLiveJSON(t, w, struct {
				Peers []replication.ReplicaPeerStatus
			}{})
		case "/status/durable":
			writeLiveJSON(t, w, struct {
				Volumes []durable.VolumeStatus
			}{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer statusServer.Close()

	deploymentJSON := fmt.Sprintf(`{"items":[
  {"metadata":{"name":"sw-blockvolume-pvc-a-r2","namespace":"default","ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-a"}]},
   "status":{"readyReplicas":1},
   "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02-r2","--volume-id=pvc-a","--replica-id=r2","--data-addr=127.0.0.1:19103","--ctrl-addr=127.0.0.1:19104","--status-addr=%s","--iscsi-listen=127.0.0.1:3261","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-a"]}]}}}}
]}`, statusServer.URL)
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:        "default",
		MasterAddr:       masterAddr,
		StatusBundleRoot: t.TempDir(),
		RunCommand: fixtureKubectl(map[string]string{
			"kubectl -n default get pvc -o json":                          singlePVCListJSON,
			"kubectl get pv -o json":                                      singlePVListJSON,
			"kubectl -n default get deploy -l app=sw-blockvolume -o json": deploymentJSON,
			"iscsiadm -m session":                                         "iscsiadm: No active sessions.\n",
			"nvme list-subsys -o json":                                    `{"Subsystems":[]}`,
		}),
	})

	inventory, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	replica := inventory.Volumes[0].Replicas[0]
	if replica.FrontendAddress != "127.0.0.1:3261" {
		t.Fatalf("frontend address overwritten by master primary frontend: %+v", replica)
	}
}

func TestKubernetesInventoryCollector_PreservesDeploymentReplicaIdentityWhenStatusReportsCurrentPrimary(t *testing.T) {
	replica := VolumeInventoryReplicaInput{
		ReplicaID:       "r1",
		ServerID:        "node-loss-r1",
		NodeName:        "m01",
		FrontendAddress: "192.168.1.181:3260",
		StatusAddress:   "192.168.1.181:23260",
		Observed:        true,
	}
	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		MasterStatus: &control.StatusResponse{
			VolumeId:        "pvc-a",
			ReplicaId:       "r2",
			Assigned:        true,
			Epoch:           2,
			EndpointVersion: 1,
		},
	})

	applyStatusReportToInventoryReplica(&replica, report)

	if replica.ReplicaID != "r1" {
		t.Fatalf("inventory row identity was overwritten by current primary status: got %q want r1", replica.ReplicaID)
	}
	if replica.ServerID != "node-loss-r1" || replica.NodeName != "m01" {
		t.Fatalf("inventory row placement changed unexpectedly: %+v", replica)
	}
}

func TestOpsStatusInventoryIssue_UsesReportEvidence(t *testing.T) {
	report := BuildVolumeStatusReport(VolumeStatusReportInput{
		ProductRevision: "product-rev",
		MasterStatus:    &control.StatusResponse{VolumeId: "pvc-a", ReplicaId: "r1", Assigned: false},
		LocalStatus: &hostvolume.StatusProjection{
			Projection: frontend.Projection{
				VolumeID:  "pvc-a",
				ReplicaID: "r1",
				Healthy:   true,
			},
			AuthorityRole:        hostvolume.AuthorityRolePrimary,
			FrontendPrimaryReady: true,
			ReplicationRole:      hostvolume.ReplicationRoleNone,
		},
	})

	issue := opsStatusInventoryIssue(VolumeStatusExitUnhealthy, report)
	want := "ops_status=unhealthy reason=authority_not_assigned assigned=false epoch=0 endpoint_version=0"
	if issue != want {
		t.Fatalf("issue=%q want %q", issue, want)
	}
}

func TestKubernetesInventoryCollector_KubernetesUnreachableIsInvalid(t *testing.T) {
	collector := NewKubernetesVolumeInventoryCollector(KubernetesInventoryConfig{
		Namespace:       "default",
		ProductRevision: "product-rev",
		RunCommand: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("The connection to the server 127.0.0.1:6443 was refused\n"), fmt.Errorf("exit status 1")
		},
	})

	inventory, err := collector.Collect(context.Background())
	if err == nil {
		t.Fatal("expected collection error")
	}
	inventory.CollectionErrors = append(inventory.CollectionErrors, splitErrorMessages(err)...)
	if got := ClassifyVolumeInventory(inventory); got != VolumeStatusExitInvalid {
		t.Fatalf("exit=%d issues=%v", got, VolumeInventoryIssues(inventory))
	}
}

func TestLoopbackStatusPortOnlyRewritesGeneratedAddresses(t *testing.T) {
	port, ok := loopbackStatusPort("127.0.0.1:23260")
	if !ok || port != "23260" {
		t.Fatalf("generated loopback addr port=%q ok=%t", port, ok)
	}
	if port, ok := loopbackStatusPort("http://127.0.0.1:23260"); ok || port != "" {
		t.Fatalf("explicit URL should be treated as caller-reachable, port=%q ok=%t", port, ok)
	}
	if port, ok := loopbackStatusPort("10.0.0.5:23260"); ok || port != "" {
		t.Fatalf("non-loopback addr port=%q ok=%t", port, ok)
	}
}

func fixtureKubectl(outputs map[string]string) func(context.Context, string, ...string) ([]byte, error) {
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		key := strings.TrimSpace(name + " " + strings.Join(args, " "))
		out, ok := outputs[key]
		if !ok {
			return nil, fmt.Errorf("unexpected command %q", key)
		}
		return []byte(out), nil
	}
}

const pvcListJSON = `{
  "items": [
    {"metadata":{"name":"app-a","namespace":"default","uid":"uid-a"},"spec":{"volumeName":"pvc-a","storageClassName":"sw-block-dynamic"},"status":{"phase":"Bound"}},
    {"metadata":{"name":"app-b","namespace":"default","uid":"uid-b"},"spec":{"volumeName":"pvc-b","storageClassName":"sw-block-dynamic"},"status":{"phase":"Bound"}},
    {"metadata":{"name":"unrelated","namespace":"default","uid":"uid-x"},"spec":{"volumeName":"pv-unrelated","storageClassName":"standard"},"status":{"phase":"Bound"}}
  ]
}`

const pvListJSON = `{
  "items": [
    {"metadata":{"name":"pvc-a"},"spec":{"claimRef":{"namespace":"default","name":"app-a","uid":"uid-a"},"csi":{"driver":"block.csi.seaweedfs.com","volumeHandle":"pvc-a"}}},
    {"metadata":{"name":"pvc-b"},"spec":{"claimRef":{"namespace":"default","name":"app-b","uid":"uid-b"},"csi":{"driver":"block.csi.seaweedfs.com","volumeHandle":"pvc-b"}}},
    {"metadata":{"name":"pv-unrelated"},"spec":{"claimRef":{"namespace":"default","name":"unrelated","uid":"uid-x"},"csi":{"driver":"other.example.com","volumeHandle":"pv-unrelated"}}}
  ]
}`

const singlePVCListJSON = `{
  "items": [
    {"metadata":{"name":"app-a","namespace":"default","uid":"uid-a"},"spec":{"volumeName":"pvc-a","storageClassName":"sw-block-dynamic"},"status":{"phase":"Bound"}}
  ]
}`

const singlePVListJSON = `{
  "items": [
    {"metadata":{"name":"pvc-a"},"spec":{"claimRef":{"namespace":"default","name":"app-a","uid":"uid-a"},"csi":{"driver":"block.csi.seaweedfs.com","volumeHandle":"pvc-a"}}}
  ]
}`

const singleRF2PVListJSON = `{
  "items": [
    {"metadata":{"name":"pvc-a"},"spec":{"claimRef":{"namespace":"default","name":"app-a","uid":"uid-a"},"csi":{"driver":"block.csi.seaweedfs.com","volumeHandle":"pvc-a","volumeAttributes":{"replicationFactor":"2","protocol":"iscsi"}}}}
  ]
}`

const singleDeploymentListJSONTemplate = `{
  "items": [
    {
      "metadata":{
        "name":"sw-blockvolume-pvc-a-r1",
        "namespace":"default",
        "labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-a","sw-block.seaweedfs.com/replica":"r1"},
        "ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-a","uid":"uid-a"}]
      },
      "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02","--volume-id=pvc-a","--replica-id=r1","--data-addr=127.0.0.1:19101","--ctrl-addr=127.0.0.1:19102","--status-addr=%s","--iscsi-listen=127.0.0.1:3260","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-a"]}]}}},
      "status":{"replicas":1,"readyReplicas":1}
    }
  ]
}`

const deploymentListJSON = `{
  "items": [
    {
      "metadata":{
        "name":"sw-blockvolume-pvc-a-r1",
        "namespace":"default",
        "labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-a","sw-block.seaweedfs.com/replica":"r1"},
        "ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-a","uid":"uid-a"}]
      },
      "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02","--volume-id=pvc-a","--replica-id=r1","--data-addr=127.0.0.1:19101","--ctrl-addr=127.0.0.1:19102","--status-addr=127.0.0.1:23260","--iscsi-listen=127.0.0.1:3260","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-a"]}]}}},
      "status":{"replicas":1,"readyReplicas":1}
    },
    {
      "metadata":{
        "name":"sw-blockvolume-pvc-b-r1",
        "namespace":"default",
        "labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-b","sw-block.seaweedfs.com/replica":"r1"},
        "ownerReferences":[{"kind":"PersistentVolumeClaim","name":"app-b","uid":"uid-b"}]
      },
      "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02","--volume-id=pvc-b","--replica-id=r1","--data-addr=127.0.0.1:19111","--ctrl-addr=127.0.0.1:19112","--status-addr=127.0.0.1:23261","--iscsi-listen=127.0.0.1:3261","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-b"]}]}}},
      "status":{"replicas":1,"readyReplicas":1}
    }
  ]
}`

const orphanDeploymentListJSON = `{
  "items": [
    {
      "metadata":{
        "name":"sw-blockvolume-pvc-orphan-r1",
        "namespace":"default",
        "labels":{"app":"sw-blockvolume","sw-block.seaweedfs.com/volume":"pvc-orphan","sw-block.seaweedfs.com/replica":"r1"}
      },
      "spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"m02"},"containers":[{"name":"blockvolume","args":["--server-id=m02","--volume-id=pvc-orphan","--replica-id=r1","--data-addr=127.0.0.1:19101","--ctrl-addr=127.0.0.1:19102","--status-addr=127.0.0.1:23260","--iscsi-listen=127.0.0.1:3260","--iscsi-iqn=iqn.2026-05.io.seaweedfs:pvc-orphan"]}]}}},
      "status":{"replicas":1,"readyReplicas":1}
    }
  ]
}`
