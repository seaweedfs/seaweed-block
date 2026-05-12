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
	for _, name := range []string{VolumeStatusReportArtifact, VolumeStatusSummaryArtifact, OpsStatusBundleArtifact} {
		if _, err := os.Stat(filepath.Join(dir, "volumes", "pvc-a", "r1", name)); err != nil {
			t.Fatalf("missing status artifact %s: %v", name, err)
		}
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
