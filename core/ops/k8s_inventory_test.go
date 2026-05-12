package ops

import (
	"context"
	"fmt"
	"strings"
	"testing"
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
