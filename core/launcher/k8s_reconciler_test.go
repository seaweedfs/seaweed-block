package launcher

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type recordingDeploymentClient struct {
	applied []string
	deleted []string
}

func (c *recordingDeploymentClient) ApplyDeployment(_ context.Context, manifest RenderedManifest) error {
	c.applied = append(c.applied, manifest.Name)
	return nil
}

func (c *recordingDeploymentClient) DeleteDeployment(_ context.Context, ref DeploymentIdentity) error {
	c.deleted = append(c.deleted, ref.Namespace+"/"+ref.Name)
	return nil
}

func TestK8sReconciler_AppliesDesiredAndDeletesOnlyOwnedStaleDeployments(t *testing.T) {
	desired := RenderedManifest{
		Name: "sw-blockvolume-pvc-a-r1",
		YAML: []byte(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sw-blockvolume-pvc-a-r1
  namespace: default
  labels:
    app: sw-blockvolume
    sw-block.seaweedfs.com/volume: pvc-a
    sw-block.seaweedfs.com/replica: r1
`),
	}
	client := &recordingDeploymentClient{}
	result, err := ReconcileBlockVolumeDeployments(context.Background(), ReconcileDeploymentsInput{
		Namespace: "default",
		Desired:   []RenderedManifest{desired},
		Existing: []DeploymentIdentity{
			{
				Namespace: "default",
				Name:      "sw-blockvolume-pvc-a-r1",
				Labels: map[string]string{
					LabelApp:     AppBlockVolume,
					LabelVolume:  "pvc-a",
					LabelReplica: "r1",
				},
			},
			{
				Namespace: "default",
				Name:      "sw-blockvolume-pvc-old-r1",
				Labels: map[string]string{
					LabelApp:     AppBlockVolume,
					LabelVolume:  "pvc-old",
					LabelReplica: "r1",
				},
			},
		},
		Client: client,
	})
	if err != nil {
		t.Fatalf("ReconcileBlockVolumeDeployments: %v", err)
	}
	if !reflect.DeepEqual(client.applied, []string{"sw-blockvolume-pvc-a-r1"}) {
		t.Fatalf("applied=%v", client.applied)
	}
	if !reflect.DeepEqual(client.deleted, []string{"default/sw-blockvolume-pvc-old-r1"}) {
		t.Fatalf("deleted=%v", client.deleted)
	}
	if result.Applied != 1 || result.Deleted != 1 || result.Skipped != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestK8sReconciler_PreservesOperatorScaledZeroReplicaDeployment(t *testing.T) {
	desired := RenderedManifest{
		Name: "sw-blockvolume-pvc-a-r1",
		YAML: []byte(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sw-blockvolume-pvc-a-r1
  namespace: default
  labels:
    app: sw-blockvolume
    sw-block.seaweedfs.com/volume: pvc-a
    sw-block.seaweedfs.com/replica: r1
`),
	}
	zero := 0
	client := &recordingDeploymentClient{}
	result, err := ReconcileBlockVolumeDeployments(context.Background(), ReconcileDeploymentsInput{
		Namespace: "default",
		Desired:   []RenderedManifest{desired},
		Existing: []DeploymentIdentity{{
			Namespace: "default",
			Name:      "sw-blockvolume-pvc-a-r1",
			Labels: map[string]string{
				LabelApp:     AppBlockVolume,
				LabelVolume:  "pvc-a",
				LabelReplica: "r1",
			},
			SpecReplicas: &zero,
		}},
		Client: client,
	})
	if err != nil {
		t.Fatalf("ReconcileBlockVolumeDeployments: %v", err)
	}
	if len(client.applied) != 0 {
		t.Fatalf("scaled-zero deployment should not be reapplied: %v", client.applied)
	}
	if result.Applied != 0 || result.Deleted != 0 || result.Skipped != 1 {
		t.Fatalf("result=%+v", result)
	}
	if got := result.Actions[0].Reason; got != "preserve-replicas-zero" {
		t.Fatalf("reason=%q", got)
	}
}

func TestK8sReconciler_SkipsUnownedOrAmbiguousDeployments(t *testing.T) {
	client := &recordingDeploymentClient{}
	result, err := ReconcileBlockVolumeDeployments(context.Background(), ReconcileDeploymentsInput{
		Namespace: "default",
		Existing: []DeploymentIdentity{
			{
				Namespace: "default",
				Name:      "user-app",
				Labels:    map[string]string{"app": "user-app"},
			},
			{
				Namespace: "default",
				Name:      "sw-blockvolume-missing-labels",
				Labels:    map[string]string{LabelApp: AppBlockVolume},
			},
			{
				Namespace: "default",
				Name:      "custom-name",
				Labels: map[string]string{
					LabelApp:     AppBlockVolume,
					LabelVolume:  "pvc-a",
					LabelReplica: "r1",
				},
			},
			{
				Namespace: "other",
				Name:      "sw-blockvolume-pvc-b-r1",
				Labels: map[string]string{
					LabelApp:     AppBlockVolume,
					LabelVolume:  "pvc-b",
					LabelReplica: "r1",
				},
			},
		},
		Client: client,
	})
	if err != nil {
		t.Fatalf("ReconcileBlockVolumeDeployments: %v", err)
	}
	if len(client.deleted) != 0 {
		t.Fatalf("deleted unowned deployments: %v", client.deleted)
	}
	if result.Deleted != 0 || result.Skipped != 4 {
		t.Fatalf("result=%+v", result)
	}
	wantReasons := []string{
		"skip-unmanaged-app",
		"skip-missing-identity-label",
		"skip-name-mismatch",
		"skip-out-of-namespace",
	}
	for i, want := range wantReasons {
		if result.Actions[i].Reason != want {
			t.Fatalf("action[%d] reason=%q want %q actions=%+v", i, result.Actions[i].Reason, want, result.Actions)
		}
	}
}

func TestK8sReconciler_RejectsDesiredManifestWithoutOwnershipLabels(t *testing.T) {
	_, err := ReconcileBlockVolumeDeployments(context.Background(), ReconcileDeploymentsInput{
		Desired: []RenderedManifest{{
			Name: "bad",
			YAML: []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: bad
  labels:
    app: sw-blockvolume
`),
		}},
		Client: &recordingDeploymentClient{},
	})
	if err == nil {
		t.Fatal("expected invalid desired manifest error")
	}
}

func TestK8sReconciler_RejectsDesiredManifestOutsideManagedNamespace(t *testing.T) {
	client := &recordingDeploymentClient{}
	_, err := ReconcileBlockVolumeDeployments(context.Background(), ReconcileDeploymentsInput{
		Namespace: "default",
		Desired: []RenderedManifest{{
			Name: "sw-blockvolume-pvc-a-r1",
			YAML: []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: sw-blockvolume-pvc-a-r1
  namespace: other
  labels:
    app: sw-blockvolume
    sw-block.seaweedfs.com/volume: pvc-a
    sw-block.seaweedfs.com/replica: r1
`),
		}},
		Client: client,
	})
	if err == nil || !strings.Contains(err.Error(), `namespace="other" does not match managed namespace="default"`) {
		t.Fatalf("expected namespace mismatch error, got %v", err)
	}
	if len(client.applied) != 0 {
		t.Fatalf("mismatched namespace manifest should not be applied: %v", client.applied)
	}
}
