package launcher

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG15d_K8sRenderer_RendersBlockVolumeDeploymentArgs(t *testing.T) {
	plan := sampleWorkloadPlan()
	manifests, err := RenderBlockVolumeDeployments(plan, K8sRenderConfig{
		Namespace:       "kube-system",
		Image:           "sw-block:local",
		MasterAddr:      "blockmaster.kube-system.svc.cluster.local:9333",
		DurableRootBase: "/var/lib/sw-block",
	})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("manifest count=%d want 2", len(manifests))
	}
	raw := string(manifests[0].YAML)
	for _, want := range []string{
		"kind: Deployment",
		"name: sw-blockvolume-pvc-a-r1",
		"hostNetwork: true",
		"dnsPolicy: ClusterFirstWithHostNet",
		"--master=blockmaster.kube-system.svc.cluster.local:9333",
		"--volume-id=pvc-a",
		"--replica-id=r1",
		"--durable-root=/var/lib/sw-block/pvc-a/r1",
		"--recovery-mode=dual-lane",
		"--iscsi-listen=0.0.0.0:3260",
		"--iscsi-iqn=iqn.test:pvc-a",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
}

func TestG15d_K8sRenderer_RF2UsesDistinctNamesAndPorts(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{MasterAddr: "m:9333"})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	if manifests[0].Name == manifests[1].Name {
		t.Fatalf("duplicate names: %q", manifests[0].Name)
	}
	if !strings.Contains(string(manifests[1].YAML), "--iscsi-listen=0.0.0.0:3261") {
		t.Fatalf("second manifest missing port 3261:\n%s", manifests[1].YAML)
	}
}

func TestG15d_K8sRenderer_RequiresMasterAddr(t *testing.T) {
	_, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestG15d_K8sRenderer_OutputIsNotAuthorityShaped(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{MasterAddr: "m:9333"})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	raw := strings.ToLower(string(manifests[0].YAML))
	for _, forbidden := range []string{"epoch", "endpointversion", "assignment", "primary", "ready", "healthy"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("manifest must not contain authority-shaped word %q:\n%s", forbidden, raw)
		}
	}
}

func sampleWorkloadPlan() lifecycle.BlockVolumeWorkloadPlan {
	return lifecycle.BlockVolumeWorkloadPlan{
		VolumeID:  "pvc-a",
		SizeBytes: 1 << 20,
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{
			{
				ServerID:           "m02",
				PoolID:             "pool-a",
				ReplicaID:          "r1",
				Source:             lifecycle.PlacementSourceBlankPool,
				DataAddr:           "10.0.0.1:9201",
				CtrlAddr:           "10.0.0.1:9101",
				ISCSIListenPort:    3260,
				ISCSIQualifiedName: "iqn.test:pvc-a",
			},
			{
				ServerID:           "m02",
				PoolID:             "pool-b",
				ReplicaID:          "r2",
				Source:             lifecycle.PlacementSourceBlankPool,
				DataAddr:           "10.0.0.1:9202",
				CtrlAddr:           "10.0.0.1:9102",
				ISCSIListenPort:    3261,
				ISCSIQualifiedName: "iqn.test:pvc-a",
			},
		},
	}
}
