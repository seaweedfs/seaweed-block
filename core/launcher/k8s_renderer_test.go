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
		"type: Recreate",
		"hostNetwork: true",
		"dnsPolicy: ClusterFirstWithHostNet",
		"- /usr/local/bin/blockvolume",
		"mountPath: /var/lib/sw-block",
		"name: state",
		"emptyDir: {}",
		"--master=blockmaster.kube-system.svc.cluster.local:9333",
		"--volume-id=pvc-a",
		"--replica-id=r1",
		"--durable-root=/var/lib/sw-block/pvc-a/r1",
		"--recovery-mode=dual-lane",
		"sw-block.seaweedfs.com/volume: pvc-a",
		"--iscsi-listen=127.0.0.1:3260",
		"--iscsi-iqn=iqn.test:pvc-a",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
	for _, forbidden := range []string{
		"--nvme-listen=",
		"--nvme-subsysnqn=",
		"--nvme-ns=",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("iscsi manifest must not contain %q:\n%s", forbidden, raw)
		}
	}
	if strings.Contains(raw, "--status-addr=") {
		t.Fatalf("status endpoint must be opt-in for generated manifests:\n%s", raw)
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
	if !strings.Contains(string(manifests[1].YAML), "--iscsi-listen=127.0.0.1:3261") {
		t.Fatalf("second manifest missing port 3261:\n%s", manifests[1].YAML)
	}
}

func TestG15d_K8sRenderer_ManifestsAreSafeToConcatenate(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{MasterAddr: "m:9333"})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	var combined strings.Builder
	for _, manifest := range manifests {
		raw := string(manifest.YAML)
		if !strings.HasPrefix(raw, "---\n") {
			t.Fatalf("manifest %s missing YAML document separator:\n%s", manifest.Name, raw)
		}
		combined.WriteString(raw)
	}
	if got := strings.Count(combined.String(), "\nkind: Deployment\n"); got != len(manifests) {
		t.Fatalf("deployment document count=%d want %d:\n%s", got, len(manifests), combined.String())
	}
}

func TestG15d_K8sRenderer_CanUseHostPathStateVolume(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{
		MasterAddr:        "m:9333",
		DurableRootBase:   "/var/lib/sw-block/",
		StateHostPathBase: "/var/lib/sw-block/test-run/",
	})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	raw := string(manifests[0].YAML)
	for _, want := range []string{
		"hostPath:",
		"path: /var/lib/sw-block/test-run",
		"type: DirectoryOrCreate",
		"initContainers:",
		"name: state-permissions",
		"runAsUser: 0",
		"mkdir -p \"/var/lib/sw-block/pvc-a/r1\" && chown -R 65532:65532 \"/var/lib/sw-block/pvc-a/r1\"",
		"mountPath: /var/lib/sw-block",
		"--durable-root=/var/lib/sw-block/pvc-a/r1",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
	if strings.Contains(raw, "emptyDir:") {
		t.Fatalf("hostPath state volume must not render emptyDir:\n%s", raw)
	}
}

func TestG15d_K8sRenderer_RejectsHostPathWithDifferentContainerDurableRoot(t *testing.T) {
	_, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{
		MasterAddr:        "m:9333",
		DurableRootBase:   "/tmp/sw-block",
		StateHostPathBase: "/var/lib/sw-block/test-run",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "state hostPath requires durable root base") {
		t.Fatalf("error=%v", err)
	}
}

func TestG15d_K8sRenderer_RendersNVMeBlockVolumeArgs(t *testing.T) {
	plan := sampleWorkloadPlan()
	plan.Protocol = "nvme"
	manifests, err := RenderBlockVolumeDeployments(plan, K8sRenderConfig{MasterAddr: "m:9333"})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	raw := string(manifests[0].YAML)
	for _, want := range []string{
		"--nvme-listen=127.0.0.1:4420",
		"--nvme-subsysnqn=nqn.test:pvc-a",
		"--nvme-ns=1",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
	for _, forbidden := range []string{
		"--iscsi-listen=",
		"--iscsi-iqn=",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("nvme manifest must not contain %q:\n%s", forbidden, raw)
		}
	}
}

func TestG15d_K8sRenderer_CanOptIntoBlockVolumeStatusEndpoint(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{
		MasterAddr:   "m:9333",
		EnableStatus: true,
	})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	if !strings.Contains(string(manifests[0].YAML), "--status-addr=127.0.0.1:23260") {
		t.Fatalf("first manifest missing status addr:\n%s", manifests[0].YAML)
	}
	if !strings.Contains(string(manifests[1].YAML), "--status-addr=127.0.0.1:23261") {
		t.Fatalf("second manifest missing distinct status addr:\n%s", manifests[1].YAML)
	}
}

func TestG15d_K8sRenderer_StatusEndpointRejectsPortOverflow(t *testing.T) {
	plan := sampleWorkloadPlan()
	plan.Replicas[0].ISCSIListenPort = 60000
	_, err := RenderBlockVolumeDeployments(plan, K8sRenderConfig{
		MasterAddr:   "m:9333",
		EnableStatus: true,
	})
	if err == nil {
		t.Fatal("expected status port overflow error")
	}
	if !strings.Contains(err.Error(), "overflows TCP port range") {
		t.Fatalf("error=%v", err)
	}
}

func TestG15d_K8sRenderer_RequiresMasterAddr(t *testing.T) {
	_, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestG15d_K8sRenderer_CanAttachPVCOwnerReference(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{
		MasterAddr:          "m:9333",
		OwnerReferenceToPVC: true,
	})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	raw := string(manifests[0].YAML)
	for _, want := range []string{
		"namespace: default",
		"ownerReferences:",
		"apiVersion: v1",
		"kind: PersistentVolumeClaim",
		"name: demo-pvc",
		"uid: uid-123",
		"controller: true",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
}

func TestG15d_K8sRenderer_PVCOwnerReferenceRequiresKubernetesMetadata(t *testing.T) {
	plan := sampleWorkloadPlan()
	plan.PVCUID = ""
	_, err := RenderBlockVolumeDeployments(plan, K8sRenderConfig{
		MasterAddr:          "m:9333",
		OwnerReferenceToPVC: true,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestG15d_K8sRenderer_CanWireCHAPSecret(t *testing.T) {
	manifests, err := RenderBlockVolumeDeployments(sampleWorkloadPlan(), K8sRenderConfig{
		MasterAddr: "m:9333",
		ISCSICHAP: CHAPSecretRef{
			Name: "sw-block-iscsi-chap",
		},
	})
	if err != nil {
		t.Fatalf("RenderBlockVolumeDeployments: %v", err)
	}
	raw := string(manifests[0].YAML)
	for _, want := range []string{
		"name: SW_BLOCK_ISCSI_CHAP_USERNAME",
		"name: SW_BLOCK_ISCSI_CHAP_SECRET",
		"name: sw-block-iscsi-chap",
		"key: chapUsername",
		"key: chapSecret",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
	for _, forbidden := range []string{
		"--iscsi-chap-username=$(SW_BLOCK_ISCSI_CHAP_USERNAME)",
		"--iscsi-chap-secret=$(SW_BLOCK_ISCSI_CHAP_SECRET)",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("manifest must not put CHAP secret material in process args %q:\n%s", forbidden, raw)
		}
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
		VolumeID:     "pvc-a",
		SizeBytes:    1 << 20,
		Protocol:     "iscsi",
		PVCName:      "demo-pvc",
		PVCNamespace: "default",
		PVCUID:       "uid-123",
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
				NVMeListenPort:     4420,
				NVMeSubsystemNQN:   "nqn.test:pvc-a",
				NVMeNSID:           1,
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
				NVMeListenPort:     4421,
				NVMeSubsystemNQN:   "nqn.test:pvc-a",
				NVMeNSID:           1,
			},
		},
	}
}
