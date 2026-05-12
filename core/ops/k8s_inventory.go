package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const seaweedBlockCSIDriver = "block.csi.seaweedfs.com"

type KubernetesInventoryConfig struct {
	Namespace       string
	ProductRevision string
	RunnerRevision  string
	RunCommand      func(context.Context, string, ...string) ([]byte, error)
}

func NewKubernetesVolumeInventoryCollector(cfg KubernetesInventoryConfig) VolumeInventoryCollector {
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.RunCommand == nil {
		cfg.RunCommand = DefaultRunCommand
	}
	return VolumeInventoryCollectorFunc(func(ctx context.Context) (VolumeInventory, error) {
		return collectKubernetesVolumeInventory(ctx, cfg)
	})
}

func collectKubernetesVolumeInventory(ctx context.Context, cfg KubernetesInventoryConfig) (VolumeInventory, error) {
	pvcRaw, err := cfg.RunCommand(ctx, "kubectl", "-n", cfg.Namespace, "get", "pvc", "-o", "json")
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("kubernetes_unreachable: list pvc namespace=%s: %w", cfg.Namespace, err)
	}
	pvRaw, err := cfg.RunCommand(ctx, "kubectl", "get", "pv", "-o", "json")
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("kubernetes_unreachable: list pv: %w", err)
	}
	deployRaw, err := cfg.RunCommand(ctx, "kubectl", "-n", cfg.Namespace, "get", "deploy", "-l", "app=sw-blockvolume", "-o", "json")
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("kubernetes_unreachable: list blockvolume deployments namespace=%s: %w", cfg.Namespace, err)
	}

	pvcs, err := decodeK8sList[k8sPVC](pvcRaw)
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("parse pvc list: %w", err)
	}
	pvs, err := decodeK8sList[k8sPV](pvRaw)
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("parse pv list: %w", err)
	}
	deploys, err := decodeK8sList[k8sDeployment](deployRaw)
	if err != nil {
		return emptyKubernetesInventory(cfg), fmt.Errorf("parse blockvolume deployment list: %w", err)
	}

	pvByClaim := map[string]k8sPV{}
	for _, pv := range pvs {
		key := claimKey(pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
		if key != "/" {
			pvByClaim[key] = pv
		}
	}
	replicasByVolume := map[string][]VolumeInventoryReplicaInput{}
	for _, deploy := range deploys {
		replica := replicaFromDeployment(deploy)
		volumeID := deploy.Metadata.Labels["sw-block.seaweedfs.com/volume"]
		if volumeID == "" {
			volumeID = argValue(deploymentArgs(deploy), "--volume-id")
		}
		if volumeID == "" {
			continue
		}
		replicasByVolume[volumeID] = append(replicasByVolume[volumeID], replica)
	}

	volumes := make([]VolumeInventoryVolumeInput, 0, len(pvcs))
	for _, pvc := range pvcs {
		pv, hasPV := pvByClaim[claimKey(pvc.Metadata.Namespace, pvc.Metadata.Name)]
		if !isSeaweedBlockPVC(pvc, pv, hasPV) {
			continue
		}
		volumeID := pvc.Spec.VolumeName
		pvName := pvc.Spec.VolumeName
		if hasPV {
			pvName = pv.Metadata.Name
			if pv.Spec.CSI.VolumeHandle != "" {
				volumeID = pv.Spec.CSI.VolumeHandle
			}
		}
		volume := VolumeInventoryVolumeInput{
			VolumeID:          volumeID,
			Namespace:         pvc.Metadata.Namespace,
			PVCName:           pvc.Metadata.Name,
			PVName:            pvName,
			ReplicationFactor: max(1, len(replicasByVolume[volumeID])),
			SupportBundle:     "volumes/" + volumeID,
			Replicas:          replicasByVolume[volumeID],
		}
		if len(volume.Replicas) == 0 {
			volume.Issues = append(volume.Issues, "generated_deployment_missing")
		}
		volumes = append(volumes, volume)
	}

	return BuildVolumeInventory(VolumeInventoryInput{
		Source:          ReportSource{Component: "sw-block ops inventory", Scenario: "namespace=" + cfg.Namespace},
		ProductRevision: cfg.ProductRevision,
		RunnerRevision:  cfg.RunnerRevision,
		Volumes:         volumes,
	}), nil
}

func isSeaweedBlockPVC(pvc k8sPVC, pv k8sPV, hasPV bool) bool {
	if strings.HasPrefix(pvc.Spec.StorageClassName, "sw-block") {
		return true
	}
	return hasPV && pv.Spec.CSI.Driver == seaweedBlockCSIDriver
}

func emptyKubernetesInventory(cfg KubernetesInventoryConfig) VolumeInventory {
	return BuildVolumeInventory(VolumeInventoryInput{
		Source:          ReportSource{Component: "sw-block ops inventory", Scenario: "namespace=" + cfg.Namespace},
		ProductRevision: cfg.ProductRevision,
		RunnerRevision:  cfg.RunnerRevision,
	})
}

func decodeK8sList[T any](raw []byte) ([]T, error) {
	var list struct {
		Items []T `json:"items"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	if list.Items == nil {
		return []T{}, nil
	}
	return list.Items, nil
}

func replicaFromDeployment(deploy k8sDeployment) VolumeInventoryReplicaInput {
	args := deploymentArgs(deploy)
	protocol := "iscsi"
	frontend := argValue(args, "--iscsi-listen")
	if nvme := argValue(args, "--nvme-listen"); nvme != "" {
		protocol = "nvme"
		frontend = nvme
	}
	ready := deploy.Status.ReadyReplicas > 0
	replicaID := deploy.Metadata.Labels["sw-block.seaweedfs.com/replica"]
	if replicaID == "" {
		replicaID = argValue(args, "--replica-id")
	}
	return VolumeInventoryReplicaInput{
		ReplicaID:            replicaID,
		ServerID:             firstNonEmpty(argValue(args, "--server-id"), deploy.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]),
		NodeName:             firstNonEmpty(deploy.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"], argValue(args, "--server-id")),
		GeneratedDeployment:  deploy.Metadata.Name,
		Protocol:             protocol,
		FrontendAddress:      frontend,
		StatusAddress:        argValue(args, "--status-addr"),
		DataAddr:             argValue(args, "--data-addr"),
		CtrlAddr:             argValue(args, "--ctrl-addr"),
		Observed:             true,
		AuthorityRole:        hostRoleFromReadiness(ready),
		Healthy:              ready,
		FrontendPrimaryReady: ready,
		ReplicationRole:      hostReplicationFromReadiness(ready),
	}
}

func deploymentArgs(deploy k8sDeployment) []string {
	for _, c := range deploy.Spec.Template.Spec.Containers {
		if c.Name == "blockvolume" {
			return c.Args
		}
	}
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return nil
	}
	return deploy.Spec.Template.Spec.Containers[0].Args
}

func argValue(args []string, flag string) string {
	prefix := flag + "="
	for i, arg := range args {
		if strings.HasPrefix(arg, prefix) {
			return strings.TrimPrefix(arg, prefix)
		}
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func hostRoleFromReadiness(ready bool) string {
	if ready {
		return "primary"
	}
	return Unavailable
}

func hostReplicationFromReadiness(ready bool) string {
	if ready {
		return "none"
	}
	return Unavailable
}

func claimKey(namespace, name string) string {
	return namespace + "/" + name
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type k8sObjectMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid"`
	Labels          map[string]string `json:"labels"`
	OwnerReferences []struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"ownerReferences"`
}

type k8sPVC struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Spec     struct {
		VolumeName       string `json:"volumeName"`
		StorageClassName string `json:"storageClassName"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

type k8sPV struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Spec     struct {
		ClaimRef struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
			UID       string `json:"uid"`
		} `json:"claimRef"`
		CSI struct {
			Driver       string `json:"driver"`
			VolumeHandle string `json:"volumeHandle"`
		} `json:"csi"`
	} `json:"spec"`
}

type k8sDeployment struct {
	Metadata k8sObjectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int `json:"replicas"`
		Template struct {
			Spec struct {
				NodeSelector map[string]string `json:"nodeSelector"`
				Containers   []struct {
					Name string   `json:"name"`
					Args []string `json:"args"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		Replicas      int `json:"replicas"`
		ReadyReplicas int `json:"readyReplicas"`
	} `json:"status"`
}
