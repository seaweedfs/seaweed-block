package launcher

import (
	"bytes"
	"fmt"
	"path"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"gopkg.in/yaml.v3"
)

const stateMountPath = "/var/lib/sw-block"

type K8sRenderConfig struct {
	Namespace           string
	Image               string
	MasterAddr          string
	DurableRootBase     string
	StateHostPathBase   string
	RecoveryMode        string
	OwnerReferenceToPVC bool
	EnableStatus        bool
	ISCSICHAP           CHAPSecretRef
}

type CHAPSecretRef struct {
	Name        string
	UsernameKey string
	SecretKey   string
}

type RenderedManifest struct {
	Name string
	YAML []byte
}

func RenderBlockVolumeDeployments(plan lifecycle.BlockVolumeWorkloadPlan, cfg K8sRenderConfig) ([]RenderedManifest, error) {
	if cfg.Namespace == "" {
		cfg.Namespace = "kube-system"
	}
	if cfg.Image == "" {
		cfg.Image = "sw-block:local"
	}
	if cfg.DurableRootBase == "" {
		cfg.DurableRootBase = stateMountPath
	}
	if cfg.StateHostPathBase != "" && strings.TrimRight(cfg.DurableRootBase, "/") != stateMountPath {
		return nil, fmt.Errorf("launcher: state hostPath requires durable root base %q, got %q", stateMountPath, cfg.DurableRootBase)
	}
	if cfg.RecoveryMode == "" {
		cfg.RecoveryMode = "dual-lane"
	}
	if cfg.MasterAddr == "" {
		return nil, fmt.Errorf("launcher: master addr is required")
	}
	namespace := cfg.Namespace
	ownerRefs, err := ownerReferences(plan, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.OwnerReferenceToPVC {
		namespace = plan.PVCNamespace
	}
	out := make([]RenderedManifest, 0, len(plan.Replicas))
	for _, replica := range plan.Replicas {
		name := workloadName(plan.VolumeID, replica.ReplicaID)
		args, err := blockVolumeArgs(plan, replica, cfg)
		if err != nil {
			return nil, err
		}
		deploy := blockVolumeDeployment{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Metadata: metadata{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					"app":                            "sw-blockvolume",
					"sw-block.seaweedfs.com/volume":  plan.VolumeID,
					"sw-block.seaweedfs.com/replica": replica.ReplicaID,
				},
				OwnerReferences: ownerRefs,
			},
			Spec: deploymentSpec{
				Replicas: intPtr(1),
				Strategy: deploymentStrategy{Type: "Recreate"},
				Selector: selector{MatchLabels: map[string]string{"app": name}},
				Template: podTemplate{
					Metadata: metadata{Labels: map[string]string{
						"app":                            name,
						"sw-block.seaweedfs.com/volume":  plan.VolumeID,
						"sw-block.seaweedfs.com/replica": replica.ReplicaID,
					}},
					Spec: podSpec{
						HostNetwork:    true,
						DNSPolicy:      "ClusterFirstWithHostNet",
						NodeSelector:   map[string]string{"kubernetes.io/hostname": replica.ServerID},
						InitContainers: blockVolumeInitContainers(plan, replica, cfg),
						Containers: []container{{
							Name:         "blockvolume",
							Image:        cfg.Image,
							Command:      []string{"/usr/local/bin/blockvolume"},
							Args:         args,
							Env:          blockVolumeEnv(cfg),
							VolumeMounts: []volumeMount{{Name: "state", MountPath: stateMountPath}},
						}},
						Volumes: []volume{stateVolume(cfg)},
					},
				},
			},
		}
		raw, err := yaml.Marshal(deploy)
		if err != nil {
			return nil, fmt.Errorf("launcher: marshal %s: %w", name, err)
		}
		out = append(out, RenderedManifest{Name: name, YAML: raw})
	}
	return out, nil
}

func stateVolume(cfg K8sRenderConfig) volume {
	if cfg.StateHostPathBase == "" {
		return volume{Name: "state", EmptyDir: &emptyDir{}}
	}
	return volume{Name: "state", HostPath: &hostPath{
		Path: strings.TrimRight(cfg.StateHostPathBase, "/"),
		Type: "DirectoryOrCreate",
	}}
}

func ownerReferences(plan lifecycle.BlockVolumeWorkloadPlan, cfg K8sRenderConfig) ([]ownerReference, error) {
	if !cfg.OwnerReferenceToPVC {
		return nil, nil
	}
	if plan.PVCName == "" || plan.PVCNamespace == "" || plan.PVCUID == "" {
		return nil, fmt.Errorf("launcher: pvc owner reference requires pvc name, namespace, and uid")
	}
	return []ownerReference{{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Name:       plan.PVCName,
		UID:        plan.PVCUID,
		Controller: boolPtr(true),
	}}, nil
}

func blockVolumeArgs(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload, cfg K8sRenderConfig) ([]string, error) {
	args := []string{
		"--master=" + cfg.MasterAddr,
		"--server-id=" + replica.ServerID,
		"--volume-id=" + plan.VolumeID,
		"--replica-id=" + replica.ReplicaID,
		"--data-addr=" + replica.DataAddr,
		"--ctrl-addr=" + replica.CtrlAddr,
		"--durable-root=" + durableRoot(plan, replica, cfg),
		"--durable-impl=walstore",
		fmt.Sprintf("--durable-blocks=%d", plan.SizeBytes/4096),
		"--durable-blocksize=4096",
		"--recovery-mode=" + cfg.RecoveryMode,
	}
	if cfg.EnableStatus {
		port, err := blockVolumeStatusPort(plan, replica)
		if err != nil {
			return nil, err
		}
		args = append(args, fmt.Sprintf("--status-addr=127.0.0.1:%d", port))
	}
	switch plan.Protocol {
	case "nvme":
		args = append(args,
			fmt.Sprintf("--nvme-listen=127.0.0.1:%d", replica.NVMeListenPort),
			"--nvme-subsysnqn="+replica.NVMeSubsystemNQN,
			fmt.Sprintf("--nvme-ns=%d", replica.NVMeNSID),
		)
	default:
		args = append(args,
			fmt.Sprintf("--iscsi-listen=127.0.0.1:%d", replica.ISCSIListenPort),
			"--iscsi-iqn="+replica.ISCSIQualifiedName,
		)
	}
	return args, nil
}

func blockVolumeStatusPort(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload) (int, error) {
	const offset = 20000
	base := replica.ISCSIListenPort
	if plan.Protocol == "nvme" && replica.NVMeListenPort > 0 {
		base = replica.NVMeListenPort
	}
	if base <= 0 {
		return 0, fmt.Errorf("launcher: status endpoint requires a positive frontend port for volume=%s replica=%s", plan.VolumeID, replica.ReplicaID)
	}
	port := base + offset
	if port > 65535 {
		return 0, fmt.Errorf("launcher: derived status port %d overflows TCP port range for volume=%s replica=%s frontend_port=%d", port, plan.VolumeID, replica.ReplicaID, base)
	}
	return port, nil
}

func blockVolumeInitContainers(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload, cfg K8sRenderConfig) []container {
	if cfg.StateHostPathBase == "" {
		return nil
	}
	root := durableRoot(plan, replica, cfg)
	return []container{{
		Name:    "state-permissions",
		Image:   cfg.Image,
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{fmt.Sprintf("mkdir -p %q && chown -R 65532:65532 %q", root, root)},
		VolumeMounts: []volumeMount{{
			Name:      "state",
			MountPath: stateMountPath,
		}},
		SecurityContext: &containerSecurityContext{RunAsUser: int64Ptr(0)},
	}}
}

func durableRoot(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload, cfg K8sRenderConfig) string {
	return path.Join(cfg.DurableRootBase, plan.VolumeID, replica.ReplicaID)
}

func blockVolumeEnv(cfg K8sRenderConfig) []envVar {
	ref := cfg.ISCSICHAP
	if ref.Name == "" {
		return nil
	}
	if ref.UsernameKey == "" {
		ref.UsernameKey = "chapUsername"
	}
	if ref.SecretKey == "" {
		ref.SecretKey = "chapSecret"
	}
	return []envVar{
		{
			Name: "SW_BLOCK_ISCSI_CHAP_USERNAME",
			ValueFrom: &envVarSource{SecretKeyRef: secretKeySelector{
				Name: ref.Name,
				Key:  ref.UsernameKey,
			}},
		},
		{
			Name: "SW_BLOCK_ISCSI_CHAP_SECRET",
			ValueFrom: &envVarSource{SecretKeyRef: secretKeySelector{
				Name: ref.Name,
				Key:  ref.SecretKey,
			}},
		},
	}
}

func workloadName(volumeID, replicaID string) string {
	return "sw-blockvolume-" + dnsLabel(volumeID) + "-" + dnsLabel(replicaID)
}

func dnsLabel(s string) string {
	var b bytes.Buffer
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "x"
	}
	return out
}

func intPtr(v int) *int { return &v }

func int64Ptr(v int64) *int64 { return &v }

func boolPtr(v bool) *bool { return &v }

type blockVolumeDeployment struct {
	APIVersion string         `yaml:"apiVersion"`
	Kind       string         `yaml:"kind"`
	Metadata   metadata       `yaml:"metadata"`
	Spec       deploymentSpec `yaml:"spec"`
}

type metadata struct {
	Name            string            `yaml:"name,omitempty"`
	Namespace       string            `yaml:"namespace,omitempty"`
	Labels          map[string]string `yaml:"labels,omitempty"`
	OwnerReferences []ownerReference  `yaml:"ownerReferences,omitempty"`
}

type ownerReference struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Name       string `yaml:"name"`
	UID        string `yaml:"uid"`
	Controller *bool  `yaml:"controller,omitempty"`
}

type deploymentSpec struct {
	Replicas *int               `yaml:"replicas"`
	Strategy deploymentStrategy `yaml:"strategy"`
	Selector selector           `yaml:"selector"`
	Template podTemplate        `yaml:"template"`
}

type deploymentStrategy struct {
	Type string `yaml:"type"`
}

type selector struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type podTemplate struct {
	Metadata metadata `yaml:"metadata"`
	Spec     podSpec  `yaml:"spec"`
}

type podSpec struct {
	HostNetwork    bool              `yaml:"hostNetwork"`
	DNSPolicy      string            `yaml:"dnsPolicy"`
	NodeSelector   map[string]string `yaml:"nodeSelector,omitempty"`
	InitContainers []container       `yaml:"initContainers,omitempty"`
	Containers     []container       `yaml:"containers"`
	Volumes        []volume          `yaml:"volumes,omitempty"`
}

type container struct {
	Name            string                    `yaml:"name"`
	Image           string                    `yaml:"image"`
	Command         []string                  `yaml:"command,omitempty"`
	Args            []string                  `yaml:"args"`
	Env             []envVar                  `yaml:"env,omitempty"`
	VolumeMounts    []volumeMount             `yaml:"volumeMounts,omitempty"`
	SecurityContext *containerSecurityContext `yaml:"securityContext,omitempty"`
}

type containerSecurityContext struct {
	RunAsUser *int64 `yaml:"runAsUser,omitempty"`
}

type envVar struct {
	Name      string        `yaml:"name"`
	ValueFrom *envVarSource `yaml:"valueFrom,omitempty"`
}

type envVarSource struct {
	SecretKeyRef secretKeySelector `yaml:"secretKeyRef"`
}

type secretKeySelector struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

type volumeMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
}

type volume struct {
	Name     string    `yaml:"name"`
	EmptyDir *emptyDir `yaml:"emptyDir,omitempty"`
	HostPath *hostPath `yaml:"hostPath,omitempty"`
}

type emptyDir struct{}

type hostPath struct {
	Path string `yaml:"path"`
	Type string `yaml:"type,omitempty"`
}
