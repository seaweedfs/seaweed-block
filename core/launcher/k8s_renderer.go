package launcher

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"gopkg.in/yaml.v3"
)

type K8sRenderConfig struct {
	Namespace           string
	Image               string
	MasterAddr          string
	DurableRootBase     string
	RecoveryMode        string
	OwnerReferenceToPVC bool
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
		cfg.DurableRootBase = "/var/lib/sw-block"
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
				Selector: selector{MatchLabels: map[string]string{"app": name}},
				Template: podTemplate{
					Metadata: metadata{Labels: map[string]string{
						"app":                            name,
						"sw-block.seaweedfs.com/volume":  plan.VolumeID,
						"sw-block.seaweedfs.com/replica": replica.ReplicaID,
					}},
					Spec: podSpec{
						HostNetwork:  true,
						DNSPolicy:    "ClusterFirstWithHostNet",
						NodeSelector: map[string]string{"kubernetes.io/hostname": replica.ServerID},
						Containers: []container{{
							Name:         "blockvolume",
							Image:        cfg.Image,
							Command:      []string{"/usr/local/bin/blockvolume"},
							Args:         blockVolumeArgs(plan, replica, cfg),
							VolumeMounts: []volumeMount{{Name: "state", MountPath: "/var/lib/sw-block"}},
						}},
						Volumes: []volume{{Name: "state", EmptyDir: emptyDir{}}},
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

func blockVolumeArgs(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload, cfg K8sRenderConfig) []string {
	return []string{
		"--master=" + cfg.MasterAddr,
		"--server-id=" + replica.ServerID,
		"--volume-id=" + plan.VolumeID,
		"--replica-id=" + replica.ReplicaID,
		"--data-addr=" + replica.DataAddr,
		"--ctrl-addr=" + replica.CtrlAddr,
		"--durable-root=" + strings.TrimRight(cfg.DurableRootBase, "/") + "/" + plan.VolumeID + "/" + replica.ReplicaID,
		"--durable-impl=walstore",
		fmt.Sprintf("--durable-blocks=%d", plan.SizeBytes/4096),
		"--durable-blocksize=4096",
		"--recovery-mode=" + cfg.RecoveryMode,
		fmt.Sprintf("--iscsi-listen=127.0.0.1:%d", replica.ISCSIListenPort),
		"--iscsi-iqn=" + replica.ISCSIQualifiedName,
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
	Replicas *int        `yaml:"replicas"`
	Selector selector    `yaml:"selector"`
	Template podTemplate `yaml:"template"`
}

type selector struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type podTemplate struct {
	Metadata metadata `yaml:"metadata"`
	Spec     podSpec  `yaml:"spec"`
}

type podSpec struct {
	HostNetwork  bool              `yaml:"hostNetwork"`
	DNSPolicy    string            `yaml:"dnsPolicy"`
	NodeSelector map[string]string `yaml:"nodeSelector,omitempty"`
	Containers   []container       `yaml:"containers"`
	Volumes      []volume          `yaml:"volumes,omitempty"`
}

type container struct {
	Name         string        `yaml:"name"`
	Image        string        `yaml:"image"`
	Command      []string      `yaml:"command,omitempty"`
	Args         []string      `yaml:"args"`
	VolumeMounts []volumeMount `yaml:"volumeMounts,omitempty"`
}

type volumeMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
}

type volume struct {
	Name     string   `yaml:"name"`
	EmptyDir emptyDir `yaml:"emptyDir"`
}

type emptyDir struct{}
