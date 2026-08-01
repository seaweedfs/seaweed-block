package launcher

import (
	"bytes"
	"fmt"
	"net"
	"path"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"gopkg.in/yaml.v3"
)

const (
	stateMountPath           = "/var/lib/sw-block"
	snapshotRuntimeMountPath = "/var/run/sw-block/snapshot-runtime"
)

type K8sRenderConfig struct {
	Namespace                     string
	Image                         string
	MasterAddr                    string
	DurableRootBase               string
	DurableImpl                   string
	WALMultiBlockRecords          bool
	WALRecoveryTestDisableFlusher bool
	StateHostPathBase             string
	RecoveryMode                  string
	ReplicationAck                string
	OwnerReferenceToPVC           bool
	EnableStatus                  bool
	ExternalISCSI                 bool
	ExternalNVMe                  bool
	ExternalStatus                bool
	NVMeMaxH2CDataLength          uint32
	ISCSICHAP                     CHAPSecretRef
	SnapshotRuntimeSecretName     string
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
	if cfg.DurableImpl == "" {
		cfg.DurableImpl = "walstore"
	}
	switch cfg.DurableImpl {
	case "walstore", "smartwal", "parallel-walstore":
	default:
		return nil, fmt.Errorf("launcher: durable impl %q invalid; want walstore, smartwal, or parallel-walstore", cfg.DurableImpl)
	}
	if cfg.StateHostPathBase != "" && path.Clean(cfg.DurableRootBase) != stateMountPath {
		return nil, fmt.Errorf("launcher: state hostPath requires durable root base %q, got %q", stateMountPath, cfg.DurableRootBase)
	}
	if cfg.RecoveryMode == "" {
		cfg.RecoveryMode = "dual-lane"
	}
	if cfg.ReplicationAck == "" {
		cfg.ReplicationAck = "best-effort"
	}
	switch cfg.ReplicationAck {
	case "best-effort", "sync-quorum", "sync-all":
	default:
		return nil, fmt.Errorf("launcher: replication ack %q invalid; want best-effort, sync-quorum, or sync-all", cfg.ReplicationAck)
	}
	if cfg.ExternalISCSI && cfg.ISCSICHAP.Name == "" {
		return nil, fmt.Errorf("launcher: external iSCSI requires CHAP secret")
	}
	if cfg.ExternalStatus && !cfg.ExternalISCSI && !cfg.ExternalNVMe {
		return nil, fmt.Errorf("launcher: external status requires an external block frontend mode")
	}
	if cfg.SnapshotRuntimeSecretName != "" && cfg.OwnerReferenceToPVC {
		return nil, fmt.Errorf("launcher: snapshot runtime credentials require blockvolume workloads to remain in the launcher namespace")
	}
	if plan.SourceSnapshotID != "" {
		if !lifecycle.IsSafeStorageIdentityComponent(plan.SourceSnapshotID) {
			return nil, fmt.Errorf("launcher: source snapshot id %q is invalid", plan.SourceSnapshotID)
		}
		if cfg.SnapshotRuntimeSecretName == "" {
			return nil, fmt.Errorf("launcher: source snapshot restore requires authenticated snapshot runtime credentials")
		}
	}
	if cfg.MasterAddr == "" {
		return nil, fmt.Errorf("launcher: master addr is required")
	}
	if !lifecycle.IsSafeStorageIdentityComponent(plan.VolumeID) {
		return nil, fmt.Errorf("launcher: volume id %q is not a safe hostPath component", plan.VolumeID)
	}
	seenReplicaIDs := make(map[string]bool, len(plan.Replicas))
	for _, replica := range plan.Replicas {
		if !lifecycle.IsSafeStorageIdentityComponent(replica.ReplicaID) {
			return nil, fmt.Errorf("launcher: replica id %q is not a safe hostPath component", replica.ReplicaID)
		}
		if seenReplicaIDs[replica.ReplicaID] {
			return nil, fmt.Errorf("launcher: duplicate replica id %q would reuse one hostPath leaf", replica.ReplicaID)
		}
		seenReplicaIDs[replica.ReplicaID] = true
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
		volumeMounts := []volumeMount{stateVolumeMount(plan, replica, cfg)}
		volumes := []volume{stateVolume(plan, replica, cfg)}
		if cfg.SnapshotRuntimeSecretName != "" {
			volumeMounts = append(volumeMounts, volumeMount{Name: "snapshot-runtime-identity", MountPath: snapshotRuntimeMountPath, ReadOnly: true})
			volumes = append(volumes, volume{Name: "snapshot-runtime-identity", Secret: &secretVolumeSource{
				SecretName: cfg.SnapshotRuntimeSecretName,
				Items: []keyToPath{
					{Key: "ca.crt", Path: "ca.crt"},
					{Key: "tls.crt", Path: "tls.crt"},
					{Key: "tls.key", Path: "tls.key"},
					{Key: "token", Path: "token"},
				},
			}})
		}
		var securityContext *containerSecurityContext
		if plan.Protocol == "nvme" && plan.NVMeTransport == "rdma" {
			volumeMounts = append(volumeMounts,
				volumeMount{Name: "dev", MountPath: "/dev"},
				volumeMount{Name: "configfs", MountPath: "/sys/kernel/config"},
				volumeMount{Name: "modules-dir", MountPath: "/lib/modules", ReadOnly: true},
			)
			volumes = append(volumes,
				volume{Name: "dev", HostPath: &hostPath{Path: "/dev", Type: "Directory"}},
				volume{Name: "configfs", HostPath: &hostPath{Path: "/sys/kernel/config", Type: "Directory"}},
				volume{Name: "modules-dir", HostPath: &hostPath{Path: "/lib/modules", Type: "Directory"}},
			)
			securityContext = &containerSecurityContext{RunAsUser: int64Ptr(0), Privileged: boolPtr(true)}
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
						NodeSelector:   map[string]string{"kubernetes.io/hostname": replicaKubernetesNodeName(replica)},
						InitContainers: blockVolumeInitContainers(plan, replica, cfg),
						Containers: []container{{
							Name:            "blockvolume",
							Image:           cfg.Image,
							Command:         []string{"/usr/local/bin/blockvolume"},
							Args:            args,
							Env:             blockVolumeEnv(cfg),
							VolumeMounts:    volumeMounts,
							SecurityContext: securityContext,
						}},
						Volumes: volumes,
					},
				},
			},
		}
		raw, err := yaml.Marshal(deploy)
		if err != nil {
			return nil, fmt.Errorf("launcher: marshal %s: %w", name, err)
		}
		raw = append([]byte("---\n"), raw...)
		out = append(out, RenderedManifest{Name: name, YAML: raw})
	}
	return out, nil
}

func stateVolume(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload, cfg K8sRenderConfig) volume {
	if cfg.StateHostPathBase == "" {
		return volume{Name: "state", EmptyDir: &emptyDir{}}
	}
	return volume{Name: "state", HostPath: &hostPath{
		Path: path.Join(path.Clean(cfg.StateHostPathBase), plan.VolumeID, replica.ReplicaID),
		Type: "DirectoryOrCreate",
	}}
}

func stateVolumeMount(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload, cfg K8sRenderConfig) volumeMount {
	mountPath := stateMountPath
	if cfg.StateHostPathBase != "" {
		mountPath = durableRoot(plan, replica, cfg)
	}
	return volumeMount{Name: "state", MountPath: mountPath}
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
		"--durable-impl=" + cfg.DurableImpl,
		fmt.Sprintf("--durable-blocks=%d", plan.SizeBytes/4096),
		"--durable-blocksize=4096",
		"--recovery-mode=" + cfg.RecoveryMode,
		"--replication-ack=" + cfg.ReplicationAck,
	}
	if plan.SourceSnapshotID != "" {
		args = append(args, "--restore-snapshot-id="+plan.SourceSnapshotID)
	}
	if cfg.WALMultiBlockRecords {
		args = append(args, "--durable-wal-multiblock-records")
	}
	if cfg.WALRecoveryTestDisableFlusher {
		args = append(args, "--durable-wal-recovery-test-disable-flusher")
	}
	if cfg.EnableStatus {
		port, err := blockVolumeStatusPort(plan, replica)
		if err != nil {
			return nil, err
		}
		statusHost := "127.0.0.1"
		if cfg.ExternalStatus {
			host, err := hostFromAddr(replica.DataAddr)
			if err != nil {
				return nil, fmt.Errorf("launcher: external status volume=%s replica=%s: %w", plan.VolumeID, replica.ReplicaID, err)
			}
			statusHost = host
		}
		args = append(args, fmt.Sprintf("--status-addr=%s:%d", statusHost, port))
		if cfg.ExternalStatus {
			args = append(args, "--allow-external-status-bind")
		}
	}
	if cfg.SnapshotRuntimeSecretName != "" {
		port, err := blockVolumeSnapshotRuntimePort(plan, replica)
		if err != nil {
			return nil, err
		}
		host, err := hostFromAddr(replica.DataAddr)
		if err != nil {
			return nil, fmt.Errorf("launcher: snapshot runtime volume=%s replica=%s: %w", plan.VolumeID, replica.ReplicaID, err)
		}
		endpoint := "https://" + net.JoinHostPort(host, fmt.Sprintf("%d", port))
		args = append(args,
			fmt.Sprintf("--snapshot-runtime-listen=0.0.0.0:%d", port),
			"--snapshot-runtime-advertise="+endpoint,
			"--snapshot-runtime-tls-cert="+path.Join(snapshotRuntimeMountPath, "tls.crt"),
			"--snapshot-runtime-tls-key="+path.Join(snapshotRuntimeMountPath, "tls.key"),
			"--snapshot-runtime-client-ca="+path.Join(snapshotRuntimeMountPath, "ca.crt"),
			"--snapshot-runtime-token-file="+path.Join(snapshotRuntimeMountPath, "token"),
		)
	}
	switch plan.Protocol {
	case "nvme":
		nvmeTransport := plan.NVMeTransport
		if nvmeTransport == "" {
			nvmeTransport = "tcp"
		}
		if nvmeTransport != "tcp" && nvmeTransport != "rdma" {
			return nil, fmt.Errorf("launcher: NVMe transport %q invalid; want tcp or rdma", nvmeTransport)
		}
		if nvmeTransport == "rdma" && !cfg.ExternalNVMe {
			return nil, fmt.Errorf("launcher: NVMe/RDMA requires external NVMe node addressing")
		}
		nvmeListen := fmt.Sprintf("127.0.0.1:%d", replica.NVMeListenPort)
		if cfg.ExternalNVMe {
			host, err := hostFromAddr(replica.DataAddr)
			if err != nil {
				return nil, fmt.Errorf("launcher: external NVMe volume=%s replica=%s: %w", plan.VolumeID, replica.ReplicaID, err)
			}
			nvmeListen = fmt.Sprintf("%s:%d", host, replica.NVMeListenPort)
			args = append(args, "--allow-external-nvme-bind")
		}
		args = append(args,
			"--nvme-listen="+nvmeListen,
			"--nvme-transport="+nvmeTransport,
			"--nvme-subsysnqn="+replica.NVMeSubsystemNQN,
			fmt.Sprintf("--nvme-ns=%d", replica.NVMeNSID),
		)
		if cfg.NVMeMaxH2CDataLength != 0 && cfg.NVMeMaxH2CDataLength != 32768 && nvmeTransport == "rdma" {
			return nil, fmt.Errorf("launcher: NVMe MaxH2CDataLength is TCP-only")
		}
		if cfg.NVMeMaxH2CDataLength != 0 && nvmeTransport == "tcp" {
			args = append(args, fmt.Sprintf("--nvme-max-h2c-data-length=%d", cfg.NVMeMaxH2CDataLength))
		}
	default:
		iscsiListen := fmt.Sprintf("127.0.0.1:%d", replica.ISCSIListenPort)
		if cfg.ExternalISCSI {
			host, err := hostFromAddr(replica.DataAddr)
			if err != nil {
				return nil, fmt.Errorf("launcher: external iSCSI volume=%s replica=%s: %w", plan.VolumeID, replica.ReplicaID, err)
			}
			iscsiListen = fmt.Sprintf("%s:%d", host, replica.ISCSIListenPort)
			args = append(args, "--allow-external-iscsi-bind")
		}
		args = append(args,
			"--iscsi-listen="+iscsiListen,
			"--iscsi-iqn="+replica.ISCSIQualifiedName,
		)
	}
	return args, nil
}

func hostFromAddr(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("node address %q is not host:port: %w", addr, err)
	}
	if host == "" {
		return "", fmt.Errorf("node address %q has empty host", addr)
	}
	if isLocalhostOrLoopbackHost(host) {
		return "", fmt.Errorf("node address %q is loopback; external node-loss endpoints require non-loopback node addresses", addr)
	}
	return host, nil
}

func isLocalhostOrLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func replicaKubernetesNodeName(replica lifecycle.BlockVolumeReplicaWorkload) string {
	if replica.KubernetesNodeName != "" {
		return replica.KubernetesNodeName
	}
	return replica.ServerID
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

func blockVolumeSnapshotRuntimePort(plan lifecycle.BlockVolumeWorkloadPlan, replica lifecycle.BlockVolumeReplicaWorkload) (int, error) {
	const offset = 30000
	base := replica.ISCSIListenPort
	if plan.Protocol == "nvme" && replica.NVMeListenPort > 0 {
		base = replica.NVMeListenPort
	}
	if base <= 0 || base+offset > 65535 {
		return 0, fmt.Errorf("launcher: cannot derive snapshot runtime port for volume=%s replica=%s frontend_port=%d", plan.VolumeID, replica.ReplicaID, base)
	}
	return base + offset, nil
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
			MountPath: root,
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
	RunAsUser  *int64 `yaml:"runAsUser,omitempty"`
	Privileged *bool  `yaml:"privileged,omitempty"`
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
	ReadOnly  bool   `yaml:"readOnly,omitempty"`
}

type volume struct {
	Name     string              `yaml:"name"`
	EmptyDir *emptyDir           `yaml:"emptyDir,omitempty"`
	HostPath *hostPath           `yaml:"hostPath,omitempty"`
	Secret   *secretVolumeSource `yaml:"secret,omitempty"`
}

type secretVolumeSource struct {
	SecretName string      `yaml:"secretName"`
	Items      []keyToPath `yaml:"items,omitempty"`
}

type keyToPath struct {
	Key  string `yaml:"key"`
	Path string `yaml:"path"`
}

type emptyDir struct{}

type hostPath struct {
	Path string `yaml:"path"`
	Type string `yaml:"type,omitempty"`
}
