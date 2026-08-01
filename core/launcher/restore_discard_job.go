package launcher

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"gopkg.in/yaml.v3"
)

const (
	AppRestoreDiscard                         = "sw-block-restore-discard"
	AnnotationDiscardOperation                = "sw-block.seaweedfs.com/discard-operation"
	AnnotationDiscardSnapshot                 = "sw-block.seaweedfs.com/discard-snapshot"
	AnnotationDiscardVolume                   = "sw-block.seaweedfs.com/discard-volume"
	AnnotationDiscardReplica                  = "sw-block.seaweedfs.com/discard-replica"
	AnnotationDiscardNode                     = "sw-block.seaweedfs.com/discard-node"
	LabelDiscardOperationHash                 = "sw-block.seaweedfs.com/discard-operation-hash"
	restoreDiscardEvidencePath                = "/dev/termination-log"
	restoreDiscardActiveDeadlineSeconds int64 = 120
)

type RestoreDiscardJobConfig struct {
	Namespace         string
	Image             string
	StateHostPathBase string
}

type RestoreDiscardJobIdentity struct {
	Namespace          string
	Name               string
	OperationID        string
	SnapshotID         string
	VolumeID           string
	ReplicaID          string
	KubernetesNodeName string
}

func RenderRestoreDiscardJob(record lifecycle.VolumeRecord, replica lifecycle.RestoreAbortReplica, cfg RestoreDiscardJobConfig) (RenderedManifest, error) {
	if record.RestoreState != lifecycle.VolumeRestoreAbortRequested || record.RestoreAbort == nil {
		return RenderedManifest{}, fmt.Errorf("launcher: volume %q is not awaiting restore discard", record.Spec.VolumeID)
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "kube-system"
	}
	if cfg.Image == "" || cfg.StateHostPathBase == "" || !path.IsAbs(cfg.StateHostPathBase) || path.Clean(cfg.StateHostPathBase) == "/" {
		return RenderedManifest{}, fmt.Errorf("launcher: restore discard image and state hostPath base are required")
	}
	if !lifecycle.IsSafeStorageIdentityComponent(record.Spec.VolumeID) || !lifecycle.IsSafeStorageIdentityComponent(record.RestoreAbort.OperationID) || !lifecycle.IsSafeStorageIdentityComponent(record.RestoreAbort.SnapshotID) || !lifecycle.IsSafeStorageIdentityComponent(replica.ReplicaID) || !lifecycle.IsSafeStorageIdentityComponent(replica.KubernetesNodeName) {
		return RenderedManifest{}, fmt.Errorf("launcher: invalid restore discard identity")
	}
	found := false
	for _, expected := range record.RestoreAbort.Replicas {
		if expected.ServerID == replica.ServerID && expected.ReplicaID == replica.ReplicaID && expected.KubernetesNodeName == replica.KubernetesNodeName {
			found = true
			break
		}
	}
	if !found {
		return RenderedManifest{}, fmt.Errorf("launcher: restore discard replica is not in the durable abort operation")
	}

	identity := RestoreDiscardJobIdentity{
		Namespace:          cfg.Namespace,
		Name:               restoreDiscardJobName(record.RestoreAbort.OperationID, record.Spec.VolumeID, replica.ReplicaID),
		OperationID:        record.RestoreAbort.OperationID,
		SnapshotID:         record.RestoreAbort.SnapshotID,
		VolumeID:           record.Spec.VolumeID,
		ReplicaID:          replica.ReplicaID,
		KubernetesNodeName: replica.KubernetesNodeName,
	}
	annotations := restoreDiscardAnnotations(identity)
	labels := map[string]string{
		LabelApp:                  AppRestoreDiscard,
		LabelDiscardOperationHash: restoreDiscardIdentityHash(identity.OperationID),
	}
	doc := restoreDiscardJob{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: restoreDiscardMetadata{
			Name: identity.Name, Namespace: identity.Namespace, Labels: labels, Annotations: annotations,
		},
		Spec: restoreDiscardJobSpec{
			BackoffLimit:          intPtr(0),
			ActiveDeadlineSeconds: int64Ptr(restoreDiscardActiveDeadlineSeconds),
			Template: restoreDiscardPodTemplate{
				Metadata: restoreDiscardMetadata{Labels: labels, Annotations: annotations},
				Spec: restoreDiscardPodSpec{
					AutomountServiceAccountToken: boolPtr(false),
					RestartPolicy:                "Never",
					NodeSelector:                 map[string]string{lifecycle.KubernetesNodeNameLabel: identity.KubernetesNodeName},
					Containers: []restoreDiscardContainer{{
						Name:    "restore-discard",
						Image:   cfg.Image,
						Command: []string{"/usr/local/bin/blockvolume"},
						Args: []string{
							"restore-discard",
							"--root=" + stateMountPath,
							"--operation-id=" + identity.OperationID,
							"--snapshot-id=" + identity.SnapshotID,
							"--volume-id=" + identity.VolumeID,
							"--replica-id=" + identity.ReplicaID,
							"--allow-activated",
							"--evidence-file=" + restoreDiscardEvidencePath,
						},
						TerminationMessagePath:   restoreDiscardEvidencePath,
						TerminationMessagePolicy: "FallbackToLogsOnError",
						VolumeMounts:             []volumeMount{{Name: "state", MountPath: stateMountPath}},
					}},
					Volumes: []volume{{Name: "state", HostPath: &hostPath{
						Path: path.Join(path.Clean(cfg.StateHostPathBase), identity.VolumeID, identity.ReplicaID), Type: "Directory",
					}}},
				},
			},
		},
	}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return RenderedManifest{}, fmt.Errorf("launcher: marshal restore discard job: %w", err)
	}
	return RenderedManifest{Name: identity.Name, YAML: append([]byte("---\n"), raw...)}, nil
}

func DecodeRestoreDiscardJobIdentity(manifest RenderedManifest) (RestoreDiscardJobIdentity, error) {
	var doc struct {
		Kind     string                 `yaml:"kind"`
		Metadata restoreDiscardMetadata `yaml:"metadata"`
	}
	if err := yaml.Unmarshal(manifest.YAML, &doc); err != nil {
		return RestoreDiscardJobIdentity{}, fmt.Errorf("launcher: parse restore discard job: %w", err)
	}
	if doc.Kind != "Job" || doc.Metadata.Labels[LabelApp] != AppRestoreDiscard {
		return RestoreDiscardJobIdentity{}, fmt.Errorf("launcher: manifest is not an owned restore discard Job")
	}
	identity := restoreDiscardIdentityFromAnnotations(doc.Metadata.Namespace, doc.Metadata.Name, doc.Metadata.Annotations)
	if err := validateRestoreDiscardJobIdentity(identity); err != nil {
		return RestoreDiscardJobIdentity{}, err
	}
	if identity.Name != restoreDiscardJobName(identity.OperationID, identity.VolumeID, identity.ReplicaID) || doc.Metadata.Labels[LabelDiscardOperationHash] != restoreDiscardIdentityHash(identity.OperationID) {
		return RestoreDiscardJobIdentity{}, fmt.Errorf("launcher: restore discard Job name or operation hash mismatch")
	}
	return identity, nil
}

func restoreDiscardJobName(operationID, volumeID, replicaID string) string {
	digest := sha256.Sum256([]byte(operationID + "\x00" + volumeID + "\x00" + replicaID))
	return "sw-block-discard-" + hex.EncodeToString(digest[:12])
}

func restoreDiscardIdentityHash(operationID string) string {
	digest := sha256.Sum256([]byte(operationID))
	return hex.EncodeToString(digest[:16])
}

func restoreDiscardAnnotations(identity RestoreDiscardJobIdentity) map[string]string {
	return map[string]string{
		AnnotationDiscardOperation: identity.OperationID,
		AnnotationDiscardSnapshot:  identity.SnapshotID,
		AnnotationDiscardVolume:    identity.VolumeID,
		AnnotationDiscardReplica:   identity.ReplicaID,
		AnnotationDiscardNode:      identity.KubernetesNodeName,
	}
}

func restoreDiscardIdentityFromAnnotations(namespace, name string, annotations map[string]string) RestoreDiscardJobIdentity {
	return RestoreDiscardJobIdentity{
		Namespace:          namespace,
		Name:               name,
		OperationID:        annotations[AnnotationDiscardOperation],
		SnapshotID:         annotations[AnnotationDiscardSnapshot],
		VolumeID:           annotations[AnnotationDiscardVolume],
		ReplicaID:          annotations[AnnotationDiscardReplica],
		KubernetesNodeName: annotations[AnnotationDiscardNode],
	}
}

func validateRestoreDiscardJobIdentity(identity RestoreDiscardJobIdentity) error {
	if identity.Namespace == "" || identity.Name == "" || !lifecycle.IsSafeStorageIdentityComponent(identity.OperationID) || !lifecycle.IsSafeStorageIdentityComponent(identity.SnapshotID) || !lifecycle.IsSafeStorageIdentityComponent(identity.VolumeID) || !lifecycle.IsSafeStorageIdentityComponent(identity.ReplicaID) || !lifecycle.IsSafeStorageIdentityComponent(identity.KubernetesNodeName) {
		return fmt.Errorf("launcher: incomplete restore discard Job identity")
	}
	return nil
}

type restoreDiscardJob struct {
	APIVersion string                 `yaml:"apiVersion"`
	Kind       string                 `yaml:"kind"`
	Metadata   restoreDiscardMetadata `yaml:"metadata"`
	Spec       restoreDiscardJobSpec  `yaml:"spec"`
}

type restoreDiscardMetadata struct {
	Name        string            `yaml:"name,omitempty"`
	Namespace   string            `yaml:"namespace,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

type restoreDiscardJobSpec struct {
	BackoffLimit          *int                      `yaml:"backoffLimit"`
	ActiveDeadlineSeconds *int64                    `yaml:"activeDeadlineSeconds"`
	Template              restoreDiscardPodTemplate `yaml:"template"`
}

type restoreDiscardPodTemplate struct {
	Metadata restoreDiscardMetadata `yaml:"metadata"`
	Spec     restoreDiscardPodSpec  `yaml:"spec"`
}

type restoreDiscardPodSpec struct {
	AutomountServiceAccountToken *bool                     `yaml:"automountServiceAccountToken"`
	RestartPolicy                string                    `yaml:"restartPolicy"`
	NodeSelector                 map[string]string         `yaml:"nodeSelector"`
	Containers                   []restoreDiscardContainer `yaml:"containers"`
	Volumes                      []volume                  `yaml:"volumes"`
}

type restoreDiscardContainer struct {
	Name                     string        `yaml:"name"`
	Image                    string        `yaml:"image"`
	Command                  []string      `yaml:"command"`
	Args                     []string      `yaml:"args"`
	TerminationMessagePath   string        `yaml:"terminationMessagePath"`
	TerminationMessagePolicy string        `yaml:"terminationMessagePolicy"`
	VolumeMounts             []volumeMount `yaml:"volumeMounts"`
}
