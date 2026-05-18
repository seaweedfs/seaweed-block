package launcher

import (
	"context"
	"fmt"

	"gopkg.in/yaml.v3"
)

const (
	LabelApp       = "app"
	AppBlockVolume = "sw-blockvolume"
	LabelVolume    = "sw-block.seaweedfs.com/volume"
	LabelReplica   = "sw-block.seaweedfs.com/replica"
)

type DeploymentIdentity struct {
	Namespace    string
	Name         string
	Labels       map[string]string
	SpecReplicas *int
}

type DeploymentClient interface {
	ApplyDeployment(context.Context, RenderedManifest) error
	DeleteDeployment(context.Context, DeploymentIdentity) error
}

type ReconcileDeploymentsInput struct {
	Namespace string
	Desired   []RenderedManifest
	Existing  []DeploymentIdentity
	Client    DeploymentClient
}

type ReconcileDeploymentsResult struct {
	Applied int
	Deleted int
	Skipped int
	Actions []ReconcileDeploymentAction
}

type ReconcileDeploymentAction struct {
	Action    string
	Namespace string
	Name      string
	Reason    string
}

// ReconcileBlockVolumeDeployments applies desired generated blockvolume
// Deployments and deletes only stale Deployments that are clearly owned by the
// Seaweed Block launcher identity contract.
func ReconcileBlockVolumeDeployments(ctx context.Context, in ReconcileDeploymentsInput) (ReconcileDeploymentsResult, error) {
	if in.Client == nil {
		return ReconcileDeploymentsResult{}, fmt.Errorf("launcher: deployment client is required")
	}
	var result ReconcileDeploymentsResult
	desiredKeys := make(map[string]bool, len(in.Desired))
	existingByKey := make(map[string]DeploymentIdentity, len(in.Existing))
	for _, existing := range in.Existing {
		existingByKey[deploymentKey(existing)] = existing
	}
	for _, manifest := range in.Desired {
		identity, err := DecodeRenderedDeploymentIdentity(manifest)
		if err != nil {
			return result, err
		}
		if err := validateManagedDeployment(identity); err != nil {
			return result, fmt.Errorf("launcher: desired manifest %s: %w", manifest.Name, err)
		}
		if in.Namespace != "" && identity.Namespace != in.Namespace {
			return result, fmt.Errorf(
				"launcher: desired manifest %s namespace=%q does not match managed namespace=%q",
				manifest.Name, identity.Namespace, in.Namespace,
			)
		}
		desiredKeys[deploymentKey(identity)] = true
		if existing, ok := existingByKey[deploymentKey(identity)]; ok {
			reason := "already-exists"
			if existing.SpecReplicas != nil && *existing.SpecReplicas == 0 {
				reason = "preserve-replicas-zero"
			}
			result.Skipped++
			result.Actions = append(result.Actions, ReconcileDeploymentAction{
				Action:    "skip",
				Namespace: identity.Namespace,
				Name:      identity.Name,
				Reason:    reason,
			})
			continue
		}
		if err := in.Client.ApplyDeployment(ctx, manifest); err != nil {
			return result, fmt.Errorf("launcher: apply %s: %w", manifest.Name, err)
		}
		result.Applied++
		result.Actions = append(result.Actions, ReconcileDeploymentAction{
			Action:    "apply",
			Namespace: identity.Namespace,
			Name:      identity.Name,
			Reason:    "desired",
		})
	}

	for _, existing := range in.Existing {
		reason := skipDeleteReason(in.Namespace, existing)
		if reason != "" {
			result.Skipped++
			result.Actions = append(result.Actions, ReconcileDeploymentAction{
				Action:    "skip",
				Namespace: existing.Namespace,
				Name:      existing.Name,
				Reason:    reason,
			})
			continue
		}
		if desiredKeys[deploymentKey(existing)] {
			continue
		}
		if err := in.Client.DeleteDeployment(ctx, existing); err != nil {
			return result, fmt.Errorf("launcher: delete %s/%s: %w", existing.Namespace, existing.Name, err)
		}
		result.Deleted++
		result.Actions = append(result.Actions, ReconcileDeploymentAction{
			Action:    "delete",
			Namespace: existing.Namespace,
			Name:      existing.Name,
			Reason:    "stale-owned",
		})
	}
	return result, nil
}

func skipDeleteReason(managedNamespace string, identity DeploymentIdentity) string {
	if managedNamespace != "" && identity.Namespace != managedNamespace {
		return "skip-out-of-namespace"
	}
	if identity.Labels[LabelApp] != AppBlockVolume {
		return "skip-unmanaged-app"
	}
	if identity.Labels[LabelVolume] == "" || identity.Labels[LabelReplica] == "" {
		return "skip-missing-identity-label"
	}
	if identity.Name != workloadName(identity.Labels[LabelVolume], identity.Labels[LabelReplica]) {
		return "skip-name-mismatch"
	}
	return ""
}

func validateManagedDeployment(identity DeploymentIdentity) error {
	if identity.Name == "" {
		return fmt.Errorf("missing deployment name")
	}
	if identity.Labels[LabelApp] != AppBlockVolume {
		return fmt.Errorf("missing %s=%s", LabelApp, AppBlockVolume)
	}
	if identity.Labels[LabelVolume] == "" {
		return fmt.Errorf("missing %s label", LabelVolume)
	}
	if identity.Labels[LabelReplica] == "" {
		return fmt.Errorf("missing %s label", LabelReplica)
	}
	if identity.Name != workloadName(identity.Labels[LabelVolume], identity.Labels[LabelReplica]) {
		return fmt.Errorf("deployment name %q does not match generated identity", identity.Name)
	}
	return nil
}

func deploymentKey(identity DeploymentIdentity) string {
	return identity.Namespace + "/" + identity.Name
}

func DecodeRenderedDeploymentIdentity(manifest RenderedManifest) (DeploymentIdentity, error) {
	var doc struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name      string            `yaml:"name"`
			Namespace string            `yaml:"namespace"`
			Labels    map[string]string `yaml:"labels"`
		} `yaml:"metadata"`
	}
	if err := yaml.Unmarshal(manifest.YAML, &doc); err != nil {
		return DeploymentIdentity{}, fmt.Errorf("launcher: parse manifest %s: %w", manifest.Name, err)
	}
	if doc.Kind != "Deployment" {
		return DeploymentIdentity{}, fmt.Errorf("launcher: manifest %s kind=%q, expected Deployment", manifest.Name, doc.Kind)
	}
	if doc.Metadata.Name == "" {
		doc.Metadata.Name = manifest.Name
	}
	return DeploymentIdentity{
		Namespace: doc.Metadata.Namespace,
		Name:      doc.Metadata.Name,
		Labels:    doc.Metadata.Labels,
	}, nil
}
