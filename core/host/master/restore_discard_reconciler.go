package master

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/launcher"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
)

type RestoreDiscardKubernetesClient interface {
	ListBlockVolumeDeployments(context.Context, string) ([]launcher.DeploymentIdentity, error)
	ListBlockVolumePods(context.Context, string, string, string) ([]launcher.BlockVolumePodIdentity, error)
	GetRestoreDiscardJob(context.Context, launcher.RestoreDiscardJobIdentity) (launcher.RestoreDiscardJobObservation, bool, error)
	ListRestoreDiscardPods(context.Context, launcher.RestoreDiscardJobIdentity) ([]launcher.RestoreDiscardPodObservation, error)
	ApplyRestoreDiscardJob(context.Context, launcher.RenderedManifest) error
	DeleteRestoreDiscardJob(context.Context, launcher.RestoreDiscardJobIdentity) error
}

type RestoreDiscardReconcileConfig struct {
	Namespace         string
	Image             string
	StateHostPathBase string
	MaxAttempts       uint32
	RetryBaseDelay    time.Duration
	Now               func() time.Time
}

type RestoreDiscardReconcileResult struct {
	Volumes             int
	WaitingForWorkloads int
	JobsCreated         int
	JobsActive          int
	JobsFailed          int
	JobsRetryWaiting    int
	TerminalFailures    int
	EvidenceRecorded    int
	JobsDeleted         int
	VolumesDiscarded    int
}

func (h *Host) RunSnapshotRestoreDiscardTick(ctx context.Context, cfg RestoreDiscardReconcileConfig, client RestoreDiscardKubernetesClient) (RestoreDiscardReconcileResult, error) {
	if h == nil || h.lifecycle == nil || h.lifecycle.Volumes == nil || client == nil {
		return RestoreDiscardReconcileResult{}, fmt.Errorf("snapshot: restore discard reconciler is not configured")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "kube-system"
	}
	if cfg.Image == "" || cfg.StateHostPathBase == "" {
		return RestoreDiscardReconcileResult{}, fmt.Errorf("snapshot: restore discard image and state hostPath base are required")
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	var result RestoreDiscardReconcileResult
	for _, record := range h.lifecycle.Volumes.ListVolumes() {
		if record.RestoreState != lifecycle.VolumeRestoreAbortRequested || record.RestoreAbort == nil {
			continue
		}
		result.Volumes++
		for _, replica := range record.RestoreAbort.Replicas {
			now := cfg.Now().UTC()
			manifest, err := launcher.RenderRestoreDiscardJob(record, replica, launcher.RestoreDiscardJobConfig{
				Namespace: cfg.Namespace, Image: cfg.Image, StateHostPathBase: cfg.StateHostPathBase,
			})
			if err != nil {
				return result, err
			}
			identity, err := launcher.DecodeRestoreDiscardJobIdentity(manifest)
			if err != nil {
				return result, err
			}
			if replica.State == lifecycle.RestoreDiscardSucceeded {
				deleted, err := removeCompletedRestoreDiscardJob(ctx, client, identity)
				if err != nil {
					return result, err
				}
				if deleted {
					result.JobsDeleted++
				}
				continue
			}
			if replica.State == lifecycle.RestoreDiscardTerminalFailure {
				result.TerminalFailures++
				deleted, err := removeCompletedRestoreDiscardJob(ctx, client, identity)
				if err != nil {
					return result, err
				}
				if deleted {
					result.JobsDeleted++
				}
				continue
			}
			job, exists, err := client.GetRestoreDiscardJob(ctx, identity)
			if err != nil {
				return result, err
			}
			fenced, fenceReason, err := h.restoreDiscardExecutionFence(ctx, cfg, client, record.Spec.VolumeID, record.RestoreAbort.OperationID, replica.ReplicaID)
			if err != nil {
				return result, err
			}
			if !fenced {
				result.WaitingForWorkloads++
				if replica.State == lifecycle.RestoreDiscardRunning {
					result.JobsFailed++
					reason := "restore discard execution fence lost: " + fenceReason
					terminal, err := h.persistRestoreDiscardFailure(record, replica, cfg, now, reason, "job/"+identity.Namespace+"/"+identity.Name)
					if err != nil {
						return result, err
					}
					if terminal {
						result.TerminalFailures++
					} else {
						result.JobsRetryWaiting++
					}
				}
				if exists {
					if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
						return result, err
					}
					result.JobsDeleted++
				}
				continue
			}
			if replica.State == lifecycle.RestoreDiscardRetryWait {
				if exists {
					if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
						return result, err
					}
					result.JobsDeleted++
					continue
				}
				oldPods, err := client.ListRestoreDiscardPods(ctx, identity)
				if err != nil {
					return result, err
				}
				if len(oldPods) != 0 || now.Before(replica.RetryNotBefore) {
					result.JobsRetryWaiting++
					continue
				}
				updated, running, err := h.lifecycle.Volumes.BeginRestoreDiscardAttempt(record.Spec.VolumeID, record.RestoreAbort.OperationID, replica.ServerID, replica.ReplicaID, now)
				if err != nil {
					return result, err
				}
				manifest, err = launcher.RenderRestoreDiscardJob(updated, running, launcher.RestoreDiscardJobConfig{
					Namespace: cfg.Namespace, Image: cfg.Image, StateHostPathBase: cfg.StateHostPathBase,
				})
				if err != nil {
					return result, err
				}
				fenced, fenceReason, err = h.restoreDiscardExecutionFence(ctx, cfg, client, updated.Spec.VolumeID, updated.RestoreAbort.OperationID, running.ReplicaID)
				if err != nil {
					return result, err
				}
				if !fenced {
					result.WaitingForWorkloads++
					result.JobsFailed++
					terminal, err := h.persistRestoreDiscardFailure(updated, running, cfg, now, "restore discard execution fence lost: "+fenceReason, "job/"+identity.Namespace+"/"+identity.Name)
					if err != nil {
						return result, err
					}
					if terminal {
						result.TerminalFailures++
					} else {
						result.JobsRetryWaiting++
					}
					continue
				}
				if err := client.ApplyRestoreDiscardJob(ctx, manifest); err != nil {
					return result, err
				}
				result.JobsCreated++
				continue
			}
			if replica.State == lifecycle.RestoreDiscardPending {
				updated, running, err := h.lifecycle.Volumes.BeginRestoreDiscardAttempt(record.Spec.VolumeID, record.RestoreAbort.OperationID, replica.ServerID, replica.ReplicaID, now)
				if err != nil {
					return result, err
				}
				manifest, err = launcher.RenderRestoreDiscardJob(updated, running, launcher.RestoreDiscardJobConfig{
					Namespace: cfg.Namespace, Image: cfg.Image, StateHostPathBase: cfg.StateHostPathBase,
				})
				if err != nil {
					return result, err
				}
				fenced, fenceReason, err = h.restoreDiscardExecutionFence(ctx, cfg, client, updated.Spec.VolumeID, updated.RestoreAbort.OperationID, running.ReplicaID)
				if err != nil {
					return result, err
				}
				if !fenced {
					result.WaitingForWorkloads++
					result.JobsFailed++
					terminal, err := h.persistRestoreDiscardFailure(updated, running, cfg, now, "restore discard execution fence lost: "+fenceReason, "job/"+identity.Namespace+"/"+identity.Name)
					if err != nil {
						return result, err
					}
					if terminal {
						result.TerminalFailures++
					} else {
						result.JobsRetryWaiting++
					}
					continue
				}
				if err := client.ApplyRestoreDiscardJob(ctx, manifest); err != nil {
					return result, err
				}
				result.JobsCreated++
				continue
			}
			if replica.State != lifecycle.RestoreDiscardRunning {
				return result, fmt.Errorf("snapshot: invalid restore discard state %q", replica.State)
			}
			if !exists {
				oldPods, err := client.ListRestoreDiscardPods(ctx, identity)
				if err != nil {
					return result, err
				}
				if len(oldPods) != 0 {
					result.JobsRetryWaiting++
					continue
				}
				fenced, fenceReason, err = h.restoreDiscardExecutionFence(ctx, cfg, client, record.Spec.VolumeID, record.RestoreAbort.OperationID, replica.ReplicaID)
				if err != nil {
					return result, err
				}
				if !fenced {
					result.WaitingForWorkloads++
					result.JobsFailed++
					terminal, err := h.persistRestoreDiscardFailure(record, replica, cfg, now, "restore discard execution fence lost: "+fenceReason, "job/"+identity.Namespace+"/"+identity.Name)
					if err != nil {
						return result, err
					}
					if terminal {
						result.TerminalFailures++
					} else {
						result.JobsRetryWaiting++
					}
					continue
				}
				if err := client.ApplyRestoreDiscardJob(ctx, manifest); err != nil {
					return result, err
				}
				result.JobsCreated++
				continue
			}
			if job.Failed != 0 {
				result.JobsFailed++
				jobPods, err := client.ListRestoreDiscardPods(ctx, identity)
				if err != nil {
					return result, err
				}
				reason, evidenceRef := restoreDiscardFailureEvidence(identity, job.FailureReason, jobPods)
				terminal, err := h.persistRestoreDiscardFailure(record, replica, cfg, now, reason, evidenceRef)
				if err != nil {
					return result, err
				}
				if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
					return result, err
				}
				result.JobsDeleted++
				if terminal {
					result.TerminalFailures++
				} else {
					result.JobsRetryWaiting++
				}
				continue
			}
			if job.Succeeded == 0 && restoreDiscardJobDeadlineExceeded(job, now) {
				result.JobsFailed++
				reason := fmt.Sprintf("restore discard Job exceeded active deadline of %s", time.Duration(job.ActiveDeadlineSeconds)*time.Second)
				evidenceRef := "job/" + identity.Namespace + "/" + identity.Name
				terminal, err := h.persistRestoreDiscardFailure(record, replica, cfg, now, reason, evidenceRef)
				if err != nil {
					return result, err
				}
				if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
					return result, err
				}
				result.JobsDeleted++
				if terminal {
					result.TerminalFailures++
				} else {
					result.JobsRetryWaiting++
				}
				continue
			}
			if job.Succeeded == 0 {
				result.JobsActive++
				continue
			}
			jobPods, err := client.ListRestoreDiscardPods(ctx, identity)
			if err != nil {
				return result, err
			}
			evidence, evidenceRef, err := verifiedRestoreDiscardEvidence(identity, jobPods)
			if err != nil {
				result.JobsFailed++
				reason := "invalid restore discard terminal evidence: " + err.Error()
				failureRef := "job/" + identity.Namespace + "/" + identity.Name
				if len(jobPods) != 0 {
					failureRef += "/pod/" + jobPods[0].Name
				}
				terminal, persistErr := h.persistRestoreDiscardFailure(record, replica, cfg, now, reason, failureRef)
				if persistErr != nil {
					return result, persistErr
				}
				if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
					return result, err
				}
				result.JobsDeleted++
				if terminal {
					result.TerminalFailures++
				} else {
					result.JobsRetryWaiting++
				}
				continue
			}
			if _, err := h.lifecycle.Volumes.RecordRestoreDiscard(record.Spec.VolumeID, record.RestoreAbort.OperationID, lifecycle.RestoreAbortReplica{
				ServerID: replica.ServerID, KubernetesNodeName: replica.KubernetesNodeName, ReplicaID: replica.ReplicaID,
				State: lifecycle.RestoreDiscardSucceeded, Attempt: replica.Attempt, MarkerRemoved: evidence.MarkerRemoved, DataRemoved: evidence.DataRemoved, EvidenceRef: evidenceRef,
			}); err != nil {
				return result, err
			}
			result.EvidenceRecorded++
			if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
				return result, err
			}
			result.JobsDeleted++
		}
		latest, ok := h.lifecycle.Volumes.GetVolume(record.Spec.VolumeID)
		if !ok || latest.RestoreState != lifecycle.VolumeRestoreAbortRequested || latest.RestoreAbort == nil || !restoreDiscardEvidenceComplete(*latest.RestoreAbort) {
			continue
		}
		clean := true
		for _, replica := range latest.RestoreAbort.Replicas {
			manifest, err := launcher.RenderRestoreDiscardJob(latest, replica, launcher.RestoreDiscardJobConfig{
				Namespace: cfg.Namespace, Image: cfg.Image, StateHostPathBase: cfg.StateHostPathBase,
			})
			if err != nil {
				return result, err
			}
			identity, err := launcher.DecodeRestoreDiscardJobIdentity(manifest)
			if err != nil {
				return result, err
			}
			_, exists, err := client.GetRestoreDiscardJob(ctx, identity)
			if err != nil {
				return result, err
			}
			pods, err := client.ListRestoreDiscardPods(ctx, identity)
			if err != nil {
				return result, err
			}
			if exists || len(pods) != 0 {
				clean = false
			}
		}
		if clean {
			if _, err := h.lifecycle.Volumes.MarkRestoreDiscarded(latest.Spec.VolumeID, latest.RestoreAbort.OperationID); err != nil {
				return result, err
			}
			result.VolumesDiscarded++
		}
	}
	return result, nil
}

func restoreDiscardRetryDelay(base time.Duration, attempt uint32) time.Duration {
	shift := attempt - 1
	if shift > 10 {
		shift = 10
	}
	return base * time.Duration(uint64(1)<<shift)
}

func restoreDiscardJobDeadlineExceeded(job launcher.RestoreDiscardJobObservation, now time.Time) bool {
	if job.CreatedAt.IsZero() || job.ActiveDeadlineSeconds <= 0 {
		return false
	}
	deadline := job.CreatedAt.Add(time.Duration(job.ActiveDeadlineSeconds) * time.Second)
	return !now.Before(deadline)
}

func (h *Host) persistRestoreDiscardFailure(record lifecycle.VolumeRecord, replica lifecycle.RestoreAbortReplica, cfg RestoreDiscardReconcileConfig, now time.Time, reason, evidenceRef string) (bool, error) {
	reason = strings.TrimSpace(reason)
	if len(reason) > 512 {
		reason = reason[:512]
	}
	updated, err := h.lifecycle.Volumes.RecordRestoreDiscardFailure(
		record.Spec.VolumeID, record.RestoreAbort.OperationID, replica.ServerID, replica.ReplicaID,
		replica.Attempt, cfg.MaxAttempts, reason, evidenceRef, now, restoreDiscardRetryDelay(cfg.RetryBaseDelay, replica.Attempt),
	)
	if err != nil {
		return false, err
	}
	updatedReplica, ok := restoreDiscardReplica(updated, replica.ServerID, replica.ReplicaID)
	return ok && updatedReplica.State == lifecycle.RestoreDiscardTerminalFailure, nil
}

func restoreDiscardFailureEvidence(identity launcher.RestoreDiscardJobIdentity, jobFailureReason string, pods []launcher.RestoreDiscardPodObservation) (string, string) {
	reason := "restore discard Job failed"
	if jobFailureReason = strings.TrimSpace(jobFailureReason); jobFailureReason != "" {
		reason += ": " + jobFailureReason
	}
	evidenceRef := "job/" + identity.Namespace + "/" + identity.Name
	for _, pod := range pods {
		if !pod.Terminated || pod.ExitCode == 0 {
			continue
		}
		evidenceRef += "/pod/" + pod.Name
		if message := strings.TrimSpace(pod.Message); message != "" {
			if len(message) > 480 {
				message = message[:480]
			}
			reason += ": " + message
		}
		break
	}
	return reason, evidenceRef
}

func restoreDiscardReplica(record lifecycle.VolumeRecord, serverID, replicaID string) (lifecycle.RestoreAbortReplica, bool) {
	if record.RestoreAbort == nil {
		return lifecycle.RestoreAbortReplica{}, false
	}
	for _, replica := range record.RestoreAbort.Replicas {
		if replica.ServerID == serverID && replica.ReplicaID == replicaID {
			return replica, true
		}
	}
	return lifecycle.RestoreAbortReplica{}, false
}

func (h *Host) restoreDiscardTargetStillFenced(volumeID, operationID string) bool {
	record, ok := h.lifecycle.Volumes.GetVolume(volumeID)
	if !ok || record.RestoreState != lifecycle.VolumeRestoreAbortRequested || record.RestoreAbort == nil || record.RestoreAbort.OperationID != operationID || record.AttachedTo != "" {
		return false
	}
	line, ok := h.Publisher().VolumeAuthorityLine(volumeID)
	return !ok || !line.Assigned
}

func (h *Host) restoreDiscardExecutionFence(ctx context.Context, cfg RestoreDiscardReconcileConfig, client RestoreDiscardKubernetesClient, volumeID, operationID, replicaID string) (bool, string, error) {
	deployments, err := client.ListBlockVolumeDeployments(ctx, cfg.Namespace)
	if err != nil {
		return false, "", err
	}
	if restoreDiscardDeploymentExists(deployments, volumeID, replicaID) {
		return false, "blockvolume Deployment exists", nil
	}
	pods, err := client.ListBlockVolumePods(ctx, cfg.Namespace, volumeID, replicaID)
	if err != nil {
		return false, "", err
	}
	if len(pods) != 0 {
		return false, "blockvolume Pod exists", nil
	}
	if !h.restoreDiscardTargetStillFenced(volumeID, operationID) {
		return false, "target attach or authority fence is not closed", nil
	}
	return true, "", nil
}

func restoreDiscardDeploymentExists(deployments []launcher.DeploymentIdentity, volumeID, replicaID string) bool {
	for _, deployment := range deployments {
		if deployment.Labels[launcher.LabelApp] == launcher.AppBlockVolume && deployment.Labels[launcher.LabelVolume] == volumeID && deployment.Labels[launcher.LabelReplica] == replicaID {
			return true
		}
	}
	return false
}

func verifiedRestoreDiscardEvidence(identity launcher.RestoreDiscardJobIdentity, pods []launcher.RestoreDiscardPodObservation) (snapshot.RestoreDiscardResult, string, error) {
	if len(pods) != 1 {
		return snapshot.RestoreDiscardResult{}, "", fmt.Errorf("snapshot: restore discard Job %s has %d Pods, want 1", identity.Name, len(pods))
	}
	pod := pods[0]
	if pod.Phase != "Succeeded" || !pod.Terminated || pod.ExitCode != 0 || pod.NodeName != identity.KubernetesNodeName {
		return snapshot.RestoreDiscardResult{}, "", fmt.Errorf("snapshot: restore discard Pod %s has invalid terminal state", pod.Name)
	}
	var evidence snapshot.RestoreDiscardResult
	if err := json.Unmarshal([]byte(pod.Message), &evidence); err != nil {
		return snapshot.RestoreDiscardResult{}, "", fmt.Errorf("snapshot: decode restore discard evidence: %w", err)
	}
	if evidence.OperationID != identity.OperationID || evidence.SnapshotID != identity.SnapshotID || evidence.TargetVolumeID != identity.VolumeID || evidence.TargetReplicaID != identity.ReplicaID || !evidence.MarkerRemoved || !evidence.DataRemoved {
		return snapshot.RestoreDiscardResult{}, "", fmt.Errorf("snapshot: restore discard evidence identity or terminal facts mismatch")
	}
	return evidence, "job/" + identity.Namespace + "/" + identity.Name + "/pod/" + pod.Name, nil
}

func removeCompletedRestoreDiscardJob(ctx context.Context, client RestoreDiscardKubernetesClient, identity launcher.RestoreDiscardJobIdentity) (bool, error) {
	_, exists, err := client.GetRestoreDiscardJob(ctx, identity)
	if err != nil || !exists {
		return false, err
	}
	if err := client.DeleteRestoreDiscardJob(ctx, identity); err != nil {
		return false, err
	}
	return true, nil
}

func restoreDiscardEvidenceComplete(abort lifecycle.RestoreAbortRecord) bool {
	for _, replica := range abort.Replicas {
		if replica.State != lifecycle.RestoreDiscardSucceeded {
			return false
		}
	}
	return true
}
