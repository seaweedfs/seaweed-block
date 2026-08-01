package master

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/launcher"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
)

type fakeRestoreDiscardClient struct {
	deployments []launcher.DeploymentIdentity
	blockPods   []launcher.BlockVolumePodIdentity
	jobs        map[string]launcher.RestoreDiscardJobObservation
	jobPods     map[string][]launcher.RestoreDiscardPodObservation
	created     int
	deleted     int
}

func (c *fakeRestoreDiscardClient) ListBlockVolumeDeployments(context.Context, string) ([]launcher.DeploymentIdentity, error) {
	return c.deployments, nil
}
func (c *fakeRestoreDiscardClient) ListBlockVolumePods(context.Context, string, string, string) ([]launcher.BlockVolumePodIdentity, error) {
	return c.blockPods, nil
}
func (c *fakeRestoreDiscardClient) GetRestoreDiscardJob(_ context.Context, identity launcher.RestoreDiscardJobIdentity) (launcher.RestoreDiscardJobObservation, bool, error) {
	job, ok := c.jobs[identity.Name]
	return job, ok, nil
}
func (c *fakeRestoreDiscardClient) ListRestoreDiscardPods(_ context.Context, identity launcher.RestoreDiscardJobIdentity) ([]launcher.RestoreDiscardPodObservation, error) {
	return c.jobPods[identity.Name], nil
}
func (c *fakeRestoreDiscardClient) ApplyRestoreDiscardJob(_ context.Context, manifest launcher.RenderedManifest) error {
	identity, err := launcher.DecodeRestoreDiscardJobIdentity(manifest)
	if err != nil {
		return err
	}
	c.jobs[identity.Name] = launcher.RestoreDiscardJobObservation{Identity: identity, Active: 1}
	c.created++
	return nil
}
func (c *fakeRestoreDiscardClient) DeleteRestoreDiscardJob(_ context.Context, identity launcher.RestoreDiscardJobIdentity) error {
	delete(c.jobs, identity.Name)
	delete(c.jobPods, identity.Name)
	c.deleted++
	return nil
}

func TestPhase175RestoreDiscardReconcilerWaitsForWorkloadAndPersistsTerminalEvidence(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	spec := lifecycle.VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotID: "snap-abc"}
	if _, err := h.Lifecycle().Volumes.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	abort := lifecycle.RestoreAbortRecord{
		OperationID: "abort-001", SnapshotID: "snap-abc",
		Replicas: []lifecycle.RestoreAbortReplica{{ServerID: "m01", KubernetesNodeName: "node-a", ReplicaID: "r1", State: lifecycle.RestoreDiscardPending}},
	}
	if _, err := h.Lifecycle().Volumes.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort); err != nil {
		t.Fatal(err)
	}
	client := &fakeRestoreDiscardClient{jobs: map[string]launcher.RestoreDiscardJobObservation{}, jobPods: map[string][]launcher.RestoreDiscardPodObservation{}}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cfg := RestoreDiscardReconcileConfig{
		Namespace: "kube-system", Image: "sw-block:test", StateHostPathBase: "/var/lib/sw-block/replicas",
		MaxAttempts: 2, RetryBaseDelay: time.Minute, Now: func() time.Time { return now },
	}
	client.deployments = []launcher.DeploymentIdentity{{Labels: map[string]string{launcher.LabelApp: launcher.AppBlockVolume, launcher.LabelVolume: spec.VolumeID, launcher.LabelReplica: "r1"}}}
	result, err := h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.WaitingForWorkloads != 1 || client.created != 0 {
		t.Fatalf("deployment hold result=%+v created=%d error=%v", result, client.created, err)
	}
	client.deployments = nil
	client.blockPods = []launcher.BlockVolumePodIdentity{{Name: "old-pod", VolumeID: spec.VolumeID, ReplicaID: "r1"}}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.WaitingForWorkloads != 1 || client.created != 0 {
		t.Fatalf("pod hold result=%+v created=%d error=%v", result, client.created, err)
	}
	client.blockPods = nil
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsCreated != 1 || client.created != 1 {
		t.Fatalf("create result=%+v created=%d error=%v", result, client.created, err)
	}
	var identity launcher.RestoreDiscardJobIdentity
	for _, job := range client.jobs {
		identity = job.Identity
	}
	client.jobs[identity.Name] = launcher.RestoreDiscardJobObservation{Identity: identity, Failed: 1}
	client.jobPods[identity.Name] = []launcher.RestoreDiscardPodObservation{{
		Namespace: identity.Namespace, Name: identity.Name + "-failed", NodeName: identity.KubernetesNodeName, Phase: "Failed",
		OperationID: identity.OperationID, SnapshotID: identity.SnapshotID, VolumeID: identity.VolumeID, ReplicaID: identity.ReplicaID, KubernetesNodeName: identity.KubernetesNodeName,
		Terminated: true, ExitCode: 1, Message: "temporary fsync failure",
	}}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsFailed != 1 || result.JobsDeleted != 1 || result.JobsRetryWaiting != 1 {
		t.Fatalf("failed attempt result=%+v error=%v", result, err)
	}
	record, _ := h.Lifecycle().Volumes.GetVolume(spec.VolumeID)
	if record.RestoreAbort.Replicas[0].State != lifecycle.RestoreDiscardRetryWait || record.RestoreAbort.Replicas[0].Attempt != 1 {
		t.Fatalf("retry record=%+v", record)
	}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsRetryWaiting != 1 || result.JobsCreated != 0 {
		t.Fatalf("backoff result=%+v error=%v", result, err)
	}
	now = now.Add(time.Minute)
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsCreated != 1 {
		t.Fatalf("retry create result=%+v error=%v", result, err)
	}
	evidence, _ := json.Marshal(snapshot.RestoreDiscardResult{
		OperationID: identity.OperationID, SnapshotID: identity.SnapshotID, TargetVolumeID: identity.VolumeID, TargetReplicaID: identity.ReplicaID, MarkerRemoved: true, DataRemoved: true,
	})
	client.jobs[identity.Name] = launcher.RestoreDiscardJobObservation{Identity: identity, Succeeded: 1}
	client.jobPods[identity.Name] = []launcher.RestoreDiscardPodObservation{{
		Namespace: identity.Namespace, Name: identity.Name + "-pod", NodeName: identity.KubernetesNodeName, Phase: "Succeeded",
		OperationID: identity.OperationID, SnapshotID: identity.SnapshotID, VolumeID: identity.VolumeID, ReplicaID: identity.ReplicaID, KubernetesNodeName: identity.KubernetesNodeName,
		Terminated: true, ExitCode: 0, Message: string(evidence),
	}}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.EvidenceRecorded != 1 || result.JobsDeleted != 1 {
		t.Fatalf("evidence result=%+v error=%v", result, err)
	}
	record, _ = h.Lifecycle().Volumes.GetVolume(spec.VolumeID)
	if record.RestoreAbort.Replicas[0].State != lifecycle.RestoreDiscardSucceeded || record.RestoreState != lifecycle.VolumeRestoreDiscarded {
		t.Fatalf("record=%+v", record)
	}

	invalidSpec := lifecycle.VolumeSpec{VolumeID: "restored-b", SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotID: "snap-def"}
	if _, err := h.Lifecycle().Volumes.CreateVolume(invalidSpec); err != nil {
		t.Fatal(err)
	}
	invalidAbort := lifecycle.RestoreAbortRecord{
		OperationID: "abort-002", SnapshotID: invalidSpec.SourceSnapshotID,
		Replicas: []lifecycle.RestoreAbortReplica{{ServerID: "m02", KubernetesNodeName: "node-b", ReplicaID: "r1", State: lifecycle.RestoreDiscardPending}},
	}
	if _, err := h.Lifecycle().Volumes.RequestRestoreAbort(invalidSpec.VolumeID, invalidSpec.SourceSnapshotID, invalidAbort); err != nil {
		t.Fatal(err)
	}
	cfg.MaxAttempts = 1
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsCreated != 1 {
		t.Fatalf("invalid-evidence create result=%+v error=%v", result, err)
	}
	for _, job := range client.jobs {
		identity = job.Identity
	}
	client.jobs[identity.Name] = launcher.RestoreDiscardJobObservation{Identity: identity, Succeeded: 1}
	client.jobPods[identity.Name] = []launcher.RestoreDiscardPodObservation{{
		Namespace: identity.Namespace, Name: identity.Name + "-bad", NodeName: identity.KubernetesNodeName, Phase: "Succeeded",
		OperationID: identity.OperationID, SnapshotID: identity.SnapshotID, VolumeID: identity.VolumeID, ReplicaID: identity.ReplicaID, KubernetesNodeName: identity.KubernetesNodeName,
		Terminated: true, ExitCode: 0, Message: `{}`,
	}}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsFailed != 1 || result.TerminalFailures != 1 || result.JobsDeleted != 1 {
		t.Fatalf("invalid-evidence terminal result=%+v error=%v", result, err)
	}
	invalidRecord, _ := h.Lifecycle().Volumes.GetVolume(invalidSpec.VolumeID)
	if invalidRecord.RestoreAbort.Replicas[0].State != lifecycle.RestoreDiscardTerminalFailure || !strings.Contains(invalidRecord.RestoreAbort.Replicas[0].FailureReason, "terminal evidence") {
		t.Fatalf("invalid-evidence record=%+v", invalidRecord)
	}
}

func TestPhase175RestoreDiscardReconcilerPersistsAttemptDeadlineAsTerminalFailure(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	spec := lifecycle.VolumeSpec{VolumeID: "restored-timeout", SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotID: "snap-timeout"}
	if _, err := h.Lifecycle().Volumes.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	abort := lifecycle.RestoreAbortRecord{
		OperationID: "abort-timeout", SnapshotID: spec.SourceSnapshotID,
		Replicas: []lifecycle.RestoreAbortReplica{{ServerID: "m01", KubernetesNodeName: "node-a", ReplicaID: "r1", State: lifecycle.RestoreDiscardPending}},
	}
	if _, err := h.Lifecycle().Volumes.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort); err != nil {
		t.Fatal(err)
	}
	client := &fakeRestoreDiscardClient{jobs: map[string]launcher.RestoreDiscardJobObservation{}, jobPods: map[string][]launcher.RestoreDiscardPodObservation{}}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cfg := RestoreDiscardReconcileConfig{
		Namespace: "kube-system", Image: "sw-block:test", StateHostPathBase: "/var/lib/sw-block/replicas",
		MaxAttempts: 1, RetryBaseDelay: time.Minute, Now: func() time.Time { return now },
	}
	result, err := h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsCreated != 1 {
		t.Fatalf("create result=%+v error=%v", result, err)
	}
	var identity launcher.RestoreDiscardJobIdentity
	for _, job := range client.jobs {
		identity = job.Identity
	}
	client.jobs[identity.Name] = launcher.RestoreDiscardJobObservation{
		Identity: identity, CreatedAt: now.Add(-121 * time.Second), ActiveDeadlineSeconds: 120, Active: 1,
	}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsFailed != 1 || result.TerminalFailures != 1 || result.JobsDeleted != 1 || result.JobsActive != 0 {
		t.Fatalf("deadline result=%+v error=%v", result, err)
	}
	record, _ := h.Lifecycle().Volumes.GetVolume(spec.VolumeID)
	replica := record.RestoreAbort.Replicas[0]
	if replica.State != lifecycle.RestoreDiscardTerminalFailure || !strings.Contains(replica.FailureReason, "active deadline") || replica.Attempt != 1 {
		t.Fatalf("record=%+v", record)
	}
}

func TestPhase175RestoreDiscardReconcilerStopsRunningJobWhenWorkloadReappears(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	spec := lifecycle.VolumeSpec{VolumeID: "restored-refenced", SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotID: "snap-refenced"}
	if _, err := h.Lifecycle().Volumes.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	abort := lifecycle.RestoreAbortRecord{
		OperationID: "abort-refenced", SnapshotID: spec.SourceSnapshotID,
		Replicas: []lifecycle.RestoreAbortReplica{{ServerID: "m01", KubernetesNodeName: "node-a", ReplicaID: "r1", State: lifecycle.RestoreDiscardPending}},
	}
	if _, err := h.Lifecycle().Volumes.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort); err != nil {
		t.Fatal(err)
	}
	client := &fakeRestoreDiscardClient{jobs: map[string]launcher.RestoreDiscardJobObservation{}, jobPods: map[string][]launcher.RestoreDiscardPodObservation{}}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cfg := RestoreDiscardReconcileConfig{
		Namespace: "kube-system", Image: "sw-block:test", StateHostPathBase: "/var/lib/sw-block/replicas",
		MaxAttempts: 1, RetryBaseDelay: time.Minute, Now: func() time.Time { return now },
	}
	result, err := h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.JobsCreated != 1 {
		t.Fatalf("create result=%+v error=%v", result, err)
	}
	for name, job := range client.jobs {
		job.CreatedAt = now.Add(-121 * time.Second)
		job.ActiveDeadlineSeconds = 120
		client.jobs[name] = job
	}
	client.deployments = []launcher.DeploymentIdentity{{Labels: map[string]string{
		launcher.LabelApp: launcher.AppBlockVolume, launcher.LabelVolume: spec.VolumeID, launcher.LabelReplica: "r1",
	}}}
	result, err = h.RunSnapshotRestoreDiscardTick(context.Background(), cfg, client)
	if err != nil || result.WaitingForWorkloads != 1 || result.JobsFailed != 1 || result.TerminalFailures != 1 || result.JobsDeleted != 1 {
		t.Fatalf("refence result=%+v error=%v", result, err)
	}
	record, _ := h.Lifecycle().Volumes.GetVolume(spec.VolumeID)
	replica := record.RestoreAbort.Replicas[0]
	if replica.State != lifecycle.RestoreDiscardTerminalFailure || !strings.Contains(replica.FailureReason, "execution fence lost") || len(client.jobs) != 0 {
		t.Fatalf("record=%+v jobs=%+v", record, client.jobs)
	}
}
