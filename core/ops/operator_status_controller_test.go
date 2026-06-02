package ops

import (
	"context"
	"testing"
	"time"
)

func TestOperatorStatusReconcilerWritesStatusOnlyProjection(t *testing.T) {
	capturedAt := time.Date(2026, 6, 2, 22, 30, 0, 0, time.UTC)
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    capturedAt,
		Status:        ObservationStatusOK,
		Nodes: []NodeEvidence{{
			NodeName: "m02",
			Ready:    true,
		}},
		ManagedVolumes: []ManagedVolumeProjection{
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID: "pvc-ready",
				PVCName:  "demo-pvc",
				PVC:      &PVCFact{Phase: "Bound"},
				Authority: &AuthorityFact{
					PrimaryReplica: "r1",
					PublishTarget:  "192.168.1.184:3260",
				},
				Replicas: []ReplicaFact{{
					ReplicaID:      "r1",
					KubernetesNode: "m02",
					Role:           "primary",
					Observed:       true,
				}},
				CSIStages: []CSIStageFact{{NodeName: "m02", Target: "192.168.1.184:3260"}},
				Workload:  &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
			}),
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID:      "pvc-blocked",
				PVCName:       "blocked-pvc",
				ProductStatus: ObservationStatusBlocked,
				ProductReason: ReasonCSINodeImagePullFailed,
				EvidenceRefs:  []string{"diagnostics/events.txt"},
			}),
		},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}

	result, err := (OperatorStatusReconciler{
		Namespace:   "kube-system",
		ClusterName: "sw-block",
		Source:      source,
		Writer:      writer,
		EventSink:   events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.ClusterRef.Kind != SwBlockClusterKind || result.ClusterRef.Name != "sw-block" {
		t.Fatalf("cluster ref=%+v", result.ClusterRef)
	}
	if len(result.VolumeRefs) != 2 || len(writer.volumes) != 2 {
		t.Fatalf("volume refs=%+v writes=%+v", result.VolumeRefs, writer.volumes)
	}
	if writer.cluster.NodeCount != 1 || writer.cluster.VolumeCount != 2 || writer.cluster.ReadyVolumeCount != 1 || writer.cluster.BlockedVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if writer.cluster.MutationAllowed {
		t.Fatalf("cluster status must not allow mutation: %+v", writer.cluster)
	}
	if got := writer.volumes[0].status.Status; got != ManagedVolumeStatusReady {
		t.Fatalf("ready volume status=%s", got)
	}
	if got := writer.volumes[1].status.ReasonCode; got != ReasonCSINodeImagePullFailed {
		t.Fatalf("blocked volume reason=%s", got)
	}
	for _, write := range writer.volumes {
		for _, action := range write.status.AllowedActions {
			if action.MutationAllowed {
				t.Fatalf("mutating action leaked into status: %+v", action)
			}
		}
	}
	if events.countByReason(ReasonFirstVolumeVerified) == 0 {
		t.Fatalf("missing ready event: %+v", events.events)
	}
	if events.countByReason(ReasonCSINodeImagePullFailed) == 0 {
		t.Fatalf("missing blocked event: %+v", events.events)
	}
	if result.EventCount != len(events.events) {
		t.Fatalf("event count=%d events=%d", result.EventCount, len(events.events))
	}
}

func TestOperatorStatusReconcilerStaleEvidenceProjectsUnknownAndWarningEvent(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 2, 23, 0, 0, 0, time.UTC),
		Status:        ObservationStatusUnavailable,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:            "pvc-stale",
			PVCName:             "stale_pvc",
			EvidenceStale:       true,
			EvidenceStaleReason: ReasonEvidenceStale,
			EvidenceRefs:        []string{"product/unreachable.txt"},
		})},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &fakeOperatorEventSink{}

	_, err := (OperatorStatusReconciler{
		Source:    source,
		Writer:    writer,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if writer.cluster.StaleVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	volume := writer.volumes[0]
	if volume.ref.Name != "stale-pvc" {
		t.Fatalf("volume name=%q", volume.ref.Name)
	}
	if volume.status.Status != ManagedVolumeStatusUnknown || volume.status.ReasonCode != ReasonEvidenceStale {
		t.Fatalf("volume status=%+v", volume.status)
	}
	foundUnknownReady := false
	for _, condition := range volume.status.Conditions {
		if condition.Type == ConditionReady && condition.Status == "Unknown" && condition.Reason == ReasonEvidenceStale {
			foundUnknownReady = true
		}
	}
	if !foundUnknownReady {
		t.Fatalf("missing Ready=Unknown condition: %+v", volume.status.Conditions)
	}
	if events.countByReason(ReasonEvidenceStale) == 0 {
		t.Fatalf("missing EvidenceStale event: %+v", events.events)
	}
	for _, event := range events.events {
		if event.Reason == ReasonEvidenceStale && event.Type != "Warning" {
			t.Fatalf("EvidenceStale event type=%s want Warning", event.Type)
		}
	}
}

func TestSwBlockVolumeObjectNameIsDNSLabelLike(t *testing.T) {
	cases := map[string]string{
		"Demo_PVC":      "demo-pvc",
		"pvc:1234.5678": "pvc-1234-5678",
		"---":           "unknown-volume",
		"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnop": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk",
	}
	for in, want := range cases {
		got := SwBlockVolumeObjectName(ManagedVolumeOperatorStatus{PVCName: in})
		if got != want {
			t.Fatalf("name %q=%q want %q", in, got, want)
		}
	}
}

type fakeOperatorStatusSource struct {
	cluster ClusterEvidence
}

func (f fakeOperatorStatusSource) ClusterEvidence(context.Context) (ClusterEvidence, error) {
	return f.cluster, nil
}

type fakeOperatorStatusWriter struct {
	cluster OperatorStatusWriterClusterRecord
	volumes []OperatorStatusWriterVolumeRecord
}

type OperatorStatusWriterClusterRecord = SwBlockClusterCRDStatus

type OperatorStatusWriterVolumeRecord struct {
	ref    OperatorObjectRef
	status SwBlockVolumeCRDStatus
}

func (f *fakeOperatorStatusWriter) WriteClusterStatus(_ context.Context, _ OperatorObjectRef, status SwBlockClusterCRDStatus) error {
	f.cluster = status
	return nil
}

func (f *fakeOperatorStatusWriter) WriteVolumeStatus(_ context.Context, ref OperatorObjectRef, status SwBlockVolumeCRDStatus) error {
	f.volumes = append(f.volumes, OperatorStatusWriterVolumeRecord{ref: ref, status: status})
	return nil
}

type fakeOperatorEventSink struct {
	events []OperatorKubernetesEvent
}

func (f *fakeOperatorEventSink) EmitEvent(_ context.Context, event OperatorKubernetesEvent) error {
	f.events = append(f.events, event)
	return nil
}

func (f *fakeOperatorEventSink) countByReason(reason string) int {
	count := 0
	for _, event := range f.events {
		if event.Reason == reason {
			count++
		}
	}
	return count
}
