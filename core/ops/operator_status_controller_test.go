package ops

import (
	"context"
	"encoding/json"
	"strings"
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
	rawStatus, err := json.Marshal(writer.volumes[0].status)
	if err != nil {
		t.Fatalf("marshal volume status: %v", err)
	}
	if !strings.Contains(string(rawStatus), `"mutationAllowed":false`) {
		t.Fatalf("CRD volume status must use camelCase mutationAllowed: %s", string(rawStatus))
	}
	if strings.Contains(string(rawStatus), "mutation_allowed") {
		t.Fatalf("CRD volume status must not use operator-snapshot snake_case fields: %s", string(rawStatus))
	}
	if events.countByReason(ReasonFirstVolumeVerified) == 0 {
		t.Fatalf("missing ready event: %+v", events.events)
	}
	if events.countByReason(ReasonCSINodeImagePullFailed) == 0 {
		t.Fatalf("missing blocked event: %+v", events.events)
	}
	for _, event := range events.events {
		if event.Reason == ReasonCSINodeImagePullFailed {
			if event.Type != "Warning" || event.InvolvedObject.Kind != SwBlockVolumeKind || event.InvolvedObject.Name != "blocked-pvc" {
				t.Fatalf("blocked event shape=%+v", event)
			}
		}
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

func TestOperatorStatusReconcilerStatusEndpointUnreachableIsUnknownNotBlocked(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 4, 2, 0, 0, 0, time.UTC),
		Status:        ObservationStatusUnavailable,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:      "pvc-unreachable",
			PVCName:       "status-unreachable-pvc",
			ProductStatus: ObservationStatusUnavailable,
			ProductReason: ReasonStatusEndpointUnreachable,
			EvidenceRefs:  []string{"diagnostics/status-endpoint-unreachable.txt"},
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
	if writer.cluster.StaleVolumeCount != 1 || writer.cluster.BlockedVolumeCount != 0 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	volume := writer.volumes[0]
	if volume.status.Status != ManagedVolumeStatusUnknown || volume.status.ReasonCode != ReasonStatusEndpointUnreachable {
		t.Fatalf("volume status=%+v", volume.status)
	}
	assertCondition(t, volume.status.Conditions, ConditionReady, "Unknown", ReasonStatusEndpointUnreachable)
	assertCondition(t, volume.status.Conditions, ConditionEvidenceStale, "True", ReasonStatusEndpointUnreachable)
	if condition := findObservationCondition(volume.status.Conditions, ConditionBlocked); condition != nil && condition.Status == "True" {
		t.Fatalf("pure unreachable status must not become Blocked=True: %+v", volume.status.Conditions)
	}
	if events.countByReason(ReasonStatusEndpointUnreachable) == 0 {
		t.Fatalf("missing status_endpoint_unreachable event: %+v", events.events)
	}
	for _, event := range events.events {
		if event.Reason == ReasonStatusEndpointUnreachable && event.Type != "Warning" {
			t.Fatalf("status endpoint event type=%s want Warning", event.Type)
		}
	}
}

func TestOperatorStatusReconcilerWALIntegrityFaultIsNeverReady(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 4, 2, 15, 0, 0, time.UTC),
		Status:        ObservationStatusBlocked,
		ManagedVolumes: []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID:      "pvc-corrupt",
			PVCName:       "wal-corrupt-pvc",
			ProductStatus: ObservationStatusBlocked,
			ProductReason: ReasonWALIntegrityFault,
			EvidenceRefs:  []string{"status/report/cluster-evidence.json", "blockvolume.log"},
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
	if writer.cluster.ReadyVolumeCount != 0 || writer.cluster.BlockedVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", writer.cluster)
	}
	if len(writer.volumes) != 1 {
		t.Fatalf("volume writes=%+v", writer.volumes)
	}
	volume := writer.volumes[0]
	if volume.status.Status == ManagedVolumeStatusReady {
		t.Fatalf("wal integrity fault must never be Ready: %+v", volume.status)
	}
	if volume.status.ReasonCode != ReasonWALIntegrityFault {
		t.Fatalf("volume status=%+v", volume.status)
	}
	assertCondition(t, volume.status.Conditions, ConditionReady, "False", ReasonWALIntegrityFault)
	assertCondition(t, volume.status.Conditions, ConditionBlocked, "True", ReasonWALIntegrityFault)
	if events.countByReason(ReasonWALIntegrityFault) == 0 {
		t.Fatalf("missing wal_integrity_fault event: %+v", events.events)
	}
}

func TestOperatorStatusReconcilerEventFailureDoesNotBlockLaterStatusWrites(t *testing.T) {
	source := fakeOperatorStatusSource{cluster: ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    time.Date(2026, 6, 4, 1, 30, 0, 0, time.UTC),
		ManagedVolumes: []ManagedVolumeProjection{
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID:      "pvc-blocked",
				PVCName:       "blocked-pvc",
				ProductStatus: ObservationStatusBlocked,
				ProductReason: ReasonCSINodeImagePullFailed,
				EvidenceRefs:  []string{"blocked/events.txt"},
			}),
			ProjectManagedVolume(ManagedVolumeFacts{
				VolumeID: "pvc-ready",
				PVCName:  "ready-pvc",
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
		},
	}}
	writer := &fakeOperatorStatusWriter{}
	events := &failingOperatorEventSink{}

	result, err := (OperatorStatusReconciler{
		Namespace: "kube-system",
		Source:    source,
		Writer:    writer,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("event failure must not abort reconcile: %v", err)
	}
	if len(writer.volumes) != 2 {
		t.Fatalf("status writes=%+v", writer.volumes)
	}
	if writer.volumes[0].status.Status != ManagedVolumeStatusBlocked || writer.volumes[1].status.Status != ManagedVolumeStatusReady {
		t.Fatalf("volume statuses=%+v", writer.volumes)
	}
	if events.count == 0 {
		t.Fatalf("expected event attempts")
	}
	if result.EventCount != 0 {
		t.Fatalf("successful event count=%d want 0", result.EventCount)
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

type failingOperatorEventSink struct {
	count int
}

func (f *failingOperatorEventSink) EmitEvent(context.Context, OperatorKubernetesEvent) error {
	f.count++
	return errOperatorEventSinkFailed{}
}

type errOperatorEventSinkFailed struct{}

func (errOperatorEventSinkFailed) Error() string {
	return "event sink failed"
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

func assertCondition(t *testing.T, conditions []ObservationCondition, typ, status, reason string) {
	t.Helper()
	condition := findObservationCondition(conditions, typ)
	if condition == nil {
		t.Fatalf("missing %s condition: %+v", typ, conditions)
	}
	if condition.Status != status || condition.Reason != reason {
		t.Fatalf("%s condition=%+v want status=%s reason=%s", typ, *condition, status, reason)
	}
}
