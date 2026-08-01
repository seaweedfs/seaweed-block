package lifecycle

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPhase175RestoreAbortStateSurvivesRestartAndRequiresAllReplicaEvidence(t *testing.T) {
	root := t.TempDir()
	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 3, SourceSnapshotID: "snap-abc"}
	if _, err := store.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	abort := restoreAbortForTest()
	rec, err := store.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort)
	if err != nil || rec.RestoreState != VolumeRestoreAbortRequested || rec.RestoreAbort.OperationID != abort.OperationID {
		t.Fatalf("request abort record=%+v error=%v", rec, err)
	}
	if _, err := store.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort); err != nil {
		t.Fatalf("idempotent abort request: %v", err)
	}
	changed := abort
	changed.OperationID = "abort-002"
	if _, err := store.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, changed); !errors.Is(err, ErrRestoreConflict) {
		t.Fatalf("changed abort request error=%v", err)
	}
	if err := store.DeleteVolume(spec.VolumeID); !errors.Is(err, ErrRestorePending) {
		t.Fatalf("delete aborting restore error=%v", err)
	}
	if _, err := store.AttachVolume(spec.VolumeID, "node-a"); !errors.Is(err, ErrRestorePending) {
		t.Fatalf("attach aborting restore error=%v", err)
	}
	reopened, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if rec, ok := reopened.GetVolume(spec.VolumeID); !ok || rec.RestoreState != VolumeRestoreAbortRequested || len(rec.RestoreAbort.Replicas) != 3 {
		t.Fatalf("reopened record=%+v ok=%v", rec, ok)
	}
	for i, replica := range abort.Replicas {
		_, running, err := reopened.BeginRestoreDiscardAttempt(spec.VolumeID, abort.OperationID, replica.ServerID, replica.ReplicaID, time.Now())
		if err != nil {
			t.Fatalf("begin replica %d: %v", i, err)
		}
		evidence := replica
		evidence.State = RestoreDiscardSucceeded
		evidence.Attempt = running.Attempt
		evidence.MarkerRemoved = true
		evidence.DataRemoved = true
		evidence.EvidenceRef = "job/abort-001-" + replica.ReplicaID
		if _, err := reopened.RecordRestoreDiscard(spec.VolumeID, abort.OperationID, evidence); err != nil {
			t.Fatalf("record replica %d: %v", i, err)
		}
		if i < len(abort.Replicas)-1 {
			if _, err := reopened.MarkRestoreDiscarded(spec.VolumeID, abort.OperationID); !errors.Is(err, ErrDiscardIncomplete) {
				t.Fatalf("mark discarded after replica %d error=%v", i, err)
			}
		}
	}
	rec, err = reopened.MarkRestoreDiscarded(spec.VolumeID, abort.OperationID)
	if err != nil || rec.RestoreState != VolumeRestoreDiscarded {
		t.Fatalf("mark discarded record=%+v error=%v", rec, err)
	}
	if err := reopened.DeleteVolume(spec.VolumeID); err != nil {
		t.Fatalf("delete discarded restore: %v", err)
	}
}

func TestPhase175RestoreCompleteAndAbortAreMutuallyExclusive(t *testing.T) {
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 3, SourceSnapshotID: "snap-abc"}
	if _, err := store.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var completeErr, abortErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, completeErr = store.MarkRestoreComplete(spec.VolumeID, spec.SourceSnapshotID)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, abortErr = store.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, restoreAbortForTest())
	}()
	close(start)
	wg.Wait()
	if (completeErr == nil) == (abortErr == nil) {
		t.Fatalf("complete error=%v abort error=%v", completeErr, abortErr)
	}
	rec, _ := store.GetVolume(spec.VolumeID)
	if completeErr == nil && rec.RestoreState != VolumeRestoreComplete {
		t.Fatalf("complete won with state=%q", rec.RestoreState)
	}
	if abortErr == nil && rec.RestoreState != VolumeRestoreAbortRequested {
		t.Fatalf("abort won with state=%q", rec.RestoreState)
	}
}

func TestPhase175RestoreDiscardAttemptsPersistBackoffAndTerminalFailure(t *testing.T) {
	root := t.TempDir()
	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotID: "snap-abc"}
	if _, err := store.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	abort := RestoreAbortRecord{OperationID: "abort-001", SnapshotID: spec.SourceSnapshotID, Replicas: []RestoreAbortReplica{{
		ServerID: "m01", KubernetesNodeName: "node-a", ReplicaID: "r1", State: RestoreDiscardPending,
	}}}
	if _, err := store.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	_, running, err := store.BeginRestoreDiscardAttempt(spec.VolumeID, abort.OperationID, "m01", "r1", now)
	if err != nil || running.Attempt != 1 || running.State != RestoreDiscardRunning {
		t.Fatalf("first attempt=%+v error=%v", running, err)
	}
	if _, err := store.RecordRestoreDiscardFailure(spec.VolumeID, abort.OperationID, "m01", "r1", 1, 2, "temporary fsync failure", "job/a/pod/one", now, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	record, _ := reopened.GetVolume(spec.VolumeID)
	retry := record.RestoreAbort.Replicas[0]
	if retry.State != RestoreDiscardRetryWait || retry.Attempt != 1 || !retry.RetryNotBefore.Equal(now.Add(5*time.Second)) || retry.FailureReason == "" {
		t.Fatalf("retry state=%+v", retry)
	}
	if _, _, err := reopened.BeginRestoreDiscardAttempt(spec.VolumeID, abort.OperationID, "m01", "r1", now.Add(4*time.Second)); !errors.Is(err, ErrRestoreConflict) {
		t.Fatalf("early retry error=%v", err)
	}
	_, running, err = reopened.BeginRestoreDiscardAttempt(spec.VolumeID, abort.OperationID, "m01", "r1", now.Add(5*time.Second))
	if err != nil || running.Attempt != 2 {
		t.Fatalf("second attempt=%+v error=%v", running, err)
	}
	terminal, err := reopened.RecordRestoreDiscardFailure(spec.VolumeID, abort.OperationID, "m01", "r1", 2, 2, "permanent permission failure", "job/a/pod/two", now.Add(5*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	failed := terminal.RestoreAbort.Replicas[0]
	if failed.State != RestoreDiscardTerminalFailure || failed.Attempt != 2 || failed.FailureReason == "" || !failed.RetryNotBefore.IsZero() {
		t.Fatalf("terminal state=%+v", failed)
	}
	if err := reopened.DeleteVolume(spec.VolumeID); !errors.Is(err, ErrRestorePending) {
		t.Fatalf("terminal failure must hold delete: %v", err)
	}
}

func TestPhase175RestoreAbortCopiesMutableEvidence(t *testing.T) {
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "restored-a", SizeBytes: 1 << 20, ReplicationFactor: 3, SourceSnapshotID: "snap-abc"}
	if _, err := store.CreateVolume(spec); err != nil {
		t.Fatal(err)
	}
	abort := restoreAbortForTest()
	rec, err := store.RequestRestoreAbort(spec.VolumeID, spec.SourceSnapshotID, abort)
	if err != nil {
		t.Fatal(err)
	}
	rec.RestoreAbort.Replicas[0].ReplicaID = "mutated"
	abort.Replicas[0].ReplicaID = "also-mutated"
	stored, _ := store.GetVolume(spec.VolumeID)
	if stored.RestoreAbort.Replicas[0].ReplicaID != "r1" {
		t.Fatalf("stored abort mutated through returned value: %+v", stored.RestoreAbort.Replicas)
	}
}

func restoreAbortForTest() RestoreAbortRecord {
	return RestoreAbortRecord{
		OperationID: "abort-001",
		SnapshotID:  "snap-abc",
		Replicas: []RestoreAbortReplica{
			{ServerID: "m03", KubernetesNodeName: "node-c", ReplicaID: "r3", State: RestoreDiscardPending},
			{ServerID: "m01", KubernetesNodeName: "node-a", ReplicaID: "r1", State: RestoreDiscardPending},
			{ServerID: "m02", KubernetesNodeName: "node-b", ReplicaID: "r2", State: RestoreDiscardPending},
		},
	}
}
