package lifecycle

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	RestoreDiscardPending         = "pending"
	RestoreDiscardRunning         = "running"
	RestoreDiscardRetryWait       = "retry_wait"
	RestoreDiscardSucceeded       = "discarded"
	RestoreDiscardTerminalFailure = "terminal_failure"
)

type RestoreAbortRecord struct {
	OperationID string                `json:"operation_id"`
	SnapshotID  string                `json:"snapshot_id"`
	Replicas    []RestoreAbortReplica `json:"replicas"`
}

type RestoreAbortReplica struct {
	ServerID           string    `json:"server_id"`
	KubernetesNodeName string    `json:"kubernetes_node_name"`
	ReplicaID          string    `json:"replica_id"`
	State              string    `json:"state"`
	MarkerRemoved      bool      `json:"marker_removed,omitempty"`
	DataRemoved        bool      `json:"data_removed,omitempty"`
	EvidenceRef        string    `json:"evidence_ref,omitempty"`
	Attempt            uint32    `json:"attempt,omitempty"`
	RetryNotBefore     time.Time `json:"retry_not_before,omitempty"`
	FailureReason      string    `json:"failure_reason,omitempty"`
}

func (s *FileStore) BeginRestoreDiscardAttempt(volumeID, operationID, serverID, replicaID string, now time.Time) (VolumeRecord, RestoreAbortReplica, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, RestoreAbortReplica{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, RestoreAbortReplica{}, ErrVolumeNotFound
	}
	if rec.RestoreState != VolumeRestoreAbortRequested || rec.RestoreAbort == nil || rec.RestoreAbort.OperationID != operationID {
		return VolumeRecord{}, RestoreAbortReplica{}, ErrRestoreConflict
	}
	abort := *rec.RestoreAbort
	abort.Replicas = append([]RestoreAbortReplica(nil), abort.Replicas...)
	for i := range abort.Replicas {
		replica := &abort.Replicas[i]
		if replica.ServerID != serverID || replica.ReplicaID != replicaID {
			continue
		}
		if replica.State == RestoreDiscardRunning {
			return copyVolumeRecord(rec), *replica, nil
		}
		if replica.State != RestoreDiscardPending && replica.State != RestoreDiscardRetryWait {
			return VolumeRecord{}, RestoreAbortReplica{}, ErrRestoreConflict
		}
		if replica.State == RestoreDiscardRetryWait && (now.IsZero() || now.Before(replica.RetryNotBefore)) {
			return VolumeRecord{}, RestoreAbortReplica{}, ErrRestoreConflict
		}
		replica.State = RestoreDiscardRunning
		replica.Attempt++
		replica.RetryNotBefore = time.Time{}
		replica.FailureReason = ""
		replica.EvidenceRef = ""
		if err := validateRestoreAbortRecord(rec.Spec, VolumeRestoreAbortRequested, &abort); err != nil {
			return VolumeRecord{}, RestoreAbortReplica{}, err
		}
		rec.RestoreAbort = &abort
		if err := s.putLocked(rec); err != nil {
			return VolumeRecord{}, RestoreAbortReplica{}, err
		}
		s.records[volumeID] = rec
		return copyVolumeRecord(rec), *replica, nil
	}
	return VolumeRecord{}, RestoreAbortReplica{}, ErrRestoreConflict
}

func (s *FileStore) RecordRestoreDiscardFailure(volumeID, operationID, serverID, replicaID string, attempt, maxAttempts uint32, reason, evidenceRef string, now time.Time, retryDelay time.Duration) (VolumeRecord, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, err
	}
	reason = strings.TrimSpace(reason)
	if attempt == 0 || maxAttempts == 0 || attempt > maxAttempts || reason == "" || len(reason) > 512 || evidenceRef == "" || now.IsZero() || retryDelay <= 0 {
		return VolumeRecord{}, ErrRestoreConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.RestoreState != VolumeRestoreAbortRequested || rec.RestoreAbort == nil || rec.RestoreAbort.OperationID != operationID {
		return VolumeRecord{}, ErrRestoreConflict
	}
	abort := *rec.RestoreAbort
	abort.Replicas = append([]RestoreAbortReplica(nil), abort.Replicas...)
	for i := range abort.Replicas {
		replica := &abort.Replicas[i]
		if replica.ServerID != serverID || replica.ReplicaID != replicaID {
			continue
		}
		if replica.State != RestoreDiscardRunning || replica.Attempt != attempt {
			return VolumeRecord{}, ErrRestoreConflict
		}
		replica.FailureReason = reason
		replica.EvidenceRef = evidenceRef
		if attempt >= maxAttempts {
			replica.State = RestoreDiscardTerminalFailure
			replica.RetryNotBefore = time.Time{}
		} else {
			replica.State = RestoreDiscardRetryWait
			replica.RetryNotBefore = now.Add(retryDelay)
		}
		if err := validateRestoreAbortRecord(rec.Spec, VolumeRestoreAbortRequested, &abort); err != nil {
			return VolumeRecord{}, err
		}
		rec.RestoreAbort = &abort
		if err := s.putLocked(rec); err != nil {
			return VolumeRecord{}, err
		}
		s.records[volumeID] = rec
		return copyVolumeRecord(rec), nil
	}
	return VolumeRecord{}, ErrRestoreConflict
}

func (s *FileStore) RequestRestoreAbort(volumeID, snapshotID string, abort RestoreAbortRecord) (VolumeRecord, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.Spec.SourceSnapshotID != snapshotID {
		return VolumeRecord{}, ErrRestoreConflict
	}
	switch rec.RestoreState {
	case VolumeRestoreComplete:
		return VolumeRecord{}, ErrRestoreConflict
	case VolumeRestoreAbortRequested, VolumeRestoreDiscarded:
		if rec.RestoreAbort == nil || !sameRestoreAbortIdentity(*rec.RestoreAbort, abort) {
			return VolumeRecord{}, ErrRestoreConflict
		}
		return copyVolumeRecord(rec), nil
	case VolumeRestorePending:
	default:
		return VolumeRecord{}, ErrRestoreConflict
	}
	abort = normalizeRestoreAbortRecord(abort)
	if err := validateRestoreAbortRecord(rec.Spec, VolumeRestoreAbortRequested, &abort); err != nil {
		return VolumeRecord{}, err
	}
	rec.RestoreState = VolumeRestoreAbortRequested
	rec.RestoreAbort = &abort
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[volumeID] = rec
	return copyVolumeRecord(rec), nil
}

func (s *FileStore) RecordRestoreDiscard(volumeID, operationID string, evidence RestoreAbortReplica) (VolumeRecord, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.RestoreState != VolumeRestoreAbortRequested || rec.RestoreAbort == nil || rec.RestoreAbort.OperationID != operationID {
		return VolumeRecord{}, ErrRestoreConflict
	}
	abort := *rec.RestoreAbort
	abort.Replicas = append([]RestoreAbortReplica(nil), abort.Replicas...)
	found := false
	for i := range abort.Replicas {
		current := abort.Replicas[i]
		if current.ServerID != evidence.ServerID || current.ReplicaID != evidence.ReplicaID {
			continue
		}
		if current.KubernetesNodeName != evidence.KubernetesNodeName {
			return VolumeRecord{}, ErrRestoreConflict
		}
		found = true
		if current.State == RestoreDiscardSucceeded {
			if current != evidence {
				return VolumeRecord{}, ErrRestoreConflict
			}
			return copyVolumeRecord(rec), nil
		}
		if current.State != RestoreDiscardRunning || current.Attempt == 0 || evidence.Attempt != current.Attempt {
			return VolumeRecord{}, ErrRestoreConflict
		}
		evidence.RetryNotBefore = time.Time{}
		evidence.FailureReason = ""
		abort.Replicas[i] = evidence
		break
	}
	if !found {
		return VolumeRecord{}, ErrRestoreConflict
	}
	if err := validateRestoreAbortRecord(rec.Spec, VolumeRestoreAbortRequested, &abort); err != nil {
		return VolumeRecord{}, err
	}
	rec.RestoreAbort = &abort
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[volumeID] = rec
	return copyVolumeRecord(rec), nil
}

func (s *FileStore) MarkRestoreDiscarded(volumeID, operationID string) (VolumeRecord, error) {
	if err := validateVolumeID(volumeID); err != nil {
		return VolumeRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[volumeID]
	if !ok {
		return VolumeRecord{}, ErrVolumeNotFound
	}
	if rec.RestoreAbort == nil || rec.RestoreAbort.OperationID != operationID {
		return VolumeRecord{}, ErrRestoreConflict
	}
	if rec.RestoreState == VolumeRestoreDiscarded {
		return copyVolumeRecord(rec), nil
	}
	if rec.RestoreState != VolumeRestoreAbortRequested {
		return VolumeRecord{}, ErrRestoreConflict
	}
	for _, replica := range rec.RestoreAbort.Replicas {
		if replica.State != RestoreDiscardSucceeded {
			return VolumeRecord{}, ErrDiscardIncomplete
		}
	}
	rec.RestoreState = VolumeRestoreDiscarded
	if err := s.putLocked(rec); err != nil {
		return VolumeRecord{}, err
	}
	s.records[volumeID] = rec
	return copyVolumeRecord(rec), nil
}

func normalizeRestoreAbortRecord(abort RestoreAbortRecord) RestoreAbortRecord {
	abort.Replicas = append([]RestoreAbortReplica(nil), abort.Replicas...)
	sort.Slice(abort.Replicas, func(i, j int) bool { return abort.Replicas[i].ReplicaID < abort.Replicas[j].ReplicaID })
	return abort
}

func validateRestoreAbortRecord(spec VolumeSpec, restoreState string, abort *RestoreAbortRecord) error {
	if abort == nil || !IsSafeStorageIdentityComponent(abort.OperationID) || abort.SnapshotID != spec.SourceSnapshotID || len(abort.Replicas) != spec.ReplicationFactor {
		return fmt.Errorf("%w: invalid restore abort identity", ErrInvalidVolumeSpec)
	}
	seenServers := make(map[string]bool, len(abort.Replicas))
	seenReplicas := make(map[string]bool, len(abort.Replicas))
	allDiscarded := true
	for _, replica := range abort.Replicas {
		if validateServerID(replica.ServerID) != nil || !IsSafeStorageIdentityComponent(replica.KubernetesNodeName) || !IsSafeStorageIdentityComponent(replica.ReplicaID) || seenServers[replica.ServerID] || seenReplicas[replica.ReplicaID] {
			return fmt.Errorf("%w: invalid restore abort replica identity", ErrInvalidVolumeSpec)
		}
		seenServers[replica.ServerID] = true
		seenReplicas[replica.ReplicaID] = true
		switch replica.State {
		case RestoreDiscardPending:
			allDiscarded = false
			if replica.Attempt != 0 || replica.MarkerRemoved || replica.DataRemoved || replica.EvidenceRef != "" || !replica.RetryNotBefore.IsZero() || replica.FailureReason != "" {
				return fmt.Errorf("%w: pending restore discard carries terminal evidence", ErrInvalidVolumeSpec)
			}
		case RestoreDiscardRunning:
			allDiscarded = false
			if replica.Attempt == 0 || replica.MarkerRemoved || replica.DataRemoved || replica.EvidenceRef != "" || !replica.RetryNotBefore.IsZero() || replica.FailureReason != "" {
				return fmt.Errorf("%w: running restore discard has invalid attempt evidence", ErrInvalidVolumeSpec)
			}
		case RestoreDiscardRetryWait:
			allDiscarded = false
			if replica.Attempt == 0 || replica.MarkerRemoved || replica.DataRemoved || replica.EvidenceRef == "" || replica.RetryNotBefore.IsZero() || replica.FailureReason == "" {
				return fmt.Errorf("%w: restore discard retry has incomplete failure evidence", ErrInvalidVolumeSpec)
			}
		case RestoreDiscardSucceeded:
			if replica.Attempt == 0 || !replica.MarkerRemoved || !replica.DataRemoved || replica.EvidenceRef == "" || !replica.RetryNotBefore.IsZero() || replica.FailureReason != "" {
				return fmt.Errorf("%w: incomplete restore discard evidence", ErrInvalidVolumeSpec)
			}
		case RestoreDiscardTerminalFailure:
			allDiscarded = false
			if replica.Attempt == 0 || replica.MarkerRemoved || replica.DataRemoved || replica.EvidenceRef == "" || !replica.RetryNotBefore.IsZero() || replica.FailureReason == "" {
				return fmt.Errorf("%w: terminal restore discard failure lacks evidence", ErrInvalidVolumeSpec)
			}
		default:
			return fmt.Errorf("%w: invalid restore discard state %q", ErrInvalidVolumeSpec, replica.State)
		}
	}
	if restoreState == VolumeRestoreDiscarded && !allDiscarded {
		return fmt.Errorf("%w: discarded restore has pending replicas", ErrInvalidVolumeSpec)
	}
	return nil
}

func sameRestoreAbortIdentity(a, b RestoreAbortRecord) bool {
	a = normalizeRestoreAbortRecord(a)
	b = normalizeRestoreAbortRecord(b)
	if a.OperationID != b.OperationID || a.SnapshotID != b.SnapshotID || len(a.Replicas) != len(b.Replicas) {
		return false
	}
	for i := range a.Replicas {
		if a.Replicas[i].ServerID != b.Replicas[i].ServerID || a.Replicas[i].KubernetesNodeName != b.Replicas[i].KubernetesNodeName || a.Replicas[i].ReplicaID != b.Replicas[i].ReplicaID {
			return false
		}
	}
	return true
}
