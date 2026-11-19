package storage

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCheckpointDiagnostics_RefusalThenSuccess(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	s.v3.testMapPrepared = func() {
		if _, err := s.Write(2, blk(2)); err != nil {
			t.Fatal(err)
		}
	}
	before := s.v3.pub.current()
	err := s.flusher.flushOnce()
	if !errors.Is(err, ErrMapCoverageStale) {
		t.Fatalf("refusal: %v", err)
	}
	if !strings.Contains(err.Error(), "proof_changed") {
		t.Errorf("missing refusal reason: %v", err)
	}
	if s.v3.pub.current() != before || s.dm.len() != 2 || s.stickyIOFault() != nil {
		t.Fatal("refusal changed publication/retry/fault state")
	}
	raw, _ := json.Marshal(s.FlusherInstrumentation())
	var counts map[string]uint64
	if err := json.Unmarshal(raw, &counts); err != nil {
		t.Fatal(err)
	}
	if counts["CheckpointRefusals"] != 1 || counts["CheckpointSuccesses"] != 0 {
		t.Fatalf("refusal counters: %s", raw)
	}
	s.v3.testMapPrepared = nil
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(s.FlusherInstrumentation())
	if err := json.Unmarshal(raw, &counts); err != nil {
		t.Fatal(err)
	}
	if counts["CheckpointRefusals"] != 1 || counts["CheckpointSuccesses"] != 1 || s.CheckpointLSN() != 2 || s.dm.len() != 0 {
		t.Fatalf("retry counters/state: %s cp=%d", raw, s.CheckpointLSN())
	}
}
