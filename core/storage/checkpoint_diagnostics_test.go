package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"
)

func TestCheckpointDiagnostics_RefusalThenSuccess(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
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

func TestCheckpointDiagnostics_PrepareRefusalAndIOFailure(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
			writeSynced(t, s, 1, 1)
			realSync := s.v3.ownedMap.syncFile
			injected := errors.New("checkpoint map fsync fault")
			once := false
			s.v3.ownedMap.syncFile = func() error {
				if fault {
					return injected
				}
				if !once {
					once = true
					if _, err := s.Write(2, blk(2)); err != nil {
						t.Fatal(err)
					}
				}
				return realSync()
			}
			err := s.flusher.flushOnce()
			got := s.CheckpointPersistence()
			if fault {
				if !errors.Is(err, injected) || got.Failures != 1 || got.Refusals != 0 || s.stickyIOFault() == nil {
					t.Fatalf("fault %v counts=%+v", err, got)
				}
			} else {
				if !errors.Is(err, ErrMapCoverageStale) || !strings.Contains(err.Error(), "proof_changed_during_prepare") || got.Refusals != 1 || got.Failures != 0 || s.stickyIOFault() != nil {
					t.Fatalf("refusal %v counts=%+v", err, got)
				}
			}
			if got.Successes != 0 || s.CheckpointLSN() != 0 || s.dm.len() == 0 {
				t.Fatal("failed checkpoint advanced or lost retry ownership")
			}
		})
	}
}

func TestCheckpointDiagnostics_LogRateLimit(t *testing.T) {
	var out bytes.Buffer
	old := log.Writer()
	log.SetOutput(&out)
	t.Cleanup(func() { log.SetOutput(old) })
	var limiter checkpointLogLimiter
	now := time.Unix(100, 0)
	stale := fmt.Errorf("persist checkpoint: %w", mapCoverageRefusal("proof_changed"))
	limiter.report(stale, now)
	for i := 0; i < 5; i++ {
		limiter.report(stale, now.Add(time.Second))
	}
	// A terminal failure must remain visible while refusals are suppressed.
	terminal := errors.New("real fsync fault")
	limiter.report(terminal, now.Add(2*time.Second))
	limiter.report(terminal, now.Add(3*time.Second))
	limiter.report(stale, now.Add(30*time.Second))
	got := out.String()
	if strings.Count(got, "checkpoint deferred") != 2 || strings.Count(got, "real fsync fault") != 2 || !strings.Contains(got, "reason=proof_changed suppressed=5") {
		t.Fatalf("logs: %s", got)
	}
}
