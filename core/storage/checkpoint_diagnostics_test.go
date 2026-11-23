package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
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
		// Simulate the effective-mutation class; newer WAL appends alone are safe.
		if err := s.v3.mapProof.mutatedV3(); err != nil {
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
					if err := s.v3.mapProof.mutatedV3(); err != nil {
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

// A starved checkpoint: flush cycles keep succeeding (clamped to the
// acknowledged frontier, nothing persisted moves) while the persisted
// CheckpointLSN and the last advance stay frozen. Only a real move of the
// persisted checkpoint counts as an advance.
func TestCheckpointDiagnostics_StarvedCheckpointIsVisible(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
	writeSynced(t, s, 1, 1)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	first := s.CheckpointPersistence()
	if first.CheckpointLSN != 1 || first.Advances != 1 || first.LastAdvance.IsZero() || first.Successes != 1 {
		t.Fatalf("first checkpoint: %+v", first)
	}
	for i := uint32(0); i < 3; i++ { // written, never synced: the checkpoint is clamped at 1
		if _, err := s.Write(2+i, blk(byte(2+i))); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
	}
	starved := s.CheckpointPersistence()
	if starved.Successes <= first.Successes {
		t.Fatalf("flush cycles did not keep succeeding: %+v", starved)
	}
	if starved.CheckpointLSN != 1 || starved.Advances != 1 || !starved.LastAdvance.Equal(first.LastAdvance) {
		t.Fatalf("a clamped success counted as an advance: first %+v starved %+v", first, starved)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	moved := s.CheckpointPersistence()
	if moved.CheckpointLSN != s.CheckpointLSN() || moved.CheckpointLSN <= 1 || moved.Advances != 2 || !moved.LastAdvance.After(first.LastAdvance) {
		t.Fatalf("after sync the checkpoint advance was not recorded: %+v", moved)
	}
}

// Only an advancing success moves LastAdvance; an unchanged success, a
// refusal and a failure leave the latest advance time as it was.
func TestCheckpointDiagnostics_LastAdvanceFollowsOnlyAdvances(t *testing.T) {
	var i flusherInstrumentation
	t0 := time.Unix(1000, 0)
	at := func() time.Time { return time.Unix(0, i.checkpointLastAdvanceNanos.Load()) }
	i.recordCheckpoint(nil, 0, 5, t0)
	if !at().Equal(t0) || i.checkpointAdvances.Load() != 1 {
		t.Fatalf("first advance: %v %d", at(), i.checkpointAdvances.Load())
	}
	i.recordCheckpoint(nil, 5, 5, t0.Add(time.Second))
	i.recordCheckpoint(mapCoverageRefusal("proof_changed"), 5, 5, t0.Add(2*time.Second))
	i.recordCheckpoint(errors.New("fsync fault"), 5, 5, t0.Add(3*time.Second))
	if !at().Equal(t0) || i.checkpointAdvances.Load() != 1 {
		t.Fatalf("a non-advance moved LastAdvance: %v advances=%d", at(), i.checkpointAdvances.Load())
	}
	t1 := t0.Add(4 * time.Second)
	i.recordCheckpoint(nil, 5, 9, t1)
	if !at().Equal(t1) || i.checkpointAdvances.Load() != 2 {
		t.Fatalf("second advance: %v %d", at(), i.checkpointAdvances.Load())
	}
	if i.checkpointSuccesses.Load() != 3 || i.checkpointRefusals.Load() != 1 || i.checkpointFailures.Load() != 1 {
		t.Fatalf("classification: s=%d r=%d f=%d", i.checkpointSuccesses.Load(), i.checkpointRefusals.Load(), i.checkpointFailures.Load())
	}
}

// The flusher retries an extent write that failed with ENOSPC every cycle; on
// a full local disk that is one log line per cycle (issue 484). It is logged
// once per 30 s with the count of suppressed repeats.
func TestCheckpointDiagnostics_NoSpaceRetryLogRateLimit(t *testing.T) {
	var out bytes.Buffer
	old := log.Writer()
	log.SetOutput(&out)
	t.Cleanup(func() { log.SetOutput(old) })
	var limiter checkpointLogLimiter
	now := time.Unix(100, 0)
	full := fmt.Errorf("flusher: write extent LBA 653: %w", &os.PathError{Op: "write", Path: "v1.bin", Err: syscall.ENOSPC})
	for i := 0; i < 100; i++ {
		limiter.report(full, now.Add(time.Duration(i)*100*time.Millisecond))
	}
	limiter.report(full, now.Add(31*time.Second))
	got := out.String()
	if n := strings.Count(got, "no space left on device"); n != 2 || !strings.Contains(got, "suppressed=99") {
		t.Fatalf("want 2 lines, the second with suppressed=99; got %d:\n%s", n, got)
	}
}
