package testops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestControlManagerStartCompleteWritesActiveHistoryAndLocks(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	mgr := NewControlManager(root)
	mgr.Now = func() time.Time { return now }
	req := controlTestRequest("run-1")
	resources := ResourceSpec{Group: "m02-block-lab", Exclusive: []string{"node:m02", "iscsi:m02"}, Ports: []int{3260}}

	rec, err := mgr.Start(req, resources)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if rec.State != "running" || rec.SourceCommit != "abc123" || rec.ArtifactDir != req.ArtifactDir {
		t.Fatalf("active record mismatch: %+v", rec)
	}
	for _, name := range []string{"group_m02-block-lab.lock", "node_m02.lock", "iscsi_m02.lock", "port_3260.lock"} {
		if _, err := os.Stat(filepath.Join(root, "locks", name)); err != nil {
			t.Fatalf("missing lock %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "active", "run-1.json")); err != nil {
		t.Fatalf("missing active record: %v", err)
	}

	mgr.Now = func() time.Time { return now.Add(time.Minute) }
	if err := mgr.Complete(rec, StatusPass, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "active", "run-1.json")); !os.IsNotExist(err) {
		t.Fatalf("active record should be removed, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "history", "run-1.json")); err != nil {
		t.Fatalf("missing history record: %v", err)
	}
	if entries, err := os.ReadDir(filepath.Join(root, "locks")); err != nil || len(entries) != 0 {
		t.Fatalf("locks should be released entries=%v err=%v", entries, err)
	}
	events, err := os.ReadFile(filepath.Join(root, "events.jsonl"))
	if err != nil {
		t.Fatalf("missing events: %v", err)
	}
	if !strings.Contains(string(events), `"event":"start"`) || !strings.Contains(string(events), `"event":"complete"`) {
		t.Fatalf("events missing start/complete:\n%s", events)
	}
}

func TestControlManagerCompleteReleasesLocksWhenHistoryWriteFails(t *testing.T) {
	root := t.TempDir()
	mgr := NewControlManager(root)
	rec, err := mgr.Start(controlTestRequest("run-1"), ResourceSpec{Exclusive: []string{"node:m02"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(root, "history")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "history"), []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Complete(rec, StatusPass, nil); err == nil {
		t.Fatal("expected history write failure")
	}
	if entries, err := os.ReadDir(filepath.Join(root, "locks")); err != nil || len(entries) != 0 {
		t.Fatalf("locks should release even when history write fails entries=%v err=%v", entries, err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "active", "run-1.json"))
	if err != nil {
		t.Fatalf("terminal active error record should remain visible: %v", err)
	}
	if !strings.Contains(string(raw), `"state": "error"`) || !strings.Contains(string(raw), "control complete failed") {
		t.Fatalf("active record should show control completion error:\n%s", raw)
	}
}

func TestControlManagerStartCleansPartialLockOnWriteFailure(t *testing.T) {
	root := t.TempDir()
	mgr := NewControlManager(root)
	mgr.WriteLock = func(path string, data []byte) error {
		if strings.Contains(path, "node_m02") {
			return os.WriteFile(path, data, 0o644)
		}
		_ = os.WriteFile(path, []byte("partial"), 0o644)
		return errors.New("simulated lock write failure")
	}
	_, err := mgr.Start(controlTestRequest("run-1"), ResourceSpec{Exclusive: []string{"node:m02", "iscsi:m02"}})
	if err == nil || !strings.Contains(err.Error(), "simulated lock write failure") {
		t.Fatalf("expected simulated lock write failure, got %v", err)
	}
	if entries, err := os.ReadDir(filepath.Join(root, "locks")); err != nil || len(entries) != 0 {
		t.Fatalf("partial locks should be removed entries=%v err=%v", entries, err)
	}
	if _, err := os.Stat(filepath.Join(root, "active", "run-1.json")); !os.IsNotExist(err) {
		t.Fatalf("failed start should not create active record, err=%v", err)
	}
}

func TestControlManagerRejectsInvalidRunID(t *testing.T) {
	root := t.TempDir()
	mgr := NewControlManager(root)
	for _, runID := range []string{"../run-1", "nested/run-1", `nested\run-1`, "run..1"} {
		_, err := mgr.Start(controlTestRequest(runID), ResourceSpec{})
		if err == nil {
			t.Fatalf("Start(%q) succeeded, expected invalid run_id", runID)
		}
	}
	if entries, err := os.ReadDir(filepath.Join(root, "active")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("read active dir: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("invalid run_id should not create active records: %v", entries)
	}
	if _, err := mgr.Start(controlTestRequest("run-1"), ResourceSpec{}); err != nil {
		t.Fatalf("valid run_id rejected: %v", err)
	}
}

func TestControlManagerRejectsConflictingLock(t *testing.T) {
	root := t.TempDir()
	mgr := NewControlManager(root)
	resources := ResourceSpec{Exclusive: []string{"node:m02"}}
	rec, err := mgr.Start(controlTestRequest("run-1"), resources)
	if err != nil {
		t.Fatalf("start run-1: %v", err)
	}
	_, err = mgr.Start(controlTestRequest("run-2"), resources)
	if err == nil || !strings.Contains(err.Error(), `resource "node:m02" is locked`) {
		t.Fatalf("expected lock conflict, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "locks", "node_m02.lock")); err != nil {
		t.Fatalf("conflict should not remove owner lock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "active", "run-2.json")); !os.IsNotExist(err) {
		t.Fatalf("conflicting run should not get active record, err=%v", err)
	}
	if err := mgr.Complete(rec, StatusPass, nil); err != nil {
		t.Fatalf("complete owner: %v", err)
	}
}

func TestControlManagerListReturnsActiveAndHistory(t *testing.T) {
	root := t.TempDir()
	mgr := NewControlManager(root)
	rec, err := mgr.Start(controlTestRequest("run-1"), ResourceSpec{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := mgr.Complete(rec, StatusFail, context.Canceled); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := mgr.Start(controlTestRequest("run-2"), ResourceSpec{}); err != nil {
		t.Fatalf("start run-2: %v", err)
	}
	records, err := mgr.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got, want := len(records), 2; got != want {
		t.Fatalf("records=%d want %d: %+v", got, want, records)
	}
	states := map[string]string{}
	for _, rec := range records {
		states[rec.RunID] = rec.State
	}
	if states["run-1"] != "fail" || states["run-2"] != "running" {
		t.Fatalf("states=%v", states)
	}
}

func controlTestRequest(runID string) RunRequest {
	return RunRequest{
		SchemaVersion: SchemaVersion,
		Scenario:      "fake-scenario",
		Source:        SourceSpec{Repo: "seaweed_block", Commit: "abc123"},
		ArtifactDir:   filepath.Join(os.TempDir(), runID),
		RunID:         runID,
	}
}
