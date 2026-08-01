package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	coresnapshot "github.com/seaweedfs/seaweed-block/core/snapshot"
)

func TestPhase175RestoreDiscardCommandEmitsTerminalEvidence(t *testing.T) {
	root := t.TempDir()
	_, err := coresnapshot.OpenRestoreTarget(coresnapshot.RestoreTargetConfig{
		MarkerPath:      filepath.Join(root, "restored-a.restore.json"),
		TargetDataPath:  filepath.Join(root, "restored-a.bin"),
		SnapshotID:      "snap-abc",
		TargetVolumeID:  "restored-a",
		TargetReplicaID: "r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	exit := runRestoreDiscardCommand([]string{
		"--root", root,
		"--operation-id", "abort-001",
		"--snapshot-id", "snap-abc",
		"--volume-id", "restored-a",
		"--replica-id", "r1",
		"--evidence-file", filepath.Join(root, "evidence.json"),
	}, &stdout, &stderr)
	if exit != 0 {
		t.Fatalf("exit=%d stderr=%s", exit, stderr.String())
	}
	var evidence coresnapshot.RestoreDiscardResult
	if err := json.Unmarshal(stdout.Bytes(), &evidence); err != nil {
		t.Fatal(err)
	}
	if !evidence.MarkerRemoved || !evidence.DataRemoved || evidence.OperationID != "abort-001" {
		t.Fatalf("evidence=%+v", evidence)
	}
	if raw, err := os.ReadFile(filepath.Join(root, "evidence.json")); err != nil || !bytes.Equal(raw, stdout.Bytes()) {
		t.Fatalf("evidence file=%q error=%v stdout=%q", raw, err, stdout.Bytes())
	}
}
