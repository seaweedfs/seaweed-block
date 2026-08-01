package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/seaweedfs/seaweed-block/core/ops"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

var testSnapshotID = canonicalTestSnapshotID("daily", "vol-a")

func TestPhase175SnapshotBackupCLI(t *testing.T) {
	oldFactory := snapshotBackupAPIFactory
	fake := &fakeSnapshotBackupAPI{}
	snapshotBackupAPIFactory = func(snapshotBackupAPIConfig) (snapshotBackupAPI, error) { return fake, nil }
	defer func() { snapshotBackupAPIFactory = oldFactory }()
	common := []string{"--api", "backup.example:9444", "--ca", "ca.pem", "--client-cert", "client.crt", "--client-key", "client.key", "--token-file", "token"}

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "export", args: append([]string{"ops", "snapshot-backup", "export", "--backup-id", "backup-a", "--snapshot-id", testSnapshotID}, common...), want: "snapshot_backup_operation=export backup_id=backup-a source_snapshot_id=" + testSnapshotID},
		{name: "get", args: append([]string{"ops", "snapshot-backup", "get", "--backup-id", "backup-a"}, common...), want: "snapshot_backup_operation=get backup_id=backup-a"},
		{name: "list", args: append([]string{"ops", "snapshot-backup", "list"}, common...), want: "backup_count=1"},
		{name: "import", args: append([]string{"ops", "snapshot-backup", "import", "--backup-id", "backup-a"}, common...), want: "snapshot_backup_operation=import backup_id=backup-a snapshot_id=" + testSnapshotID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, &stdout, &stderr); code != ops.VolumeStatusExitOK || !bytes.Contains(stdout.Bytes(), []byte(tc.want)) || stderr.Len() != 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
	if fake.exportBackupID != "backup-a" || fake.exportSnapshotID != testSnapshotID || fake.importBackupID != "backup-a" {
		t.Fatalf("fake calls=%+v", fake)
	}
}

func TestPhase175SnapshotBackupCLIRejectsMalformedSuccessResponses(t *testing.T) {
	oldFactory := snapshotBackupAPIFactory
	defer func() { snapshotBackupAPIFactory = oldFactory }()
	common := []string{"--api", "backup.example:9444", "--ca", "ca.pem", "--client-cert", "client.crt", "--client-key", "client.key", "--token-file", "token"}
	wrongImport := validSnapshotRecordFor("other", "vol-b")
	wrongFrontier := validSnapshotRecord()
	wrongFrontier.Frontier++
	wrongCreatedAt := validSnapshotRecord()
	wrongCreatedAt.CreatedAt = &timestamp.Timestamp{Seconds: wrongCreatedAt.CreatedAt.Seconds - 1}
	invalidTimestamp := validSnapshotRecord()
	invalidTimestamp.CreatedAt.Nanos = 1_000_000_000
	tests := []struct {
		name string
		args []string
		fake *fakeSnapshotBackupAPI
	}{
		{name: "nil-export", args: append([]string{"ops", "snapshot-backup", "export", "--backup-id", "backup-a", "--snapshot-id", testSnapshotID}, common...), fake: &fakeSnapshotBackupAPI{recordOverrideSet: true}},
		{name: "wrong-export-identity", args: append([]string{"ops", "snapshot-backup", "export", "--backup-id", "backup-a", "--snapshot-id", testSnapshotID}, common...), fake: &fakeSnapshotBackupAPI{recordOverrideSet: true, recordOverride: validBackupRecord("backup-b")}},
		{name: "nil-list-entry", args: append([]string{"ops", "snapshot-backup", "list"}, common...), fake: &fakeSnapshotBackupAPI{listOverride: []*control.SnapshotBackupRecord{nil}}},
		{name: "incomplete-import", args: append([]string{"ops", "snapshot-backup", "import", "--backup-id", "backup-a"}, common...), fake: &fakeSnapshotBackupAPI{importOverrideSet: true, importOverride: &control.SnapshotRecord{SnapshotId: testSnapshotID, State: "ready"}}},
		{name: "wrong-import-identity", args: append([]string{"ops", "snapshot-backup", "import", "--backup-id", "backup-a"}, common...), fake: &fakeSnapshotBackupAPI{importOverrideSet: true, importOverride: wrongImport}},
		{name: "wrong-import-frontier", args: append([]string{"ops", "snapshot-backup", "import", "--backup-id", "backup-a"}, common...), fake: &fakeSnapshotBackupAPI{importOverrideSet: true, importOverride: wrongFrontier}},
		{name: "wrong-import-created-at", args: append([]string{"ops", "snapshot-backup", "import", "--backup-id", "backup-a"}, common...), fake: &fakeSnapshotBackupAPI{importOverrideSet: true, importOverride: wrongCreatedAt}},
		{name: "invalid-import-timestamp", args: append([]string{"ops", "snapshot-backup", "import", "--backup-id", "backup-a"}, common...), fake: &fakeSnapshotBackupAPI{importOverrideSet: true, importOverride: invalidTimestamp}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshotBackupAPIFactory = func(snapshotBackupAPIConfig) (snapshotBackupAPI, error) { return tc.fake, nil }
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, &stdout, &stderr); code != ops.VolumeStatusExitInvalid || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestPhase175SnapshotBackupCLIRejectsIncompleteRequests(t *testing.T) {
	tests := [][]string{
		{"ops", "snapshot-backup"},
		{"ops", "snapshot-backup", "export", "--api", "x"},
		{"ops", "snapshot-backup", "list", "--api", "x", "--ca", "ca", "--client-cert", "cert", "--client-key", "key", "--token-file", "token", "--backup-id", "not-allowed"},
		{"ops", "snapshot-backup", "unknown", "--api", "x", "--ca", "ca", "--client-cert", "cert", "--client-key", "key", "--token-file", "token"},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != ops.VolumeStatusExitInvalid || stderr.Len() == 0 {
			t.Fatalf("args=%v code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
	}
}

type fakeSnapshotBackupAPI struct {
	exportBackupID    string
	exportSnapshotID  string
	importBackupID    string
	recordOverrideSet bool
	recordOverride    *control.SnapshotBackupRecord
	listOverride      []*control.SnapshotBackupRecord
	importOverrideSet bool
	importOverride    *control.SnapshotRecord
}

func (f *fakeSnapshotBackupAPI) record() *control.SnapshotBackupRecord {
	if f.recordOverrideSet {
		return f.recordOverride
	}
	return validBackupRecord("backup-a")
}

func validBackupRecord(backupID string) *control.SnapshotBackupRecord {
	archiveSHA := strings.Repeat("a", 64)
	snapshot := validSnapshotRecord()
	return &control.SnapshotBackupRecord{BackupId: backupID, SourceSnapshotId: testSnapshotID, CreatedAt: snapshot.CreatedAt, State: "ready", ArchiveBytes: snapshot.ArchiveBytes, ArchiveSha256: archiveSHA, ManifestSha256: strings.Repeat("b", 64), DestinationEvidence: "archives/" + backupID + ".sbbackup", Snapshot: snapshot}
}

func validSnapshotRecord() *control.SnapshotRecord {
	return validSnapshotRecordFor("daily", "vol-a")
}

func validSnapshotRecordFor(name, source string) *control.SnapshotRecord {
	return &control.SnapshotRecord{SnapshotId: canonicalTestSnapshotID(name, source), Name: name, SourceVolumeId: source, CreatedAt: &timestamp.Timestamp{Seconds: time.Now().Unix()}, State: "ready", Frontier: 1, SizeBytes: 4096, NumBlocks: 1, BlockSize: 4096, RecordCount: 1, DataBytes: 4096, ArchiveBytes: 4168, ArchiveSha256: strings.Repeat("a", 64)}
}

func canonicalTestSnapshotID(name, source string) string {
	digest := sha256.Sum256([]byte(source + "\x00" + name))
	return "snap-" + hex.EncodeToString(digest[:16])
}

func (f *fakeSnapshotBackupAPI) Export(_ context.Context, backupID, snapshotID string) (*control.SnapshotBackupRecord, error) {
	f.exportBackupID, f.exportSnapshotID = backupID, snapshotID
	return f.record(), nil
}

func (f *fakeSnapshotBackupAPI) Get(context.Context, string) (*control.SnapshotBackupRecord, error) {
	return f.record(), nil
}

func (f *fakeSnapshotBackupAPI) List(context.Context) ([]*control.SnapshotBackupRecord, error) {
	if f.listOverride != nil {
		return f.listOverride, nil
	}
	return []*control.SnapshotBackupRecord{f.record()}, nil
}

func (f *fakeSnapshotBackupAPI) Import(_ context.Context, backupID string) (*control.SnapshotRecord, error) {
	f.importBackupID = backupID
	if f.importOverrideSet {
		return f.importOverride, nil
	}
	return validSnapshotRecord(), nil
}

func (*fakeSnapshotBackupAPI) Close() error { return nil }
