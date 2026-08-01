package snapshot

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175HTTPSRestoreRuntimeApplyThenActivate(t *testing.T) {
	manager, rec, want := createStreamFixture(t)
	target, store, released := newRuntimeRestoreTarget(t, rec)
	handler, err := NewRestoreRuntimeHandler(target, store, func() error {
		*released = true
		return nil
	}, "restore-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	client, err := NewHTTPSRestoreRuntime(server.Client(), "restore-token")
	if err != nil {
		t.Fatal(err)
	}
	req := RuntimeRestoreRequest{Endpoint: server.URL, Snapshot: rec, TargetVolumeID: "target-vol", TargetReplicaID: "r2"}
	result, err := client.Apply(context.Background(), req, manager)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != RestoreStateApplied || *released {
		t.Fatalf("result=%+v released=%v", result, *released)
	}
	for lba, expected := range want {
		got, err := store.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("LBA %d mismatch", lba)
		}
	}
	idempotent, err := client.Apply(context.Background(), req, manager)
	if err != nil || !idempotent.AlreadyApplied {
		t.Fatalf("idempotent result=%+v err=%v", idempotent, err)
	}
	marker, err := client.Activate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if marker.State != RestoreStateActivated || !*released {
		t.Fatalf("marker=%+v released=%v", marker, *released)
	}
}

func TestPhase175HTTPSRestoreRuntimeRejectsBadArchiveAndIdentity(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	target, store, released := newRuntimeRestoreTarget(t, rec)
	handler, err := NewRestoreRuntimeHandler(target, store, func() error { *released = true; return nil }, "right-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	req := RuntimeRestoreRequest{Endpoint: server.URL, Snapshot: rec, TargetVolumeID: "target-vol", TargetReplicaID: "r2"}

	wrongToken, err := NewHTTPSRestoreRuntime(server.Client(), "wrong-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongToken.Apply(context.Background(), req, manager); err == nil {
		t.Fatal("unauthorized restore succeeded")
	}
	client, err := NewHTTPSRestoreRuntime(server.Client(), "right-token")
	if err != nil {
		t.Fatal(err)
	}
	wrongIdentity := req
	wrongIdentity.TargetReplicaID = "r3"
	if _, err := client.Apply(context.Background(), wrongIdentity, manager); err == nil {
		t.Fatal("wrong target identity succeeded")
	}
	if _, err := client.Activate(context.Background(), req); err == nil {
		t.Fatal("activation before apply succeeded")
	}
	if _, err := client.Apply(context.Background(), req, corruptArchiveStreamer{manager: manager}); err == nil {
		t.Fatal("corrupt archive succeeded")
	}
	if marker := target.Marker(); marker.State != RestoreStatePending {
		t.Fatalf("marker=%+v", marker)
	}
	if *released {
		t.Fatal("failed restore released readiness")
	}
}

type corruptArchiveStreamer struct {
	manager *Manager
}

func (s corruptArchiveStreamer) StreamArchive(ctx context.Context, snapshotID string, w io.Writer) (Record, error) {
	var archive bytes.Buffer
	rec, err := s.manager.StreamArchive(ctx, snapshotID, &archive)
	if err != nil {
		return Record{}, err
	}
	data := corruptCopy(archive.Bytes(), archiveHeaderSize+recordHeaderSize+3)
	if _, err := w.Write(data); err != nil {
		return Record{}, err
	}
	return rec, nil
}

func newRuntimeRestoreTarget(t *testing.T, rec Record) (*RestoreTarget, storage.LogicalStorage, *bool) {
	t.Helper()
	root := t.TempDir()
	dataPath := filepath.Join(root, "target.bin")
	target, err := OpenRestoreTarget(RestoreTargetConfig{
		MarkerPath:      filepath.Join(root, "restore.json"),
		TargetDataPath:  dataPath,
		SnapshotID:      rec.SnapshotID,
		TargetVolumeID:  "target-vol",
		TargetReplicaID: "r2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := storage.NewBlockStore(rec.NumBlocks, rec.BlockSize)
	released := false
	return target, store, &released
}
