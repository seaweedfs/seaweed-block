package snapshot

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175HTTPSRestoreRuntimeApplyThenActivate(t *testing.T) {
	manager, rec, want := createStreamFixture(t)
	target, store, released := newRuntimeRestoreTarget(t, rec)
	handler, err := NewRestoreRuntimeHandler(target, func() error {
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
	req := runtimeRestoreRequestForTest(server.URL, rec)
	result, err := client.Apply(context.Background(), req, manager)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != RestoreStateApplied || result.TargetStorageID != "runtime-store" || result.TargetNumBlocks != rec.NumBlocks || result.TargetBlockSize != rec.BlockSize || result.RestoredBlocks != rec.RecordCount || result.RestoredBytes != rec.DataBytes || *released {
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
	if marker.State != RestoreStateActivated || marker.TargetStorageID != result.TargetStorageID || marker.TargetNumBlocks != result.TargetNumBlocks || marker.TargetBlockSize != result.TargetBlockSize || marker.TargetFrontier != result.TargetFrontier || !*released {
		t.Fatalf("marker=%+v released=%v", marker, *released)
	}
}

func TestPhase175HTTPSRestoreRuntimeRejectsBadArchiveAndIdentity(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	target, _, released := newRuntimeRestoreTarget(t, rec)
	handler, err := NewRestoreRuntimeHandler(target, func() error { *released = true; return nil }, "right-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	req := runtimeRestoreRequestForTest(server.URL, rec)

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
	wrongStore := req
	wrongStore.TargetStorageID = "replacement-store"
	if _, err := client.Apply(context.Background(), wrongStore, manager); err == nil {
		t.Fatal("wrong target store generation succeeded")
	}
	if marker := target.Marker(); marker.State != RestoreStatePending {
		t.Fatalf("wrong store request mutated target marker: %+v", marker)
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

func TestPhase175RestoreRuntimeRequiresMTLSAndToken(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	target, _, released := newRuntimeRestoreTarget(t, rec)
	restore, err := NewRestoreRuntimeHandler(target, func() error { *released = true; return nil }, "restore-token")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRuntimeMux(nil, restore)
	if err != nil {
		t.Fatal(err)
	}
	identity := writeRuntimeTLSIdentity(t)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()
	server, err := StartRuntimeServer(RuntimeServerConfig{
		Listen: addr, AdvertiseEndpoint: "https://" + addr, TLSCertFile: identity.serverCertFile, TLSKeyFile: identity.serverKeyFile, ClientCAFile: identity.caFile, Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	req := runtimeRestoreRequestForTest(server.Endpoint(), rec)
	withoutCertificate, err := NewHTTPSRestoreRuntime(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: identity.roots, MinVersion: tls.VersionTLS12,
	}}}, "restore-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withoutCertificate.Apply(context.Background(), req, manager); err == nil {
		t.Fatal("restore runtime accepted a client without mTLS identity")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: identity.roots, Certificates: []tls.Certificate{identity.clientCertificate}, MinVersion: tls.VersionTLS12,
	}}}
	wrongToken, err := NewHTTPSRestoreRuntime(client, "wrong-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongToken.Apply(context.Background(), req, manager); err == nil {
		t.Fatal("restore runtime accepted a wrong bearer token")
	}
	authorized, err := NewHTTPSRestoreRuntime(client, "restore-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authorized.Apply(context.Background(), req, manager); err != nil {
		t.Fatal(err)
	}
	if _, err := authorized.Activate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if !*released {
		t.Fatal("authorized restore did not release local readiness")
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
	mustPrepareRestoreStorage(t, target, "runtime-store", rec.NumBlocks, rec.BlockSize)
	if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := identifyRestoreStorage(dataPath, "runtime-store", storage.NewBlockStore(rec.NumBlocks, rec.BlockSize))
	if err := target.BindStorage(store); err != nil {
		t.Fatal(err)
	}
	released := false
	return target, store, &released
}

func runtimeRestoreRequestForTest(endpoint string, rec Record) RuntimeRestoreRequest {
	return RuntimeRestoreRequest{
		Endpoint:        endpoint,
		Snapshot:        rec,
		TargetVolumeID:  "target-vol",
		TargetReplicaID: "r2",
		TargetStorageID: "runtime-store",
		TargetNumBlocks: rec.NumBlocks,
		TargetBlockSize: rec.BlockSize,
	}
}
