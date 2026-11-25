package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// memChunkStore is a filer with its volume servers: entries in memory,
// needle payloads by fid.
type memChunkStore struct {
	*chunkstore.MemFiler
	mu    sync.Mutex
	blobs map[string][]byte
}

func newMemChunkStore() *memChunkStore {
	return &memChunkStore{MemFiler: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}
}

func (m *memChunkStore) Put(_ context.Context, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fid := fmt.Sprintf("1,%x", len(m.blobs)+1)
	m.blobs[fid] = append([]byte(nil), data...)
	return fid, nil
}

func (m *memChunkStore) Get(_ context.Context, fid string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[fid]
	if !ok {
		return nil, chunkstore.ErrBlobNotFound
	}
	return append([]byte(nil), b...), nil
}

// useMemChunkStore makes every ops chunk command dial store.
func useMemChunkStore(t *testing.T, store *memChunkStore) {
	t.Helper()
	prev := dialChunkFiler
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return store, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
}

type okLease struct{}

func (okLease) OldLeaseEnded(context.Context, uint64) error { return nil }

// cliVolume creates v with one chunk and a snapshot named snap of it,
// through the layout kernel's own steps.
func cliVolume(t *testing.T, store *memChunkStore, name, snap string) chunkstore.VolumeRef {
	t.Helper()
	ctx := context.Background()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: name, ID: id}
	cat, err := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	s, _ := cat.Store(v)
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	if _, err := layout.Start(ctx, s, okLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{ClaimID: "k", Fresh: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunBegin(ctx, s, me, "s1"); err != nil {
		t.Fatal(err)
	}
	data := []byte("chunk of " + name)
	ch := layout.NameChunk(1, data)
	next := map[uint64]layout.Ref{1: {Owner: name, OwnerID: id, Chunk: ch}}
	root, segs, _ := layout.Build(next)
	rn, rb, _ := layout.NameRoot(root)
	objs := map[string][]byte{ch.Path(): data, rn.Path(): rb}
	for n, b := range segs {
		objs[n.Path()] = b
	}
	for rel, b := range objs {
		if err := layout.EnsureObject(ctx, s, rel, b, "s1"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := layout.RunCommit(ctx, s, s, name, me, layout.CommitRequest{Session: "s1", Next: next, Position: 1, Snapshot: snap}); err != nil {
		t.Fatal(err)
	}
	return v
}

func volArgs(flag string, v chunkstore.VolumeRef) []string {
	return []string{"--" + flag, v.Name, "--" + flag + "-layout-id", v.ID.String()}
}

func cliArgs(cmd string, parts ...[]string) []string {
	out := []string{cmd, "--chunk-filer", "filer:18888"}
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// The catalog commands on the layout: list, show, usage, restore, release
// refused while the target names the source, snapshot delete refused while
// depended on, and their error codes.
func TestOpsChunk_CatalogCommands(t *testing.T) {
	store := newMemChunkStore()
	useMemChunkStore(t, store)
	src := cliVolume(t, store, "src", "daily")
	tid, _ := layout.NewVolumeID()
	tgt := chunkstore.VolumeRef{Name: "t", ID: tid}

	code, out, errs := runChunk(cliArgs("snapshots", volArgs("volume", src))...)
	var list []SnapshotEntry
	if code != 0 || json.Unmarshal([]byte(out), &list) != nil || len(list) != 1 || list[0].Name != "daily" {
		t.Fatalf("snapshots: %d %q %q", code, out, errs)
	}
	if code, out, _ := runChunk(cliArgs("show", volArgs("volume", src))...); code != 0 || !strings.Contains(out, `"Name":"src"`) || !strings.Contains(out, `"Epoch":1`) {
		t.Fatalf("show: %d %q", code, out)
	}
	if code, out, errs := runChunk(cliArgs("restore", volArgs("source", src), volArgs("target", tgt), []string{"--snapshot", "daily"})...); code != 0 || !strings.Contains(out, tgt.Dir()) {
		t.Fatalf("restore: %d %q %q", code, out, errs)
	}
	if code, out, _ := runChunk(cliArgs("usage", volArgs("volume", tgt))...); code != 0 || !strings.Contains(out, `"foreign_chunks":1`) {
		t.Fatalf("target usage: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("snapshot-delete", volArgs("volume", src), []string{"--name", "daily"})...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "snapshot_has_dependents" {
		t.Fatalf("delete a depended-on snapshot: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("release", volArgs("source", src), volArgs("target", tgt))...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "target_live" {
		t.Fatalf("release of a target still naming the source: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("restore", volArgs("source", src), volArgs("target", tgt), []string{"--snapshot", "weekly"})...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "snapshot_not_found" {
		t.Fatalf("restore of a missing snapshot: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("snapshots", []string{"--volume", "src"})...); code != 2 || chunkErrorOut(t, out).Code != "bad_request" {
		t.Fatalf("snapshots without the layout id: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("snapshots", []string{"--volume", "src", "--volume-layout-id", "zz"})...); code != 2 {
		t.Fatalf("snapshots with a bad layout id: %d %q", code, out)
	}
}

// fakeLifecycle is the blockmaster's delete admission: it refuses while
// held is set, records every call.
type fakeLifecycle struct {
	control.UnimplementedLifecycleServiceServer
	mu    sync.Mutex
	held  bool
	calls []string
}

func (f *fakeLifecycle) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeLifecycle) MarkVolumeDeleting(_ context.Context, r *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	f.record("mark")
	if f.held {
		return nil, status.Error(codes.FailedPrecondition, "volume has a live authority line")
	}
	return &control.MarkVolumeDeletingResponse{Known: true}, nil
}

func (f *fakeLifecycle) CommitVolumeDeleting(context.Context, *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	f.record("commit")
	return &control.MarkVolumeDeletingResponse{}, nil
}

func (f *fakeLifecycle) AbortVolumeDeleting(context.Context, *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	f.record("abort")
	return &control.MarkVolumeDeletingResponse{}, nil
}

func startFakeLifecycle(t *testing.T, f *fakeLifecycle) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	control.RegisterLifecycleServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// volume-delete: refused while the master holds the volume (nothing
// written); a volume whose snapshot has a dependent is refused and the
// admission given back; a restore target deletes, releases its source and
// the admission is committed.
func TestOpsChunk_VolumeDeleteOnTheLayout(t *testing.T) {
	store := newMemChunkStore()
	useMemChunkStore(t, store)
	src := cliVolume(t, store, "src", "daily")
	tid, _ := layout.NewVolumeID()
	tgt := chunkstore.VolumeRef{Name: "t", ID: tid}
	if code, out, errs := runChunk(cliArgs("restore", volArgs("source", src), volArgs("target", tgt), []string{"--snapshot", "daily"})...); code != 0 {
		t.Fatalf("restore: %d %q %q", code, out, errs)
	}
	lc := &fakeLifecycle{held: true}
	addr := startFakeLifecycle(t, lc)
	del := func(v chunkstore.VolumeRef) (int, string) {
		code, out, _ := runChunk(cliArgs("volume-delete", volArgs("volume", v), []string{"--master", addr, "--request-id", "r1"})...)
		return code, out
	}
	if code, out := del(tgt); code != chunkExitFailed || chunkErrorOut(t, out).Code != "not_admitted" || strings.Join(lc.calls, ",") != "mark" {
		t.Fatalf("delete of a held volume: %d %q calls %v", code, out, lc.calls)
	}
	lc.held, lc.calls = false, nil
	if code, out := del(src); code != chunkExitFailed || chunkErrorOut(t, out).Code != "snapshot_has_dependents" || strings.Join(lc.calls, ",") != "mark,abort" {
		t.Fatalf("delete of a depended-on source: %d %q calls %v", code, out, lc.calls)
	}
	lc.calls = nil
	if code, out := del(tgt); code != 0 || !strings.Contains(out, `"state":"deleted"`) || strings.Join(lc.calls, ",") != "mark,commit" {
		t.Fatalf("delete of the target: %d %q calls %v", code, out, lc.calls)
	}
	if code, out, _ := runChunk(cliArgs("snapshot-delete", volArgs("volume", src), []string{"--name", "daily"})...); code != 0 {
		t.Fatalf("delete the released snapshot: %d %q", code, out)
	}
}
