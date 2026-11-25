package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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

// testIDs is the master's volume records the commands resolve against:
// every volume a test names through volArgs.
var testIDs sync.Map

// useMemChunkStore makes every ops chunk command dial store and resolve
// names against testIDs.
func useMemChunkStore(t *testing.T, store *memChunkStore) {
	t.Helper()
	prev, prevResolve := dialChunkFiler, resolveVolumeID
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return store, func() error { return nil }, nil }
	resolveVolumeID = testResolve
	t.Cleanup(func() { dialChunkFiler, resolveVolumeID = prev, prevResolve })
}

func testResolve(_ context.Context, _ string, name string) (layout.VolumeID, error) {
	if id, ok := testIDs.Load(name); ok {
		return id.(layout.VolumeID), nil
	}
	return layout.VolumeID{}, status.Error(codes.NotFound, "volume not found")
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
	testIDs.Store(v.Name, v.ID)
	return []string{"--" + flag, v.Name}
}

func cliArgs(cmd string, parts ...[]string) []string {
	out := []string{cmd, "--chunk-filer", "filer:18888", "--master", "m:1"}
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
	if code, out, _ := runChunk("snapshots", "--chunk-filer", "f:1", "--volume", "src"); code != 2 || !strings.Contains(out, "--master is required") {
		t.Fatalf("snapshots without --master: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("snapshots", []string{"--volume", "unknown-to-the-master"})...); code != 2 || !strings.Contains(out, "resolve --volume") {
		t.Fatalf("snapshots of a volume the master does not know: %d %q", code, out)
	}
}

// fakeLifecycle is the blockmaster's delete authority: the reservation is
// refused while held (a live line); leaseAfterReserve grants a lease right
// after the reservation is committed (a grant racing the admission), which
// the master's check then reports; every call is recorded.
type fakeLifecycle struct {
	control.UnimplementedLifecycleServiceServer
	mu                sync.Mutex
	held              bool
	leaseAfterReserve bool
	reserved, leased  bool
	calls             []string
}

func (f *fakeLifecycle) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeLifecycle) ReserveVolumeDeletion(_ context.Context, r *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	f.record("reserve")
	if f.held {
		return nil, status.Error(codes.FailedPrecondition, "volume has a live authority line")
	}
	f.reserved = true
	if f.leaseAfterReserve {
		f.leased = true
	}
	return &control.MarkVolumeDeletingResponse{Known: true}, nil
}

func (f *fakeLifecycle) CheckVolumeNoLiveLease(context.Context, *control.MarkVolumeDeletingRequest) (*control.MarkVolumeDeletingResponse, error) {
	f.record("check")
	if !f.reserved || f.leased {
		return nil, status.Error(codes.FailedPrecondition, "a line of the volume holds a live lease")
	}
	return &control.MarkVolumeDeletingResponse{Known: true}, nil
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
	del := func(lc *fakeLifecycle, v chunkstore.VolumeRef) (int, string) {
		addr := startFakeLifecycle(t, lc)
		code, out, _ := runChunk(cliArgs("volume-delete", volArgs("volume", v), []string{"--master", addr, "--request-id", "r1"})...)
		return code, out
	}
	objects := func(v chunkstore.VolumeRef) int {
		cat, _ := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
		u, err := cat.Usage(context.Background(), v)
		if err != nil {
			return -1
		}
		return u.OwnChunks + u.ForeignChunks
	}
	held := &fakeLifecycle{held: true}
	if code, out := del(held, tgt); code != chunkExitFailed || chunkErrorOut(t, out).Code != "not_admitted" || strings.Join(held.calls, ",") != "reserve" {
		t.Fatalf("delete of a held volume: %d %q calls %v", code, out, held.calls)
	}
	// A grant racing the reservation: the master's check between the HEAD
	// read and the update refuses, and nothing is deleted.
	race := &fakeLifecycle{leaseAfterReserve: true}
	if code, out := del(race, tgt); code != chunkExitFailed || chunkErrorOut(t, out).Code != "not_admitted" || strings.Join(race.calls, ",") != "reserve,check" || objects(tgt) != 1 {
		t.Fatalf("delete with a lease granted after the reservation: %d %q calls %v objects %d; want refused by the check, nothing deleted", code, out, race.calls, objects(tgt))
	}
	if h, _, _ := func() (layout.Head, bool, error) {
		cat, _ := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
		return cat.ReadHead(context.Background(), tgt)
	}(); h.State != layout.StateLive {
		t.Fatalf("HEAD after the refused delete: %s; want live", h.State)
	}
	if code, out := del(&fakeLifecycle{}, src); code != chunkExitFailed || chunkErrorOut(t, out).Code != "snapshot_has_dependents" {
		t.Fatalf("delete of a depended-on source: %d %q", code, out)
	}
	ok := &fakeLifecycle{}
	if code, out := del(ok, tgt); code != 0 || !strings.Contains(out, `"state":"deleted"`) || strings.Join(ok.calls, ",") != "reserve,check" {
		t.Fatalf("delete of the target: %d %q calls %v", code, out, ok.calls)
	}
	if code, out, _ := runChunk(cliArgs("snapshot-delete", volArgs("volume", src), []string{"--name", "daily"})...); code != 0 {
		t.Fatalf("delete the released snapshot: %d %q", code, out)
	}
}

// A volume never created: snapshots refuses volume_not_found, show prints
// a null head, volume-delete refuses before asking the master.
func TestOpsChunk_AbsentVolume(t *testing.T) {
	store := newMemChunkStore()
	useMemChunkStore(t, store)
	id, _ := layout.NewVolumeID()
	v := chunkstore.VolumeRef{Name: "never", ID: id}
	if code, out, _ := runChunk(cliArgs("snapshots", volArgs("volume", v))...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "volume_not_found" {
		t.Fatalf("snapshots of an absent volume: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("show", volArgs("volume", v))...); code != 0 || !strings.Contains(out, `"head":null`) {
		t.Fatalf("show of an absent volume: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("retain", volArgs("volume", v))...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "volume_not_found" {
		t.Fatalf("retain of an absent volume: %d %q", code, out)
	}
	lc := &fakeLifecycle{}
	addr := startFakeLifecycle(t, lc)
	if code, out, _ := runChunk(cliArgs("volume-delete", volArgs("volume", v), []string{"--master", addr, "--request-id", "r"})...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "volume_not_found" || len(lc.calls) != 0 {
		t.Fatalf("delete of an absent volume: %d %q calls %v; want volume_not_found before any admission", code, out, lc.calls)
	}
}

// addSnapshot names v's current image again as snapshot name (a new
// snapshot transaction with no new chunks).
func addSnapshot(t *testing.T, store *memChunkStore, v chunkstore.VolumeRef, name string, meta layout.SnapshotMeta, session string, position uint64) {
	t.Helper()
	ctx := context.Background()
	cat, _ := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
	s, _ := cat.Store(v)
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	h, _, _ := s.ReadHead(ctx)
	r, _ := layout.NewReader(defaultChunkPathCLI, v.Dir(), s.Fetch)
	img, err := r.Open(ctx, *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, _ := img.Resolve(ctx, 1)
	m := map[uint64]layout.Ref{1: ref}
	if _, err := layout.RunBegin(ctx, s, me, session); err != nil {
		t.Fatal(err)
	}
	root, segs, _ := layout.Build(m)
	rn, rb, _ := layout.NameRoot(root)
	objs := map[string][]byte{rn.Path(): rb}
	for n, b := range segs {
		objs[n.Path()] = b
	}
	for rel, b := range objs {
		if err := layout.EnsureObject(ctx, s, rel, b, session); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := layout.RunCommit(ctx, s, s, v.Name, me, layout.CommitRequest{Session: session, Prev: m, Next: m, Position: position, Snapshot: name, Meta: meta}); err != nil {
		t.Fatal(err)
	}
}

// retain stores a policy with --set-policy and applies it: the newest
// unpinned snapshots by creation time are kept, the expired ones deleted,
// a pinned one kept, one a restore target depends on refused and
// reported; --dry-run writes nothing.
func TestOpsChunk_Retain(t *testing.T) {
	store := newMemChunkStore()
	useMemChunkStore(t, store)
	v := cliVolume(t, store, "v", "")
	now := time.Unix(20_000_000, 0)
	prevNow := chunkRetainNow
	chunkRetainNow = func() time.Time { return now }
	t.Cleanup(func() { chunkRetainNow = prevNow })
	for i, f := range []struct {
		name  string
		hours int
		class string
	}{{"n1", 1, layout.ClassSemantic}, {"n2", 2, layout.ClassSemantic}, {"n3", 3, layout.ClassSemantic}, {"n4", 4, layout.ClassSemantic}, {"keep", 9, layout.ClassPinned}} {
		addSnapshot(t, store, v, f.name, layout.SnapshotMeta{CreatedNs: now.Add(-time.Duration(f.hours) * time.Hour).UnixNano(), Class: f.class}, fmt.Sprintf("s%d", i+2), uint64(i+2))
	}
	tid, _ := layout.NewVolumeID()
	if code, out, errs := runChunk(cliArgs("restore", volArgs("source", v), volArgs("target", chunkstore.VolumeRef{Name: "t", ID: tid}), []string{"--snapshot", "n4"})...); code != 0 {
		t.Fatalf("restore: %d %q %q", code, out, errs)
	}
	if code, out, errs := runChunk(cliArgs("retain", volArgs("volume", v), []string{"--set-policy", "--keep-newest", "2", "--keep-younger", "0s"})...); code != 0 || !strings.Contains(out, `"policySet":true`) {
		t.Fatalf("set policy: %d %q %q", code, out, errs)
	}
	run := func(extra ...string) chunkstore.RetainResult {
		t.Helper()
		code, out, errs := runChunk(cliArgs("retain", volArgs("volume", v), extra)...)
		var r chunkstore.RetainResult
		if code != 0 || json.Unmarshal([]byte(out), &r) != nil {
			t.Fatalf("retain %v: %d %q %q", extra, code, out, errs)
		}
		return r
	}
	if dry := run("--dry-run"); strings.Join(dry.Deleted, ",") != "n3,n4" {
		t.Fatalf("dry run: %+v", dry)
	}
	got := run()
	if strings.Join(got.Deleted, ",") != "n3" || strings.Join(got.Refused, ",") != "n4" {
		t.Fatalf("retain: deleted %v refused %v; want n3 and n4 refused", got.Deleted, got.Refused)
	}
	code, out, _ := runChunk(cliArgs("snapshots", volArgs("volume", v))...)
	var list []SnapshotEntry
	if code != 0 || json.Unmarshal([]byte(out), &list) != nil || len(list) != 4 {
		t.Fatalf("after retain: %d %q; want keep,n1,n2,n4", code, out)
	}
	if code, out, _ := runChunk(cliArgs("retain", volArgs("volume", v), []string{"--set-policy", "--dry-run"})...); code != 2 {
		t.Fatalf("--set-policy with --dry-run: %d %q", code, out)
	}
	if code, out, _ := runChunk(cliArgs("retain", volArgs("volume", v), []string{"--set-policy", "--keep-newest", "-1"})...); code != chunkExitFailed || chunkErrorOut(t, out).Code != "invalid_policy" {
		t.Fatalf("negative policy: %d %q", code, out)
	}
}
