package chunkstore

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// listDeleteFilerGRPC answers ListEntries and DeleteEntry the way
// seaweed-mono's filer does, with canned replies.
type listDeleteFilerGRPC struct {
	filerpb.UnimplementedSeaweedFilerServer

	mu         sync.Mutex
	listed     []*filerpb.Entry
	listErr    error
	lastList   *filerpb.ListEntriesRequest
	deleteResp *filerpb.DeleteEntryResponse
	deleteErr  error
	lastDelete *filerpb.DeleteEntryRequest
}

func (f *listDeleteFilerGRPC) ListEntries(r *filerpb.ListEntriesRequest, s grpc.ServerStreamingServer[filerpb.ListEntriesResponse]) error {
	f.mu.Lock()
	f.lastList = r
	listed, listErr := f.listed, f.listErr
	f.mu.Unlock()
	for _, e := range listed {
		if err := s.Send(&filerpb.ListEntriesResponse{Entry: e}); err != nil {
			return err
		}
	}
	return listErr
}

func (f *listDeleteFilerGRPC) DeleteEntry(_ context.Context, r *filerpb.DeleteEntryRequest) (*filerpb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDelete = r
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if f.deleteResp != nil {
		return f.deleteResp, nil
	}
	return &filerpb.DeleteEntryResponse{}, nil
}

func startListDeleteFiler(t *testing.T, f *listDeleteFilerGRPC) *SWFS {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filerpb.RegisterSeaweedFilerServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := DialSWFS(SWFSConfig{FilerGRPC: lis.Addr().String(), Collection: "swblock", Replication: "000", AssignPath: "/swblock/chunks"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSWFS_ListEntries_StreamsOnePage(t *testing.T) {
	f := &listDeleteFilerGRPC{listed: []*filerpb.Entry{{Name: "a"}, {Name: "b"}}}
	c := startListDeleteFiler(t, f)
	got, err := c.ListEntries(context.Background(), "/swblock/chunks/v1/snaps", "pre", "after", 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("listed %v, want [a b] in order", got)
	}
	r := f.lastList
	if r.Directory != "/swblock/chunks/v1/snaps" || r.Prefix != "pre" || r.StartFromFileName != "after" || r.InclusiveStartFrom || r.Limit != 7 {
		t.Fatalf("request %+v: want directory, prefix, exclusive start and limit as given", r)
	}
}

// A directory that does not exist lists as empty; any other failure is an
// error, never an empty listing.
func TestSWFS_ListEntries_MissingDirectoryIsEmpty(t *testing.T) {
	f := &listDeleteFilerGRPC{}
	c := startListDeleteFiler(t, f)
	ctx := context.Background()
	for _, e := range []error{status.Error(codes.NotFound, "nope"), errors.New("filer: no entry is found in filer store")} {
		f.listErr = e
		got, err := c.ListEntries(ctx, "/d", "", "", 10)
		if err != nil || len(got) != 0 {
			t.Fatalf("not-found style %v: got %v, %v; want an empty listing", e, got, err)
		}
	}
	f.listErr = status.Error(codes.Unavailable, "down")
	if _, err := c.ListEntries(ctx, "/d", "", "", 10); err == nil {
		t.Fatal("Unavailable: got nil error")
	}
}

func TestSWFS_DeleteEntry_ConditionalStyles(t *testing.T) {
	f := &listDeleteFilerGRPC{}
	c := startListDeleteFiler(t, f)
	ctx := context.Background()
	exp := map[string][]byte{"swb.snap.state": []byte("deleting")}

	if err := c.DeleteEntry(ctx, "/d", "x", exp); err != nil {
		t.Fatalf("ok delete: %v", err)
	}
	r := f.lastDelete
	if r.Directory != "/d" || r.Name != "x" || !r.IsDeleteData || r.IsRecursive || string(r.ExpectedExtended["swb.snap.state"]) != "deleting" {
		t.Fatalf("request %+v: want its data deleted, not recursive, expected_extended sent", r)
	}

	// The filer reports a failed precondition, including a missing entry, as
	// gRPC FailedPrecondition.
	f.deleteErr = status.Error(codes.FailedPrecondition, "extended attribute precondition /d/x: not found")
	if err := c.DeleteEntry(ctx, "/d", "x", exp); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("FailedPrecondition: got %v want ErrPrecondition", err)
	}
	// Other failures come back in the response's Error string: still errors.
	f.deleteErr, f.deleteResp = nil, &filerpb.DeleteEntryResponse{Error: "store failure"}
	if err := c.DeleteEntry(ctx, "/d", "x", exp); err == nil || errors.Is(err, ErrPrecondition) {
		t.Fatalf("response Error: got %v, want a plain error", err)
	}
	f.deleteResp, f.deleteErr = nil, status.Error(codes.Unavailable, "down")
	if err := c.DeleteEntry(ctx, "/d", "x", exp); err == nil || errors.Is(err, ErrPrecondition) {
		t.Fatalf("Unavailable: got %v, want a plain (ambiguous) error", err)
	}
}

// The in-memory filer used by the catalog tests follows the same rules.
func TestFakeFiler_ListAndConditionalDelete(t *testing.T) {
	f := newFakeFiler()
	ctx := context.Background()
	for _, n := range []string{"c", "a", "b", "ab"} {
		if err := f.CreateEntry(ctx, "/d", &filerpb.Entry{Name: n, Extended: map[string][]byte{"k": []byte("v")}}, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.CreateEntry(ctx, "/d/sub", &filerpb.Entry{Name: "z"}, true); err != nil {
		t.Fatal(err)
	}
	names := func(es []*filerpb.Entry) (out []string) {
		for _, e := range es {
			out = append(out, e.Name)
		}
		return
	}
	got, _ := f.ListEntries(ctx, "/d", "", "", 0)
	if want := []string{"a", "ab", "b", "c"}; !equalStrings(names(got), want) {
		t.Fatalf("list /d: %v, want %v (sorted, children only)", names(got), want)
	}
	got, _ = f.ListEntries(ctx, "/d", "a", "", 0)
	if want := []string{"a", "ab"}; !equalStrings(names(got), want) {
		t.Fatalf("prefix a: %v, want %v", names(got), want)
	}
	got, _ = f.ListEntries(ctx, "/d", "", "a", 2)
	if want := []string{"ab", "b"}; !equalStrings(names(got), want) {
		t.Fatalf("after a, limit 2: %v, want %v", names(got), want)
	}
	if got, err := f.ListEntries(ctx, "/missing", "", "", 0); err != nil || len(got) != 0 {
		t.Fatalf("missing directory: %v %v", got, err)
	}

	if err := f.DeleteEntry(ctx, "/d", "a", map[string][]byte{"k": []byte("other")}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("delete with a stale condition: %v, want ErrPrecondition", err)
	}
	if err := f.DeleteEntry(ctx, "/d", "nope", map[string][]byte{"k": []byte("v")}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("conditional delete of a missing entry: %v, want ErrPrecondition", err)
	}
	if err := f.DeleteEntry(ctx, "/d", "a", map[string][]byte{"k": []byte("v")}); err != nil {
		t.Fatalf("conditional delete: %v", err)
	}
	if err := f.DeleteEntry(ctx, "/d", "a", nil); err != nil {
		t.Fatalf("unconditional delete of a missing entry: %v, want nil", err)
	}
	if _, err := f.LookupEntry(ctx, "/d", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted entry still found: %v", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
