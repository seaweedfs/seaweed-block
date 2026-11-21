package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// fakeFilerGRPC answers the real gRPC method paths with canned replies so the
// client's error mapping is tested against both SeaweedFS failure styles.
type fakeFilerGRPC struct {
	filerpb.UnimplementedSeaweedFilerServer

	mu         sync.Mutex
	createResp *filerpb.CreateEntryResponse
	createErr  error
	updateErr  error
	lookupResp *filerpb.LookupDirectoryEntryResponse
	lookupErr  error
	assignResp *filerpb.AssignVolumeResponse
	lookupVol  *filerpb.LookupVolumeResponse
	volLookups int // LookupVolume calls

	lastCreate *filerpb.CreateEntryRequest
	lastUpdate *filerpb.UpdateEntryRequest
	lastAssign *filerpb.AssignVolumeRequest
}

func (f *fakeFilerGRPC) CreateEntry(_ context.Context, r *filerpb.CreateEntryRequest) (*filerpb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastCreate = r
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createResp != nil {
		return f.createResp, nil
	}
	return &filerpb.CreateEntryResponse{}, nil
}

func (f *fakeFilerGRPC) UpdateEntry(_ context.Context, r *filerpb.UpdateEntryRequest) (*filerpb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastUpdate = r
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return &filerpb.UpdateEntryResponse{}, nil
}

func (f *fakeFilerGRPC) LookupDirectoryEntry(_ context.Context, r *filerpb.LookupDirectoryEntryRequest) (*filerpb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return f.lookupResp, nil
}

func (f *fakeFilerGRPC) AssignVolume(_ context.Context, r *filerpb.AssignVolumeRequest) (*filerpb.AssignVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAssign = r
	return f.assignResp, nil
}

func (f *fakeFilerGRPC) LookupVolume(_ context.Context, r *filerpb.LookupVolumeRequest) (*filerpb.LookupVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volLookups++
	return f.lookupVol, nil
}

func startFakeFiler(t *testing.T, f *fakeFilerGRPC) *SWFS {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filerpb.RegisterSeaweedFilerServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := DialSWFS(SWFSConfig{FilerGRPC: lis.Addr().String(), Collection: "swblock", Replication: "000", AssignPath: "/swblock/vols/v1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSWFS_CreateEntry_ErrorCodeStyle(t *testing.T) {
	f := &fakeFilerGRPC{}
	c := startFakeFiler(t, f)
	ctx := context.Background()

	// CreateEntry reports o_excl loss in ErrorCode with a nil gRPC error
	// (seaweed-mono server/filer_grpc_server.go CreateEntry).
	f.createResp = &filerpb.CreateEntryResponse{Error: "exists", ErrorCode: filerpb.FilerError_ENTRY_ALREADY_EXISTS}
	err := c.CreateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, true)
	if !errors.Is(err, ErrExists) {
		t.Fatalf("ENTRY_ALREADY_EXISTS: got %v want ErrExists", err)
	}
	if !f.lastCreate.OExcl {
		t.Fatalf("o_excl not sent")
	}

	f.createResp = &filerpb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filerpb.FilerError_PRECONDITION_FAILED}
	if err := c.CreateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, true); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("PRECONDITION_FAILED: got %v want ErrPrecondition", err)
	}

	// Any other non-empty Error is still a failure, never silent success.
	f.createResp = &filerpb.CreateEntryResponse{Error: "parent is a file", ErrorCode: filerpb.FilerError_PARENT_IS_FILE}
	if err := c.CreateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, true); err == nil {
		t.Fatalf("PARENT_IS_FILE: got nil error")
	}

	f.createResp = &filerpb.CreateEntryResponse{}
	if err := c.CreateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, true); err != nil {
		t.Fatalf("ok create: %v", err)
	}
}

func TestSWFS_UpdateEntry_FailedPreconditionStyle(t *testing.T) {
	f := &fakeFilerGRPC{}
	c := startFakeFiler(t, f)
	ctx := context.Background()
	exp := map[string][]byte{"swb.gen": []byte("3"), "swb.epoch": []byte("1")}

	f.updateErr = status.Error(codes.FailedPrecondition, `extended attribute "swb.gen" changed`)
	if err := c.UpdateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, exp); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("FailedPrecondition: got %v want ErrPrecondition", err)
	}
	if string(f.lastUpdate.ExpectedExtended["swb.gen"]) != "3" || string(f.lastUpdate.ExpectedExtended["swb.epoch"]) != "1" {
		t.Fatalf("expected_extended not sent: %v", f.lastUpdate.ExpectedExtended)
	}

	f.updateErr = status.Error(codes.NotFound, "not found /d/x")
	if err := c.UpdateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, exp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("NotFound: got %v want ErrNotFound", err)
	}

	f.updateErr = status.Error(codes.Unavailable, "down")
	err := c.UpdateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, exp)
	if err == nil || errors.Is(err, ErrPrecondition) || errors.Is(err, ErrNotFound) {
		t.Fatalf("Unavailable: got %v, want a plain (ambiguous) error", err)
	}

	f.updateErr = nil
	if err := c.UpdateEntry(ctx, "/d", &filerpb.Entry{Name: "x"}, exp); err != nil {
		t.Fatalf("ok update: %v", err)
	}
}

func TestSWFS_LookupEntry_NotFoundStyles(t *testing.T) {
	f := &fakeFilerGRPC{}
	c := startFakeFiler(t, f)
	ctx := context.Background()

	// The filer returns its raw sentinel, which crosses gRPC as Unknown.
	f.lookupErr = errors.New("filer: no entry is found in filer store")
	if _, err := c.LookupEntry(ctx, "/d", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Unknown+sentinel: got %v want ErrNotFound", err)
	}
	f.lookupErr = status.Error(codes.NotFound, "nope")
	if _, err := c.LookupEntry(ctx, "/d", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("NotFound: got %v want ErrNotFound", err)
	}
	f.lookupErr = nil
	f.lookupResp = &filerpb.LookupDirectoryEntryResponse{Entry: &filerpb.Entry{Name: "x", Content: []byte("m")}}
	e, err := c.LookupEntry(ctx, "/d", "x")
	if err != nil || string(e.Content) != "m" {
		t.Fatalf("found: %v %v", e, err)
	}
}

func TestSWFS_PutGet_RawUncompressedUpload(t *testing.T) {
	var (
		mu     sync.Mutex
		stored = map[string][]byte{}
		gotReq *http.Request
	)
	vol := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fid := strings.TrimPrefix(r.URL.Path, "/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			gotReq = r.Clone(context.Background())
			b, _ := io.ReadAll(r.Body)
			stored[fid] = b
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"size":` + "1" + `}`))
		case http.MethodGet:
			b, ok := stored[fid]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(b)
		}
	}))
	defer vol.Close()
	host := strings.TrimPrefix(vol.URL, "http://")

	f := &fakeFilerGRPC{
		assignResp: &filerpb.AssignVolumeResponse{FileId: "7,01abcdef", Auth: "tok", Fsync: true, Location: &filerpb.Location{Url: host}},
		lookupVol:  &filerpb.LookupVolumeResponse{LocationsMap: map[string]*filerpb.Locations{"7": {Locations: []*filerpb.Location{{Url: host}}}}},
	}
	c := startFakeFiler(t, f)
	ctx := context.Background()
	// Text-looking data: must still go up raw (no gzip, no type sniffing).
	data := bytes.Repeat([]byte("hello world\n"), 1000)
	fid, err := c.Put(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if fid != "7,01abcdef" {
		t.Fatalf("fid %q", fid)
	}
	if f.lastAssign.Collection != "swblock" || f.lastAssign.Path != "/swblock/vols/v1" || f.lastAssign.Count != 1 {
		t.Fatalf("assign request %+v", f.lastAssign)
	}
	mu.Lock()
	r := gotReq
	mu.Unlock()
	if r.URL.Query().Get("fsync") != "true" {
		t.Fatalf("fsync not carried onto upload: %s", r.URL.RawQuery)
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("content-type %q", ct)
	}
	if ce := r.Header.Get("Content-Encoding"); ce != "" {
		t.Fatalf("content-encoding %q, want none", ce)
	}
	if a := r.Header.Get("Authorization"); a != "BEARER tok" {
		t.Fatalf("authorization %q", a)
	}
	got, err := c.Get(ctx, fid)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("get mismatch")
	}
	if _, err := c.Get(ctx, "7,0000dead"); err == nil {
		t.Fatalf("get of missing fid returned nil error")
	}
}

func TestSWFS_Put_AssignErrorIsFailure(t *testing.T) {
	f := &fakeFilerGRPC{assignResp: &filerpb.AssignVolumeResponse{Error: "no free volumes"}}
	c := startFakeFiler(t, f)
	if _, err := c.Put(context.Background(), []byte("x")); err == nil {
		t.Fatalf("assign error swallowed")
	}
}
