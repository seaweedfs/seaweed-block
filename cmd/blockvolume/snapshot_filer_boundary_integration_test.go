//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// snapshotFilerBoundary forwards the stock filer API unchanged. One applied
// write's reply is held until the test kills the holder. Thus the receipt
// proves an actual Begin, object create or Commit, independent of a delay.
// Assumptions: one snapshot at a time, capture timer/schedule disabled, no GC.
// Writers enumerated: UpdateEntry(HEAD Begin/Commit), CreateEntry(pool).
// All other writers/readers pass through; pre-restart HEAD uses the backend.
type snapshotFilerBoundary struct {
	filerpb.UnimplementedSeaweedFilerServer
	backend    filerpb.SeaweedFilerClient
	dir        string
	id         layout.VolumeID
	mu         sync.Mutex
	kind, name string
	reached    chan struct{}
	release    chan struct{}
	hit        bool
}

func newSnapshotFilerBoundary(t *testing.T, cluster *w4WeedCluster) *snapshotFilerBoundary {
	t.Helper()
	conn, err := grpc.NewClient(cluster.filerGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	p := &snapshotFilerBoundary{backend: filerpb.NewSeaweedFilerClient(conn)}
	_, gp, _ := net.SplitHostPort(cluster.filerGRPC)
	listener, err := net.Listen("tcp", net.JoinHostPort(testListenHost(), gp))
	if err != nil {
		t.Fatal(err)
	}
	backendURL, err := url.Parse("http://" + cluster.filerHTTP)
	if err != nil {
		t.Fatal(err)
	}
	_, hp, _ := net.SplitHostPort(cluster.filerHTTP)
	httpListener, err := net.Listen("tcp", net.JoinHostPort(testListenHost(), hp))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: httputil.NewSingleHostReverseProxy(backendURL), ReadHeaderTimeout: 10 * time.Second}
	httpDone := make(chan struct{})
	go func() { defer close(httpDone); _ = httpServer.Serve(httpListener) }()
	server := grpc.NewServer()
	filerpb.RegisterSeaweedFilerServer(server, p)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		p.unblock()
		server.Stop()
		_ = listener.Close()
		<-done
		_ = httpServer.Close()
		<-httpDone
		_ = conn.Close()
	})
	cluster.filerGRPC = listener.Addr().String()
	cluster.filerHTTP = httpListener.Addr().String()
	return p
}

func (p *snapshotFilerBoundary) arm(kind, name string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kind, p.name = kind, name
	p.reached = make(chan struct{})
	p.release = make(chan struct{})
	p.hit = false
	return p.reached
}
func (p *snapshotFilerBoundary) unblock() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.release != nil {
		close(p.release)
		p.release = nil
	}
}
func (p *snapshotFilerBoundary) after(ctx context.Context, dir string, entry *filerpb.Entry) {
	p.mu.Lock()
	if p.hit || p.reached == nil || entry == nil || os.Getenv("E1_TEST_SKIP_BOUNDARY") == "1" {
		p.mu.Unlock()
		return
	}
	match := p.kind == "OBJECT" && path.Dir(dir) == path.Join(p.dir, "pool")
	if dir == p.dir && entry.Name == layout.HeadName {
		h, err := chunkstore.DecodeLayoutHeadEntry(entry, p.id)
		if err == nil {
			_, named := h.Snapshots[p.name]
			match = (p.kind == "BEGIN" && h.Activity.Kind == layout.ActSnap && !named) || (p.kind == "COMMIT" && named)
		}
	}
	if !match {
		p.mu.Unlock()
		return
	}
	p.hit = true
	close(p.reached)
	release := p.release
	p.mu.Unlock()
	select {
	case <-release:
	case <-ctx.Done():
	}
}
func (p *snapshotFilerBoundary) head(t *testing.T) layout.Head {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := p.backend.LookupDirectoryEntry(ctx, &filerpb.LookupDirectoryEntryRequest{Directory: p.dir, Name: layout.HeadName})
	if err != nil {
		t.Fatal(err)
	}
	h, err := chunkstore.DecodeLayoutHeadEntry(r.Entry, p.id)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func (p *snapshotFilerBoundary) LookupDirectoryEntry(ctx context.Context, r *filerpb.LookupDirectoryEntryRequest) (*filerpb.LookupDirectoryEntryResponse, error) {
	return p.backend.LookupDirectoryEntry(ctx, r)
}
func (p *snapshotFilerBoundary) CreateEntry(ctx context.Context, r *filerpb.CreateEntryRequest) (*filerpb.CreateEntryResponse, error) {
	out, err := p.backend.CreateEntry(ctx, r)
	if err == nil && out.GetError() == "" {
		p.after(ctx, r.Directory, r.Entry)
	}
	return out, err
}
func (p *snapshotFilerBoundary) UpdateEntry(ctx context.Context, r *filerpb.UpdateEntryRequest) (*filerpb.UpdateEntryResponse, error) {
	out, err := p.backend.UpdateEntry(ctx, r)
	if err == nil {
		p.after(ctx, r.Directory, r.Entry)
	}
	return out, err
}
func (p *snapshotFilerBoundary) DeleteEntry(ctx context.Context, r *filerpb.DeleteEntryRequest) (*filerpb.DeleteEntryResponse, error) {
	return p.backend.DeleteEntry(ctx, r)
}
func (p *snapshotFilerBoundary) AssignVolume(ctx context.Context, r *filerpb.AssignVolumeRequest) (*filerpb.AssignVolumeResponse, error) {
	return p.backend.AssignVolume(ctx, r)
}
func (p *snapshotFilerBoundary) LookupVolume(ctx context.Context, r *filerpb.LookupVolumeRequest) (*filerpb.LookupVolumeResponse, error) {
	return p.backend.LookupVolume(ctx, r)
}
func (p *snapshotFilerBoundary) GetFilerConfiguration(ctx context.Context, r *filerpb.GetFilerConfigurationRequest) (*filerpb.GetFilerConfigurationResponse, error) {
	return p.backend.GetFilerConfiguration(ctx, r)
}
func (p *snapshotFilerBoundary) ListEntries(r *filerpb.ListEntriesRequest, s grpc.ServerStreamingServer[filerpb.ListEntriesResponse]) error {
	stream, err := p.backend.ListEntries(s.Context(), r)
	if err != nil {
		return err
	}
	for {
		v, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = s.Send(v); err != nil {
			return err
		}
	}
}
