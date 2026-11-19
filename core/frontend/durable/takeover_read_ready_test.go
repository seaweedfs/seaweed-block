package durable_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Hold the return from a successful takeover CAS: an external observer can
// see the new owner while provider attachment is still in flight.
type heldTakeoverClaim struct {
	storage.ChunkRemote
	claimed chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *heldTakeoverClaim) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := r.ChunkRemote.CASHead(ctx, expected, next)
	if err != nil || next.Epoch != 4 {
		return gen, err
	}
	r.once.Do(func() { close(r.claimed) })
	select {
	case <-r.release:
		return gen, nil
	case <-ctx.Done():
		return gen, ctx.Err()
	}
}

// The frontend can connect after authority arrives but before the fresh
// host finishes attaching HEAD. That window must not become a read EIO.
func TestWALStoreChunk_TakeoverReadBeforeAttachIsRetryable(t *testing.T) {
	remote := &heldTakeoverClaim{ChunkRemote: seededRemote(t), claimed: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(remote.release) }) }
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p := chunkProvider(t, t.TempDir(), remote, view, 0)
	// Release before provider cleanup joins the authority/attach worker.
	defer release()
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	select {
	case <-remote.claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("takeover CAS never became visible")
	}
	head, err := remote.GetHead(context.Background())
	if err != nil || head.Epoch != 4 || head.Holder != "r1" {
		t.Fatalf("takeover HEAD: %+v, %v", head, err)
	}
	backend, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	handler := nvme.NewIOHandler(nvme.HandlerConfig{Backend: backend, BlockSize: 4096, VolumeSize: pcBlocks * 4096, NSID: 1})
	command := nvme.IOCommand{Opcode: 0x02, NSID: 1, SLBA: 33, NLB: 1}
	start := time.Now()
	result := handler.Handle(context.Background(), command)
	elapsed := time.Since(start)
	// Keep the actual deferred attach parked for the takeover race interval.
	time.Sleep(2500 * time.Millisecond)
	release()
	if result.SCT != nvme.SCTGeneric || result.SC != nvme.SCNamespaceNotReady || len(result.Data) != 0 {
		t.Fatalf("read before HEAD attach: SCT=%#x SC=%#x data=%d reason=%s; want retryable namespace not ready", result.SCT, result.SC, len(result.Data), result.Reason)
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("read before attach took %s", elapsed)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		result = handler.Handle(context.Background(), command)
		if result.AsError() == nil {
			break
		}
		if result.SCT != nvme.SCTGeneric || result.SC != nvme.SCNamespaceNotReady {
			t.Fatalf("read while attach settles: %v", result.AsError())
		}
		if time.Now().After(deadline) {
			t.Fatal("attach did not make the same read operational")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bytes.Equal(result.Data, block(0x33)) {
		t.Fatal("first operational read did not return exact HEAD bytes")
	}
	t.Logf("pre-attach read returned in %s; same read recovered exact HEAD bytes after 2.5s attach delay", elapsed)
}
