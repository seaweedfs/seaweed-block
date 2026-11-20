package durable_test

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"sync"
	"testing"
	"time"
)

type beltClaimNotice struct {
	storage.ChunkRemote
	claimed chan struct{}
	once    sync.Once
}

func (r *beltClaimNotice) CASHead(ctx context.Context, v storage.ChunkHeadVersion, h storage.ChunkHead) (uint64, error) {
	gen, err := r.ChunkRemote.CASHead(ctx, v, h)
	if err == nil {
		r.once.Do(func() { close(r.claimed) })
	}
	return gen, err
}

func TestHeadBeltProviderCloseCancelsCooling(t *testing.T) {
	r := &beltClaimNotice{ChunkRemote: seededRemote(t), claimed: make(chan struct{})}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks, ChunkHeadCoolingWindow: 3 * time.Second,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return r, nil },
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	backend := p.Backend("v1")
	ctx, cancel := context.WithCancel(context.Background())
	recovered := make(chan error, 1)
	closed := make(chan error, 1)
	closeStarted := false
	t.Cleanup(func() {
		cancel()
		if !closeStarted {
			go func() { closed <- p.Close() }()
		}
		select {
		case <-recovered:
		case <-time.After(5 * time.Second):
			t.Error("recovery did not join")
		}
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Error("provider close did not join")
		}
	})
	go func() { _, err := p.RecoverVolume(ctx, "v1"); recovered <- err }()
	select {
	case <-r.claimed:
	case <-time.After(30 * time.Second):
		t.Fatal("takeover did not reach cooling")
	}
	closeStarted = true
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		closed <- err // cleanup joins this completed owner too
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("provider Close waited for takeover cooling instead of canceling it")
	}
	if _, err := backend.Read(context.Background(), 0, make([]byte, 4096)); err == nil {
		t.Fatal("closed provider served I/O")
	}
}
