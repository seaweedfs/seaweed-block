package durable_test

// The attach-time check (B9 W2 §4.2, ruling 2026-11-15): one provider hook
// that runs before the chunk layer attaches, ahead of any remote read or
// CAS, with the local geometry; its refusal stops the attach.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// orderLog records the hook and the remote calls in the order they ran.
type orderLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *orderLog) add(op string) {
	l.mu.Lock()
	l.ops = append(l.ops, op)
	l.mu.Unlock()
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ops...)
}

type loggedRemote struct {
	storage.ChunkRemote
	log *orderLog
}

func (r loggedRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.log.add("gethead")
	return r.ChunkRemote.GetHead(ctx)
}

func (r loggedRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.log.add("cas")
	return r.ChunkRemote.CASHead(ctx, expected, next)
}

func checkedProvider(t *testing.T, remote storage.ChunkRemote, check func(context.Context, string, durable.ChunkGeometry) error) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:             durable.ImplWALStoreChunk,
		StorageRoot:      t.TempDir(),
		BlockSize:        4096,
		NumBlocks:        pcBlocks,
		ChunkBlocks:      pcChunkBlocks,
		ChunkRemoteFor:   func(string) (storage.ChunkRemote, error) { return remote, nil },
		ChunkAttachCheck: check,
	}, v1View())
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestWALStoreChunk_AttachCheckRunsFirstWithLocalGeometry(t *testing.T) {
	log := &orderLog{}
	var got durable.ChunkGeometry
	p := checkedProvider(t, loggedRemote{ChunkRemote: storage.NewMemChunkRemote(), log: log},
		func(_ context.Context, vol string, g durable.ChunkGeometry) error {
			log.add("check:" + vol)
			got = g
			return nil
		})
	openRecovered(t, p)
	ops := log.snapshot()
	if len(ops) < 2 || ops[0] != "check:v1" {
		t.Fatalf("attach ran %v; want the check first, before any remote call", ops)
	}
	if want := (durable.ChunkGeometry{NumBlocks: pcBlocks, BlockSize: 4096, ChunkBlocks: pcChunkBlocks}); got != want {
		t.Fatalf("check saw geometry %+v, want the local %+v", got, want)
	}
}

func TestWALStoreChunk_AttachCheckRefusalStopsAttach(t *testing.T) {
	refused := errors.New("geometry_mismatch")
	log := &orderLog{}
	p := checkedProvider(t, loggedRemote{ChunkRemote: storage.NewMemChunkRemote(), log: log},
		func(context.Context, string, durable.ChunkGeometry) error { return refused })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); !errors.Is(err, refused) {
		t.Fatalf("RecoverVolume with a refusing attach check: %v; want the refusal", err)
	}
	if ops := log.snapshot(); len(ops) != 0 {
		t.Fatalf("the remote was used although the check refused: %v", ops)
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrNotChunkVolume) {
		t.Fatalf("cut after the refused attach: %v; want ErrNotChunkVolume", err)
	}
}
