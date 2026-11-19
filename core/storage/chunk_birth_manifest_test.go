package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/protobuf/proto"
)

// Only the metadata/blob transports are in memory; capture, recovery,
// PrepareManifest, segment encoding, CAS and resolved reads are production.
type birthFiler struct {
	mu      sync.Mutex
	entries map[string]*filerpb.Entry
}

func (f *birthFiler) LookupEntry(_ context.Context, d, n string) (*filerpb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[path.Join(d, n)]
	if e == nil {
		return nil, chunkstore.ErrNotFound
	}
	return proto.Clone(e).(*filerpb.Entry), nil
}
func (f *birthFiler) CreateEntry(_ context.Context, d string, e *filerpb.Entry, excl bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := path.Join(d, e.Name)
	if excl && f.entries[k] != nil {
		return chunkstore.ErrExists
	}
	f.entries[k] = proto.Clone(e).(*filerpb.Entry)
	return nil
}
func (f *birthFiler) UpdateEntry(_ context.Context, d string, e *filerpb.Entry, expected map[string][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := path.Join(d, e.Name)
	old := f.entries[k]
	if old == nil {
		return chunkstore.ErrNotFound
	}
	for k, v := range expected {
		if !bytes.Equal(old.Extended[k], v) {
			return chunkstore.ErrPrecondition
		}
	}
	f.entries[k] = proto.Clone(e).(*filerpb.Entry)
	return nil
}
func (f *birthFiler) DeleteEntry(_ context.Context, d, n string, expected map[string][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := path.Join(d, n)
	for key, v := range expected {
		if f.entries[k] == nil || !bytes.Equal(f.entries[k].Extended[key], v) {
			return chunkstore.ErrPrecondition
		}
	}
	delete(f.entries, k)
	return nil
}
func (f *birthFiler) ListEntries(_ context.Context, d, prefix, after string, limit uint32) ([]*filerpb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*filerpb.Entry
	for k, e := range f.entries {
		if path.Dir(k) == d && strings.HasPrefix(e.Name, prefix) && e.Name > after {
			out = append(out, proto.Clone(e).(*filerpb.Entry))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if limit > 0 && len(out) > int(limit) {
		out = out[:limit]
	}
	return out, nil
}

type birthBlobs struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (b *birthBlobs) Put(_ context.Context, d []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := fmt.Sprint(len(b.data) + 1)
	b.data[id] = bytes.Clone(d)
	return id, nil
}
func (b *birthBlobs) Get(_ context.Context, id string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.data[id]
	if !ok {
		return nil, chunkstore.ErrNotFound
	}
	return bytes.Clone(d), nil
}

func TestChunkBirthDroppedSuffixPublishesWithRealManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := &birthFiler{entries: map[string]*filerpb.Entry{}}
	b := &birthBlobs{data: map[string][]byte{}}
	remote, err := chunkstore.NewChunkRemote(f, b, "/birth/v1")
	if err != nil {
		t.Fatal(err)
	}
	s := storage.DroppedSuffixBirthStoreForTest(t, remote)
	layer, err := storage.AttachChunkLayer(s, remote, storage.ChunkLayerConfig{ChunkBlocks: 4, Epoch: 2, Holder: "host-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = layer.Close() })
	cut, err := layer.Cut(ctx)
	if err != nil {
		t.Fatalf("reused-LSN cut must publish without another write: %v", err)
	}
	h, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cut.Head.RemoteLSN != 2 || h.RemoteLSN != 2 || h.Chunks[0].BirthLSN != 2 || h.Segments[0].MaxBirthLSN != 2 {
		t.Fatalf("cut-image frontier/birth: %+v", h)
	}
	data, err := remote.Get(ctx, h.Chunks[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 4*4096)
	copy(want[4096:], bytes.Repeat([]byte{0xB2}, 4096))
	if !bytes.Equal(data, want) {
		t.Fatal("published cut differs from recovered B2 image")
	}
	// The real builder's future-birth guard must still reject corrupt proposals.
	invalid := h
	invalid.Chunks = map[uint64]storage.ChunkRef{0: h.Chunks[0]}
	ref := invalid.Chunks[0]
	ref.BirthLSN = 3
	invalid.Chunks[0] = ref
	if _, _, err := remote.PrepareManifest(ctx, h, invalid, 4*4096); err == nil {
		t.Fatal("future birth was accepted")
	}
}
