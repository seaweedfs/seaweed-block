package snapshot

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175CoordinatorCapturesExactCurrentPrimary(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := SourceAuthority{VolumeID: "vol-a", ReplicaID: "r2", Epoch: 7, EndpointVersion: 3, RuntimeEndpoint: "https://10.0.0.2:24443"}
	resolver := &sequenceResolver{values: []SourceAuthority{authority, authority, authority}}
	runtime := &recordingRuntime{blocks: map[uint32][]byte{0: testBlock(0x21), 2: testBlock(0x23)}, frontier: 19, numBlocks: 4}
	coordinator, err := NewCoordinator(manager, resolver, runtime)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := coordinator.Create(context.Background(), CreateRequest{Name: "snap-a", SourceVolumeID: "vol-a"})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.calls != 1 || runtime.last.Source != authority || runtime.last.SnapshotName != "snap-a" {
		t.Fatalf("runtime calls=%d request=%+v", runtime.calls, runtime.last)
	}
	if rec.Frontier != 19 || rec.RecordCount != 2 || rec.SourceVolumeID != "vol-a" {
		t.Fatalf("record=%+v", rec)
	}

	resolver.err = ErrSourceNotReady
	retry, err := coordinator.Create(context.Background(), CreateRequest{Name: "snap-a", SourceVolumeID: "vol-a"})
	if err != nil || retry.SnapshotID != rec.SnapshotID || runtime.calls != 1 {
		t.Fatalf("idempotent retry=%+v calls=%d err=%v", retry, runtime.calls, err)
	}
}

func TestPhase175CoordinatorRejectsMissingOrInvalidSource(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		resolver *sequenceResolver
	}{
		{name: "resolver failure", resolver: &sequenceResolver{err: errors.New("no ready primary")}},
		{name: "missing runtime endpoint", resolver: &sequenceResolver{values: []SourceAuthority{{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}}}},
		{name: "wrong volume", resolver: &sequenceResolver{values: []SourceAuthority{{VolumeID: "vol-b", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: "https://runtime"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &recordingRuntime{numBlocks: 1}
			coordinator, err := NewCoordinator(manager, tc.resolver, runtime)
			if err != nil {
				t.Fatal(err)
			}
			_, err = coordinator.Create(context.Background(), CreateRequest{Name: tc.name, SourceVolumeID: "vol-a"})
			if !errors.Is(err, ErrSourceNotReady) || runtime.calls != 0 {
				t.Fatalf("error=%v runtime calls=%d", err, runtime.calls)
			}
		})
	}
}

func TestPhase175CoordinatorDiscardsCaptureWhenAuthorityChanges(t *testing.T) {
	manager, err := OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	before := SourceAuthority{VolumeID: "vol-a", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2, RuntimeEndpoint: "https://r1"}
	after := SourceAuthority{VolumeID: "vol-a", ReplicaID: "r2", Epoch: 5, EndpointVersion: 1, RuntimeEndpoint: "https://r2"}
	resolver := &sequenceResolver{values: []SourceAuthority{before, before, after}}
	runtime := &recordingRuntime{blocks: map[uint32][]byte{0: testBlock(0x42)}, frontier: 8, numBlocks: 1}
	coordinator, err := NewCoordinator(manager, resolver, runtime)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.Create(context.Background(), CreateRequest{Name: "failover-cut", SourceVolumeID: "vol-a"})
	if !errors.Is(err, ErrAuthorityChanged) {
		t.Fatalf("authority change error=%v", err)
	}
	if runtime.calls != 1 || len(manager.List("")) != 0 {
		t.Fatalf("runtime calls=%d published=%+v", runtime.calls, manager.List(""))
	}
}

type sequenceResolver struct {
	values []SourceAuthority
	index  int
	err    error
}

func (r *sequenceResolver) ResolveSnapshotSource(context.Context, string) (SourceAuthority, error) {
	if r.err != nil {
		return SourceAuthority{}, r.err
	}
	if len(r.values) == 0 {
		return SourceAuthority{}, ErrSourceNotReady
	}
	index := r.index
	if index >= len(r.values) {
		index = len(r.values) - 1
	}
	r.index++
	return r.values[index], nil
}

type recordingRuntime struct {
	blocks    map[uint32][]byte
	frontier  uint64
	numBlocks uint32
	calls     int
	last      RuntimeCaptureRequest
}

func (r *recordingRuntime) CaptureSnapshot(_ context.Context, req RuntimeCaptureRequest, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	r.calls++
	r.last = req
	cut := storage.SnapshotCut{Frontier: r.frontier, NumBlocks: r.numBlocks, BlockSize: 4096}
	for lba := uint32(0); lba < r.numBlocks; lba++ {
		data, ok := r.blocks[lba]
		if !ok {
			continue
		}
		if err := sink(lba, data); err != nil {
			return storage.SnapshotCut{}, err
		}
		cut.BlockCount++
		cut.DataBytes += uint64(len(data))
	}
	return cut, nil
}
