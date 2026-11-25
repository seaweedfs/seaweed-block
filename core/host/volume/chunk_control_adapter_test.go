package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type fakeChunkProvider struct {
	cut      storage.ChunkCutResult
	cutErr   error
	cutCheck func(context.Context) error
	detach   storage.ChunkCutResult
	last     uint64
	cuts     int
	lastMin  uint64
}

type adapterManifestFiler struct {
	chunkstore.Filer
	mu    sync.Mutex
	blobs map[string][]byte
}

func (f *adapterManifestFiler) Put(_ context.Context, b []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := fmt.Sprintf("fixture-%d", len(f.blobs))
	f.blobs[id] = append([]byte(nil), b...)
	return id, nil
}

func (f *adapterManifestFiler) Get(_ context.Context, id string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.blobs[id]
	if !ok {
		return nil, chunkstore.ErrNotFound
	}
	return append([]byte(nil), b...), nil
}

func (f *fakeChunkProvider) CutVolume(ctx context.Context, _ string, minLSN uint64) (storage.ChunkCutResult, error) {
	f.cuts++
	f.lastMin = minLSN
	if f.cutCheck != nil {
		if err := f.cutCheck(ctx); err != nil {
			return storage.ChunkCutResult{}, err
		}
	}
	if f.cutErr != nil {
		return storage.ChunkCutResult{}, f.cutErr
	}
	return f.cut, nil
}

func (f *fakeChunkProvider) DetachVolume(context.Context, string) (storage.ChunkCutResult, uint64, error) {
	return f.detach, f.last, nil
}

func adapterFixture(t *testing.T) (*fakeChunkProvider, ChunkControl) {
	t.Helper()
	fp := &fakeChunkProvider{cut: storage.ChunkCutResult{H: 9, Head: storage.ChunkHead{Gen: 3, Epoch: 2, Holder: "r1", RemoteLSN: 9}}}
	f := &adapterManifestFiler{Filer: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(f, f, "/chunks")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	v := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	return fp, NewChunkControl(fp, cat, v)
}

func (f *fakeChunkProvider) SnapshotVolume(ctx context.Context, name string, req chunkstore.SnapshotRequest) (chunkstore.SnapshotInfo, error) {
	r, err := f.CutVolume(ctx, name, 0)
	if err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	return chunkstore.SnapshotInfo{VolumeID: name, Name: req.Name, HeadGen: r.Head.Gen, HeadEpoch: r.Head.Epoch, CutLSN: r.H, Class: req.Class}, nil
}

// adapterCatalog is a catalog holding volume v1 and a provider whose cut
// installs a published HEAD of v1.
func adapterCatalog(t *testing.T) (*fakeChunkProvider, *chunkstore.Catalog) {
	t.Helper()
	head := storage.ChunkHead{Gen: 3, Epoch: 2, Holder: "r1", RemoteLSN: 9, State: storage.ChunkHeadActive,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e2/c0/k1"}, 5: {Key: "chunks/e2/c5/k2"}}}
	f := &adapterManifestFiler{Filer: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}
	r, err := chunkstore.NewChunkRemote(f, f, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	head, _, err = r.PrepareManifest(context.Background(), storage.ChunkHead{}, head, 8*4096)
	if err != nil {
		t.Fatal(err)
	}
	fp := &fakeChunkProvider{cut: storage.ChunkCutResult{H: 9, Head: head}}
	cat, err := chunkstore.NewCatalog(f, "/swblock/chunks")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(context.Background(), "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	return fp, cat
}

// Snapshot validates before it cuts, then stores the HEAD its cut
// installed (§3.2); list and delete go to the catalog.

// The cut body carries the statistics the chunk layer measured for that
// cut (§3.1), so a caller can see what one cut cost: the barrier pause,
// the chunks and bytes it uploaded, the chunks it found unchanged.
func TestChunkControl_CutReturnsTheLayerStats(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	fp.cut.Stats = storage.ChunkCutStats{BarrierPauseNs: 1500, SyncNs: 700, CandidateChunks: 4, UploadedChunks: 3,
		UploadedBytes: 3 << 22, UnchangedChunks: 1, CASResult: "ok", TotalNs: 9000, BarrierCopyChunks: 2}
	r, err := ctl.Cut(ctx, "v1", 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Stats map[string]any `json:"stats"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"barrierPauseNs": float64(1500), "syncNs": float64(700), "candidateChunks": float64(4),
		"uploadedChunks": float64(3), "uploadedBytes": float64(3 << 22), "unchangedChunks": float64(1), "casResult": "ok",
		"totalNs": float64(9000), "barrierCopyChunks": float64(2), "barrierOverBound": false}
	for k, v := range want {
		if body.Stats[k] != v {
			t.Fatalf("cut body stats[%q] = %v, want %v (body %s)", k, body.Stats[k], v, raw)
		}
	}
}

// ChunkCutStatsBody mirrors storage.ChunkCutStats: a field added to the
// layer's statistics must also reach the body.
func TestChunkCutStatsBody_MirrorsTheLayerStats(t *testing.T) {
	layer, body := reflect.TypeOf(storage.ChunkCutStats{}), reflect.TypeOf(ChunkCutStatsBody{})
	if layer.NumField() != body.NumField() {
		t.Fatalf("storage.ChunkCutStats has %d fields, ChunkCutStatsBody %d", layer.NumField(), body.NumField())
	}
	for i := 0; i < layer.NumField(); i++ {
		lf, ok := body.FieldByName(layer.Field(i).Name)
		if !ok || lf.Type != layer.Field(i).Type {
			t.Fatalf("ChunkCutStatsBody lacks %s %s", layer.Field(i).Name, layer.Field(i).Type)
		}
	}
	// Every field is copied: set each one to a distinct value and compare.
	var s storage.ChunkCutStats
	v := reflect.ValueOf(&s).Elem()
	for i := 0; i < v.NumField(); i++ {
		switch f := v.Field(i); f.Kind() {
		case reflect.Int, reflect.Int64:
			f.SetInt(int64(i + 1))
		case reflect.Uint64:
			f.SetUint(uint64(i + 1))
		case reflect.String:
			f.SetString("s")
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Struct:
			for j := 0; j < f.NumField(); j++ {
				if f.Field(j).Kind() != reflect.Int64 {
					t.Fatalf("nested field %s.%s is not a duration counter", v.Type().Field(i).Name, f.Type().Field(j).Name)
				}
				f.Field(j).SetInt(int64(100*(i+1) + j + 1))
			}
		default:
			t.Fatalf("field %s: unhandled kind %s", v.Type().Field(i).Name, f.Kind())
		}
	}
	got := reflect.ValueOf(cutStatsBody(s))
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		if !reflect.DeepEqual(got.FieldByName(name).Interface(), v.Field(i).Interface()) {
			t.Fatalf("cutStatsBody drops %s: got %v, want %v", name, got.FieldByName(name).Interface(), v.Field(i).Interface())
		}
	}
}

func TestChunkControl_CutMinLSNAndDetach(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	r, err := ctl.Cut(ctx, "v1", 9)
	if err != nil || fp.lastMin != 9 || r != (ChunkCutResponse{VolumeID: "v1", CutLSN: 9, HeadGen: 3, HeadEpoch: 2, Holder: "r1"}) {
		t.Fatalf("cut: %+v %v (provider got minLSN %d)", r, err, fp.lastMin)
	}
	fp.detach = storage.ChunkCutResult{H: 12, Head: storage.ChunkHead{Gen: 4, Epoch: 2, State: storage.ChunkHeadDetached}}
	fp.last = 12
	d, err := ctl.Detach(ctx, "v1")
	if err != nil || d != (ChunkDetachResponse{VolumeID: "v1", CutLSN: 12, HeadGen: 4, HeadEpoch: 2, LastWrittenLSN: 12}) {
		t.Fatalf("detach: %+v %v", d, err)
	}
}

func TestChunkControl_NamedSnapshotUsesUrgentFallback(t *testing.T) {
	fp, ctl := adapterFixture(t)
	c := hostlimit.DefaultConfig(t.TempDir() + "/missing")
	l, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	fp.cutCheck = func(ctx context.Context) error {
		id, err := l.Admit(ctx, 1, nil)
		if err == nil {
			l.Release(id)
		}
		return err
	}
	if _, err = ctl.Cut(context.Background(), "v1", 0); !errors.Is(err, hostlimit.ErrBusy) {
		t.Fatalf("ordinary cut bypassed missing pool: %v", err)
	}
	info, err := ctl.Snapshot(context.Background(), "v1", ChunkSnapshotRequest{Name: "named", RequestID: "named-r1"})
	if err != nil || info.Name != "named" {
		t.Fatalf("named seal did not use urgent fallback: %+v %v", info, err)
	}
	if st := l.Snapshot().(hostlimit.Stats); st.Fallback != 1 || st.SkippedByClass[hostlimit.Normal] != 1 {
		t.Fatalf("wrong named/normal policy: %+v", st)
	}
}
