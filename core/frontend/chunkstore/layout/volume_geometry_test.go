package layout

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestVolumeRequiresGeometry(t *testing.T) {
	raw := []byte(`{"id":"01010101010101010101010101010101","name":"v","format":1}`)
	if _, err := DecodeVolume(raw); !errors.Is(err, ErrMalformed) {
		t.Fatalf("VOLUME without geometry accepted: %v", err)
	}
}

func TestVolumeGeometryChecksAndIsImmutable(t *testing.T) {
	good := Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}
	for _, bad := range []Geometry{{}, {SizeBytes: good.SizeBytes, ChunkBytes: good.ChunkBytes}, {SizeBytes: 1, BlockSize: 4096, ChunkBytes: 4096}, {SizeBytes: 4096, BlockSize: 4096, ChunkBytes: 1}} {
		if err := bad.Check(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("bad geometry accepted: %+v %v", bad, err)
		}
	}
	st := newMemNewVolume()
	v := Volume{ID: idFor("v"), Name: "v", Format: VolumeFormat, Geometry: good}
	if _, err := CreateVolume(context.Background(), st, v); err != nil {
		t.Fatal(err)
	}
	v.SizeBytes *= 2
	if _, err := CreateVolume(context.Background(), st, v); !errors.Is(err, ErrGeometryMismatch) {
		t.Fatal("resized volume accepted", err)
	}
}

func TestStartGeometryMismatchPrecedesClaim(t *testing.T) {
	h := startHead(1, "s/r", "k")
	for _, g := range []Geometry{{SizeBytes: 128 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, {SizeBytes: 64 * 4096, BlockSize: 512, ChunkBytes: 4 * 4096}, {SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 8 * 4096}} {
		st := &memHead{h: &h}
		_, err := Start(context.Background(), st, &fakeLease{ended: true}, HolderID{Epoch: 2, Holder: "s/r"}, LocalRoot{Geometry: g, ClaimID: "new", Fresh: true})
		if !errors.Is(err, ErrGeometryMismatch) || st.updates != 0 {
			t.Fatalf("geometry mismatch changed HEAD: %v writes=%d", err, st.updates)
		}
	}
}

func TestVolumeGeometryLivesOnlyInVolume(t *testing.T) {
	doc := reflect.TypeOf(volumeDoc{})
	want := map[string]bool{"size_bytes": true, "block_size": true, "chunk_bytes": true, "id": true, "name": true, "format": true}
	if doc.NumField() != len(want) {
		t.Fatal("VOLUME fields changed; enumerate the format", doc.NumField())
	}
	for i := 0; i < doc.NumField(); i++ {
		if !want[doc.Field(i).Tag.Get("json")] {
			t.Fatal("unexpected VOLUME field", doc.Field(i).Name)
		}
	}
	head := reflect.TypeOf(Head{})
	for _, name := range []string{"SizeBytes", "BlockSize", "ChunkBytes", "Geometry"} {
		if _, ok := head.FieldByName(name); ok {
			t.Fatal("geometry duplicated in HEAD", name)
		}
	}
}
