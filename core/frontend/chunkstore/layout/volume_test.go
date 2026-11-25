package layout

import (
	"errors"
	"strings"
	"testing"
)

func TestVolumeID_RandomAndParsed(t *testing.T) {
	a, err := NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewVolumeID()
	if a.IsZero() || a == b {
		t.Fatalf("volume ids %s and %s: want two different non-zero ids", a, b)
	}
	back, err := ParseVolumeID(a.String())
	if err != nil || back != a {
		t.Fatalf("round trip of %s: %s %v", a, back, err)
	}
	for _, bad := range []string{"", strings.Repeat("0", 32), strings.ToUpper(a.String()), a.String()[:31], a.String() + "0", strings.Repeat("g", 32)} {
		if _, err := ParseVolumeID(bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("ParseVolumeID(%q): %v; want ErrMalformed", bad, err)
		}
	}
}

func TestVolume_GoldenEncodingRoundTrip(t *testing.T) {
	v := Volume{ID: idFor("v1"), Name: "v1", Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat}
	b, err := v.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"size_bytes":262144,"block_size":4096,"chunk_bytes":16384,"id":"` + idFor("v1").String() + `","name":"v1","format":1}`
	if string(b) != want {
		t.Fatalf("encoding\n got %s\nwant %s", b, want)
	}
	back, err := DecodeVolume(b)
	if err != nil || back != v {
		t.Fatalf("decode: %+v %v", back, err)
	}
	for name, bad := range map[string]Volume{
		"zero id":        {Name: "v1", Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat},
		"name with path": {ID: idFor("v1"), Name: "a/b", Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat},
		"invalid UTF-8":  {ID: idFor("v1"), Name: "a\xff", Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat},
		"other format":   {ID: idFor("v1"), Name: "v1", Format: 2},
	} {
		if _, err := bad.Encode(); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v; want ErrMalformed", name, err)
		}
	}
	for name, raw := range map[string]string{
		"unknown field": strings.Replace(want, `"format"`, `"extra":1,"format"`, 1),
		"whitespace":    strings.Replace(want, `,`, `, `, 1),
		"zero id":       strings.Replace(want, idFor("v1").String(), strings.Repeat("0", 32), 1),
		"not json":      "VOLUME",
	} {
		if _, err := DecodeVolume([]byte(raw)); !errors.Is(err, ErrMalformed) {
			t.Errorf("decode %s: %v; want ErrMalformed", name, err)
		}
	}
}
