package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"path"
	"testing"
)

func TestManifestFormatUpgradeRejectsUnsupportedOrInconsistentObjects(t *testing.T) {
	type mutation func(map[string]any)
	for _, tc := range []struct {
		name    string
		segment bool
		change  mutation
	}{
		{"levels", false, func(v map[string]any) { v["levels"] = 2 }},
		{"missing-levels", false, func(v map[string]any) { delete(v, "levels") }},
		{"head-version", false, func(v map[string]any) { v["format"] = 4 }},
		{"head-unknown", false, func(v map[string]any) { v["future"] = true }},
		{"segment-version-ref", false, func(v map[string]any) { v["segments"].(map[string]any)["0"].(map[string]any)["seg_format"] = 3 }},
		{"root-max", false, func(v map[string]any) { v["segments"].(map[string]any)["0"].(map[string]any)["max_birth_lsn"] = 41 }},
		{"root-max-future", false, func(v map[string]any) { v["segments"].(map[string]any)["0"].(map[string]any)["max_birth_lsn"] = 43 }},
		{"segment-version", true, func(v map[string]any) { v["format"] = 3 }},
		{"segment-unknown", true, func(v map[string]any) { v["future"] = true }},
		{"segment-max", true, func(v map[string]any) { v["max_birth_lsn"] = 41 }},
		{"segment-max-missing", true, func(v map[string]any) { delete(v, "max_birth_lsn") }},
		{"computed-max", true, func(v map[string]any) {
			for _, entry := range v["chunks"].(map[string]any) {
				entry.(map[string]any)["birth_lsn"] = 41
			}
		}},
		{"encoding", true, func(v map[string]any) { v["chunks"].(map[string]any)["0"].(map[string]any)["enc"] = "zstd" }},
		{"birth-future", true, func(v map[string]any) { v["chunks"].(map[string]any)["0"].(map[string]any)["birth_lsn"] = 43 }},
		{"birth-missing", true, func(v map[string]any) { delete(v["chunks"].(map[string]any)["0"].(map[string]any), "birth_lsn") }},
		{"hash-shape", true, func(v map[string]any) { v["chunks"].(map[string]any)["0"].(map[string]any)["sha256"] = "bad" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b, _, _, _ := gcFixture(t)
			r, h := publishSegmented(t, f, b)
			ctx := context.Background()
			e, err := f.LookupEntry(ctx, path.Join(catBase, "v1"), headName)
			if err != nil {
				t.Fatal(err)
			}
			var root map[string]any
			if err := json.Unmarshal(e.Content, &root); err != nil {
				t.Fatal(err)
			}
			if tc.segment {
				body, err := r.Get(ctx, h.Segments[0].Key)
				if err != nil {
					t.Fatal(err)
				}
				var object map[string]any
				if err := json.Unmarshal(body, &object); err != nil {
					t.Fatal(err)
				}
				tc.change(object)
				body, err = json.Marshal(object)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Put(ctx, "malformed-segment", body); err != nil {
					t.Fatal(err)
				}
				ref := root["segments"].(map[string]any)["0"].(map[string]any)
				ref["key"] = qualifyManifestKey("v1", "malformed-segment")
				ref["sha256"] = sha256.Sum256(body)
			} else {
				tc.change(root)
			}
			e.Content, err = json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.UpdateEntry(ctx, path.Join(catBase, "v1"), e, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := r.GetHead(ctx); err == nil {
				t.Fatal("invalid persisted schema accepted")
			}
		})
	}
}

func TestManifestFormatUpgradeReadsLegacyRootGoldens(t *testing.T) {
	for _, body := range []string{
		`{"format":1,"gen":1,"epoch":1,"holder":"old","remote_lsn":7,"chunks":{"0":{"key":"old","sha256":"0000000000000000000000000000000000000000000000000000000000000000"}}}`,
		`{"format":2,"gen":1,"epoch":1,"holder":"old","remote_lsn":7,"chunk_bytes":4096,"chunk_count":1,"segments":{"0":{"key":"@v1/old","sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"owner":"v1","count":1}}}`,
	} {
		h, err := DecodeHeadDocument("legacy", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if h.Segments == nil {
			if h.Chunks[0].Encoding != "full" || h.Chunks[0].BirthLSN != 7 {
				t.Fatal("legacy flat frontier not inherited")
			}
		} else if h.Segments[0].SegFormat != 1 || h.Segments[0].MaxBirthLSN != 7 {
			t.Fatal("legacy segment maximum not inherited")
		}
	}
}
