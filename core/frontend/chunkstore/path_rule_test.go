package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

func withFilerConf(t *testing.T, f *fakeFiler, body string) {
	t.Helper()
	e := &filerpb.Entry{Name: filerConfName, Content: []byte(body), Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body))}}
	if err := f.CreateEntry(context.Background(), filerConfDir, e, false); err != nil {
		t.Fatal(err)
	}
}

// The start check accepts an fsync rule over the chunk path (the form the
// filer writes for fs.configure -locationPrefix=/swblock/ -fsync) and
// refuses every rule that would expire, freeze or block the volume, a
// missing fsync, and a chunk path under the buckets directory.
func TestPathRule_StartCheck(t *testing.T) {
	const base = "/swblock/chunks"
	ok := `{"version":1,"locations":[{"locationPrefix":"/swblock/","collection":"swblock","fsync":true}]}`
	for name, tc := range map[string]struct {
		conf, buckets string
		refuse        string // "" accepts
	}{
		"fsync rule":                    {ok, "/buckets", ""},
		"snake_case names":              {`{"locations":[{"location_prefix":"/swblock/","fsync":true}]}`, "/buckets", ""},
		"fsync from an outer rule":      {`{"locations":[{"locationPrefix":"/","fsync":true},{"locationPrefix":"/swblock/","collection":"c"}]}`, "", ""},
		"no filer.conf":                 {"", "/buckets", "fsync"},
		"no rule on the path":           {`{"locations":[{"locationPrefix":"/other/","fsync":true}]}`, "", "fsync"},
		"rule without fsync":            {`{"locations":[{"locationPrefix":"/swblock/","collection":"c"}]}`, "", "fsync"},
		"ttl on the path":               {`{"locations":[{"locationPrefix":"/swblock/","fsync":true,"ttl":"7d"}]}`, "", "ttl"},
		"ttl inside the path":           {`{"locations":[{"locationPrefix":"/swblock/","fsync":true},{"locationPrefix":"/swblock/chunks/v1/","ttl":"1d"}]}`, "", "ttl"},
		"readOnly":                      {`{"locations":[{"locationPrefix":"/swblock/","fsync":true,"readOnly":true}]}`, "", "readOnly"},
		"worm":                          {`{"locations":[{"locationPrefix":"/swblock/","fsync":true,"worm":true}]}`, "", "worm"},
		"worm lifted by a deeper rule":  {`{"version":1,"locations":[{"locationPrefix":"/","fsync":true,"worm":true},{"locationPrefix":"/swblock/","worm":false}]}`, "", ""},
		"version 0 worm false is unset": {`{"locations":[{"locationPrefix":"/","fsync":true,"worm":true},{"locationPrefix":"/swblock/","worm":false}]}`, "", "worm"},
		"under the buckets directory":   {`{"locations":[{"locationPrefix":"/","fsync":true}]}`, "/swblock", "buckets"},
		"fsync turned off inside":       {`{"version":2,"locations":[{"locationPrefix":"/swblock/","fsync":true},{"locationPrefix":"/swblock/chunks/v1/","fsync":false}]}`, "", "fsync"},
		"fsync turned off on the path":  {`{"version":2,"locations":[{"locationPrefix":"/","fsync":true},{"locationPrefix":"/swblock/","fsync":false}]}`, "", "fsync"},
		"version 1 false is unset":      {`{"version":1,"locations":[{"locationPrefix":"/","fsync":true},{"locationPrefix":"/swblock/","fsync":false}]}`, "", ""},
		"disableChunkDeletion":          {`{"locations":[{"locationPrefix":"/swblock/","fsync":true,"disableChunkDeletion":true}]}`, "", "disableChunkDeletion"},
		"a sibling prefix":              {`{"locations":[{"locationPrefix":"/swblock/chunks2/","ttl":"1d"},{"locationPrefix":"/swblock/","fsync":true}]}`, "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeFiler()
			if tc.conf != "" {
				withFilerConf(t, f, tc.conf)
			}
			err := CheckPathRule(context.Background(), f, newFakeBlobs(), base, tc.buckets)
			switch {
			case tc.refuse == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.refuse != "" && (!errors.Is(err, ErrPathRule) || !strings.Contains(err.Error(), tc.refuse)):
				t.Fatalf("got %v; want a %s refusal", err, tc.refuse)
			}
		})
	}
}

// An unreadable or malformed filer.conf fails the check, never passes it.
func TestPathRule_UnreadableConfFails(t *testing.T) {
	f := newFakeFiler()
	withFilerConf(t, f, `{"locations":[`)
	if err := CheckPathRule(context.Background(), f, newFakeBlobs(), "/swblock/chunks", ""); err == nil {
		t.Fatal("malformed filer.conf accepted")
	}
}
