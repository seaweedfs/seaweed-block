package main

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

// Every command that dials the filer uploads under blockvolume's placement:
// the chunk path as the assign path (the operator's path rule: fsync,
// collection), and the same collection and replication flags.
func TestOpsChunk_UploadsFollowThePathRule(t *testing.T) {
	old := dialChunkFiler
	t.Cleanup(func() { dialChunkFiler = old })
	var got []chunkstore.SWFSConfig
	dialChunkFiler = func(cfg chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		got = append(got, cfg)
		return nil, nil, errors.New("stop after the dial")
	}
	place := []string{"--chunk-filer", "filer:18888", "--chunk-path", "/rule/blocks", "--chunk-collection", "blk", "--chunk-replication", "001"}
	for _, args := range [][]string{
		{"snapshots", "--volume", "v1"},
		{"snapshot-delete", "--volume", "v1", "--name", "s1"},
		{"fork", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--request-id", "r"},
		{"show", "--volume", "v1"},
		{"usage", "--volume", "v1"},
		{"retain", "--volume", "v1"},
		{"verify", "--volume", "v1"},
		{"volume-delete", "--volume", "v1", "--master", "127.0.0.1:1", "--request-id", "d1"},
		{"gc", "--master", "127.0.0.1:1"},
		{"restore", "--source", "v1", "--snapshot", "s1", "--target", "v3", "--request-id", "r"},
	} {
		n := len(got)
		runChunk(append(args, place...)...)
		if len(got) != n+1 {
			t.Errorf("%s: dialed %d times", args[0], len(got)-n)
			continue
		}
		want := chunkstore.SWFSConfig{FilerGRPC: "filer:18888", AssignPath: "/rule/blocks", Collection: "blk", Replication: "001"}
		if got[n] != want {
			t.Errorf("%s: dialed %+v; want %+v", args[0], got[n], want)
		}
	}
}
