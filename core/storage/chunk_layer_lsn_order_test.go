package storage

import (
	"context"
	"errors"
	"testing"
)

// MemChunkRemote refuses a proposal that lowers remote_lsn.
func TestMemChunkRemote_RefusesLowerRemoteLSN(t *testing.T) {
	ctx := context.Background()
	m := NewMemChunkRemote()
	gen, err := m.CASHead(ctx, chunkHeadVersion{}, chunkHead{Epoch: 1, Holder: "a", RemoteLSN: 10, Chunks: map[uint64]chunkRef{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CASHead(ctx, chunkHeadVersion{Gen: gen, Epoch: 1}, chunkHead{Epoch: 1, Holder: "a", RemoteLSN: 9, Chunks: map[uint64]chunkRef{}}); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("CAS lowering remote_lsn 10 -> 9: %v; want a precondition refusal", err)
	}
	if _, err := m.CASHead(ctx, chunkHeadVersion{Gen: gen, Epoch: 1}, chunkHead{Epoch: 1, Holder: "a", RemoteLSN: 10, Chunks: map[uint64]chunkRef{}}); err != nil {
		t.Fatalf("CAS keeping remote_lsn: %v", err)
	}
}
