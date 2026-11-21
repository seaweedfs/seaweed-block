package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

// A server-held CAS outlives its caller. No old-process code runs after the
// reopen: releaseDeferredCAS only evaluates the request's original guard.
func TestClaimChunkHeadSameOwnerReopenRejectsKilledProcessCAS(t *testing.T) {
	for _, gen := range []uint64{0, 7} {
		t.Run(fmt.Sprint(gen), func(t *testing.T) {
			r := newFakeChunkRemote()
			if gen != 0 {
				r.head = ChunkHead{Gen: gen, Epoch: 2, Holder: "host-a", Chunks: map[uint64]ChunkRef{}}
			}
			old := copyHead(r.head)
			old.Epoch, old.Holder = 2, "host-a"
			old.RemoteLSN = 1
			r.casModes = []fakeCASMode{casDeferred}
			if _, err := r.CASHead(context.Background(), ChunkHeadVersion{Gen: gen, Epoch: r.head.Epoch}, old); !errors.Is(err, errFakeCASUnknown) {
				t.Fatal("old cut was not left in flight", err)
			}
			claimed, err := ClaimChunkHead(context.Background(), r, ctConfig(2))
			if err != nil {
				t.Fatal(err)
			}
			if n := r.releaseDeferredCAS(); n != 0 {
				t.Fatalf("killed process CAS landed after reopen: applied=%d claimedGen=%d", n, claimed.Gen)
			}
			if claimed.Gen != gen+1 || claimed.RemoteLSN != 0 || claimed.Epoch != 2 || claimed.Holder != "host-a" {
				t.Fatalf("reopen must fence the old generation without changing the image or owner: %+v", claimed)
			}
		})
	}
}

func TestHeadBeltSameOwnerReopenPreservesAcknowledgedWrites(t *testing.T) {
	s, path, crash := crashableStore(t)
	r := newFakeChunkRemote()
	cfg := ctConfig(2)
	old := mustAttach(t, s, r, cfg)
	a := makeBlock(4096, 0x31)
	b := makeBlock(4096, 0x42)
	c := makeBlock(4096, 0x53)
	if _, err := s.Write(0, a); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, b); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	r.casModes = []fakeCASMode{casDeferred}
	if _, err := old.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatal("old cut was not unresolved", err)
	}
	// Close removes the old local publisher but cannot cancel a request already
	// retained by the server. It also must preserve that request's intent.
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	crash()
	s, _ = reopen(t, path)
	claimed, err := ClaimChunkHead(ctTimeout(t), r, cfg)
	if err != nil {
		t.Fatal(err)
	}
	next := mustAttach(t, s, r, cfg)
	landed := r.releaseDeferredCAS()
	if _, err := s.Write(2, c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	_, firstErr := next.Cut(ctTimeout(t))
	if firstErr != nil {
		t.Logf("new process first CAS after late request: %v; fenced=%t", firstErr, next.Fenced())
		if _, err := next.Cut(ctTimeout(t)); err != nil {
			t.Fatal("retry lost the new process's progress", err)
		}
	}
	image := r.imageOf(t, r.stats().head)
	for i, want := range [][]byte{a, b, c} {
		got, err := s.Read(uint32(i))
		if err != nil || !bytes.Equal(got, want) || !bytes.Equal(image[i*4096:(i+1)*4096], want) {
			t.Fatalf("acknowledged block %d lost locally or remotely: %v", i, err)
		}
	}
	if landed != 0 || firstErr != nil {
		t.Fatalf("reopen did not exclude old CAS: landed=%d claimedGen=%d firstErr=%v (all acknowledged bytes survived retry)", landed, claimed.Gen, firstErr)
	}
}
