package storage

import (
	"bytes"
	"errors"
	"testing"
)

// INV-WS-SAME-HOST-CLEAN-MOVE-001: the returning host consumes its sealed
// image only with a newly issued epoch; same-epoch reopening stays sealed.
func TestClaimChunkHeadSameHostCleanMovePreservesSealedImage(t *testing.T) {
	s, _ := newChunkTestStore(t)
	r := newFakeChunkRemote()
	layer := mustAttach(t, s, r, ctConfig(1))
	want := makeBlock(4096, 0x63)
	if _, err := s.Write(0, want); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Detach(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	sealed := copyHead(r.stats().head)
	if _, err := ClaimChunkHead(ctTimeout(t), r, ctConfig(1)); !errors.Is(err, ErrChunkLayerDetached) || !sameHead(r.stats().head, sealed) {
		t.Fatal("same-epoch reopen consumed the seal", err)
	}
	claimed, err := ClaimChunkHead(ctTimeout(t), r, ctConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	expected := copyHead(sealed)
	expected.Gen++
	expected.Epoch = 2
	expected.State = ChunkHeadActive
	expected.DetachedBy = nil
	if !sameHead(claimed, expected) || !bytes.Equal(r.imageOf(t, claimed)[:4096], want) {
		t.Fatalf("same-host claim changed sealed image/frontier: sealed=%+v claimed=%+v", sealed, claimed)
	}
}
