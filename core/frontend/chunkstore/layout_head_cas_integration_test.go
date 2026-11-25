//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// The store adapter's premise on a real filer (stock weed): HEAD is written
// only by a conditional write the filer itself refuses when the condition
// is false, and a refused write changes nothing. Every case sends a wrong
// condition to the filer (not refused locally) and then reads HEAD back:
// a second exclusive create, a stale generation, a future generation, and
// the right generation under another volume's identity.
func TestIntegration_HeadConditionalWritesRefusedByAStockFiler(t *testing.T) {
	c := startWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	other, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	root := "/swblock/heads/" + layout.VolumeDir("v1", id)
	cl := c.client(t, root+"/")
	s, err := NewLayoutStore(cl, cl, root, id)
	if err != nil {
		t.Fatal(err)
	}
	rn := layout.RootName{Hash: layout.Sum([]byte("root"))}
	at := func(vol layout.VolumeID, pos uint64) layout.Head {
		return layout.Head{VolumeID: vol, Epoch: 3, Holder: "node/r", ClaimID: "claim", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn, Position: pos, State: layout.StateLive}
	}
	// want reads HEAD back and requires generation gen and exactly h's bytes.
	want := func(step string, gen uint64, h layout.Head) {
		t.Helper()
		got, found, err := s.ReadHead(ctx)
		if err != nil || !found {
			t.Fatalf("%s: HEAD found %v, %v", step, found, err)
		}
		a, _ := got.Encode()
		b, _ := h.Encode()
		if got.Gen != gen || !bytes.Equal(a, b) {
			t.Fatalf("%s: HEAD at gen %d position %d; want gen %d position %d, unchanged bytes", step, got.Gen, got.Position, gen, h.Position)
		}
	}
	refused := func(step string, err error) {
		t.Helper()
		if !errors.Is(err, layout.ErrPrecondition) {
			t.Fatalf("%s: %v; want refused by the filer (precondition)", step, err)
		}
	}

	h1 := layout.Unclaimed(id, nil)
	if err := s.CreateHead(ctx, h1); err != nil {
		t.Fatal(err)
	}
	want("create", 1, h1)
	refused("a second exclusive create", s.UpdateHead(ctx, 0, at(id, 9)))
	want("after the refused create", 1, h1)

	h2 := at(id, 42)
	if err := s.UpdateHead(ctx, 1, h2); err != nil {
		t.Fatalf("update at the current generation: %v", err)
	}
	want("update", 2, h2)
	refused("a stale generation", s.UpdateHead(ctx, 1, at(id, 43)))
	want("after the stale update", 2, h2)
	refused("a future generation", s.UpdateHead(ctx, 5, at(id, 44)))
	want("after the future update", 2, h2)

	// The right generation, but the condition names another volume: the
	// write leaves the adapter (its own identity check passes) and the filer
	// refuses it on the volume attribute.
	foreign, err := NewLayoutStore(cl, cl, root, other)
	if err != nil {
		t.Fatal(err)
	}
	refused("another volume's identity", foreign.UpdateHead(ctx, 2, at(other, 45)))
	want("after the foreign update", 2, h2)

	// Control: the same write with the true condition lands.
	h3 := at(id, 46)
	if err := s.UpdateHead(ctx, 2, h3); err != nil {
		t.Fatalf("control update: %v", err)
	}
	want("control", 3, h3)
}
