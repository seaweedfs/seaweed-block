package durable_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func TestConditionalSealLeavesReplacementServing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := newLayoutCase(t, 64, 4)
	p := c.provider(t, t.TempDir(), viewAs("r1", 4), "node-a/r1", nil)
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	before := c.head(t)
	for _, want := range []durable.SealExpectation{
		{ReplicaID: "r1", Epoch: 3},
		{ReplicaID: "r2", Epoch: 4},
		{ReplicaID: "r1", Epoch: 4, ClaimID: "old-claim"},
	} {
		if _, _, err := p.DetachVolumePlainFor(ctx, "v1", want); !errors.Is(err, durable.ErrSealHolderChanged) {
			t.Fatalf("want changed holder refusal for %+v, got %v", want, err)
		}
		if after := c.head(t); !reflect.DeepEqual(before, after) {
			t.Fatalf("refused seal changed HEAD: before=%+v after=%+v", before, after)
		}
		if err := p.AdmitsIO("v1"); err != nil {
			t.Fatalf("replacement stopped serving: %v", err)
		}
		wantBytes := bytes.Repeat([]byte{0x47}, 4096)
		if _, err := b.Write(ctx, 0, wantBytes); err != nil {
			t.Fatalf("replacement cannot write: %v", err)
		}
		if err := b.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 4096)
		if _, err := b.Read(ctx, 0, got); err != nil || !bytes.Equal(got, wantBytes) {
			t.Fatalf("replacement readback: %v exact=%t", err, bytes.Equal(got, wantBytes))
		}
	}
}
