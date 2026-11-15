package durable_test

// A write observer whose ship order cannot follow the store's next LSN
// must not be installed with I/O left open.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

type rebasingObserver struct{ refuse bool }

func (o *rebasingObserver) Observe(context.Context, uint32, uint64, []byte) error { return nil }
func (o *rebasingObserver) Sync(context.Context, uint64) error                    { return nil }
func (o *rebasingObserver) RebaseShipLSN(uint64) error {
	if o.refuse {
		return errors.New("injected: writes pending")
	}
	return nil
}

func TestWriteObserver_RefusedRebaseKeepsIOClosed(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 64,
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	_ = openRecovered(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	be, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	sb := p.Backend("v1")
	data := bytes.Repeat([]byte{0x11}, 4096)

	sb.SetWriteObserver(&rebasingObserver{refuse: true})
	if _, err := be.Write(ctx, 0, data); err == nil {
		t.Fatal("write admitted after the observer's ship-LSN rebase was refused")
	}

	// A later install whose rebase succeeds reopens I/O.
	sb.SetWriteObserver(&rebasingObserver{})
	if _, err := be.Write(ctx, 0, data); err != nil {
		t.Fatalf("write after a successful observer install: %v", err)
	}
}
