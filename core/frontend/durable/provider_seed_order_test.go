package durable_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// A storage restart between the successful plain seal and teardown must not
// erase the proof the planned new host needs. Neither local-root nor fresh
// same-assignment reopen may reactivate it or expose I/O.
func TestRestoreEqualOwnerDetachedHEADStaysSealed(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			ctx := hangGuard(t)
			c := newLayoutCase(t, 64, 4)
			root := t.TempDir()
			makeProvider := func(root string, view *stubView, holder string) *durable.DurableProvider {
				return c.provider(t, root, view, holder, nil)
			}
			source := makeProvider(root, v1View(), "node-a/r1")
			s := openRecovered(t, source)
			want := block(0xA7)
			writeSync(t, s, 1, 0xA7)
			cut, _, err := source.DetachVolumePlain(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if !local {
				root = t.TempDir()
			}
			restarted := makeProvider(root, v1View(), "node-a/r1")
			if _, err := restarted.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.RecoverVolume(ctx, "v1"); !errors.Is(err, layout.ErrDetached) && !errors.Is(err, layout.ErrMissingLocalRoot) {
				t.Errorf("sealed incarnation reopened for serving: %v", err)
			}
			h := c.head(t)
			if h.Gen != cut.Head.Gen || h.Epoch != cut.Head.Epoch || h.Holder != cut.Head.Holder || h.State != layout.StateDetached || h.Position != cut.H {
				t.Errorf("restart changed sealed proof: HEAD=%+v seal=%+v", h, cut.Head)
			}
			be, err := restarted.Open(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := be.Read(ctx, 4096, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
				t.Errorf("sealed restart exposed I/O: %v", err)
			}
			if !bytes.Equal(c.block(t, 1), want) {
				t.Fatal("restart changed sealed bytes")
			}
			planned := makeProvider(t.TempDir(), viewAs("r2", 4), "node-b/r2")
			loaded := openRecovered(t, planned)
			if got, err := loaded.Read(1); err != nil || !bytes.Equal(got, want) {
				t.Fatal("planned new holder could not load sealed image", err)
			}
			taken := c.head(t)
			if taken.State != layout.StateLive || taken.Epoch != 4 || taken.Holder != "node-b/r2" || taken.Gen <= cut.Head.Gen || taken.Position != cut.H {
				t.Fatalf("planned takeover did not consume preserved seal: %+v", taken)
			}
		})
	}
}
