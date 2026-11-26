package main_test

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// chunk show marshals layout.Head, not the on-store headDoc or the retired
// storage HEAD. Keep the procedure checks tied to that actual output type.
func TestProcedureLayoutHeadChecksReadProductShape(t *testing.T) {
	show := func(h layout.Head) string {
		t.Helper()
		b, err := json.Marshal(struct {
			Head layout.Head `json:"head"`
		}{h})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	sealed := layout.Head{Gen: 7, Epoch: 3, Holder: "s1/r1", ClaimID: "claim",
		Position: 91, State: layout.StateDetached, Current: &layout.RootName{Hash: layout.Hash{1}}}
	if got, err := sealedLayoutHeadPosition(show(sealed), 3, 7); err != nil || got != 91 {
		t.Fatalf("detached layout image: position=%d err=%v", got, err)
	}
	live := sealed
	live.Gen, live.Epoch, live.Holder, live.ClaimID, live.State = 8, 4, "s2/r1", "next-claim", layout.StateLive
	if err := checkLayoutHeadReopenedBy(show(live), 4, 7, 91, "s2"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*layout.Head)
	}{
		{"generation", func(h *layout.Head) { h.Gen = 6 }},
		{"epoch", func(h *layout.Head) { h.Epoch++ }},
		{"holder", func(h *layout.Head) { h.Holder = "foreign/r1" }},
		{"claim", func(h *layout.Head) { h.ClaimID = "" }},
		{"image", func(h *layout.Head) { h.Current = nil }},
		{"state", func(h *layout.Head) { h.State = layout.StateDeleting }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sealed
			tc.mutate(&h)
			if _, err := sealedLayoutHeadPosition(show(h), 3, 7); err == nil {
				t.Fatal("invalid detached proof accepted")
			}
			h = live
			tc.mutate(&h)
			if err := checkLayoutHeadReopenedBy(show(h), 4, 7, 91, "s2"); err == nil {
				t.Fatal("invalid successor accepted")
			}
		})
	}
	live.Position++
	if err := checkLayoutHeadReopenedBy(show(live), 4, 7, 91, "s2"); err == nil {
		t.Fatal("changed committed position accepted")
	}
}
