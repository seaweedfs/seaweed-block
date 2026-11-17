package storage

import (
	"errors"
	"testing"
)

// Row 8 publisher controls for the reset transaction (DESIGN 8.6).

var newNonceV3 = [16]byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF}

// The reset precondition is CheckpointLSN >= W (row 8 as coded: no v3 tail
// release exists, so retained old-incarnation bytes at or below the
// checkpoint are preserved in the extent and are not a refusal reason).
func TestResetV3RefusesWhileUncheckpointed(t *testing.T) {
	h := newDescHarnessV3(t)
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.W = 9 // acknowledged beyond the checkpoint (7)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	writes := h.writes.Load()
	if _, err := h.pub.resetIncarnationV3(newNonceV3, 0); !errors.Is(err, ErrResetNotPreserved) {
		t.Fatalf("uncheckpointed W: err = %v, want ErrResetNotPreserved", err)
	}
	if h.writes.Load() != writes {
		t.Fatal("a refused reset wrote a slot")
	}
}

func TestResetV3RefusesTheCurrentOrAZeroNonce(t *testing.T) {
	for _, n := range [][16]byte{{}, newDescHarnessV3(t).pub.current().Nonce} {
		h := newDescHarnessV3(t)
		if n != ([16]byte{}) {
			n = h.pub.current().Nonce
		}
		writes := h.writes.Load()
		if _, err := h.pub.resetIncarnationV3(n, 0); !errors.Is(err, errDescriptorTransition) {
			t.Fatalf("reset to nonce %x: err = %v, want errDescriptorTransition", n, err)
		}
		if h.writes.Load() != writes {
			t.Fatal("a refused reset wrote a slot")
		}
	}
}

func TestResetV3RetainedButCheckpointedBytesDoNotBlock(t *testing.T) {
	h := newDescHarnessV3(t)
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.RetainedHead = 4166 // one retained record, at or below the checkpoint
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	d, err := h.pub.resetIncarnationV3(newNonceV3, 0)
	if err != nil {
		t.Fatalf("reset with checkpointed retained bytes: %v", err)
	}
	if d.RetainedTail != 0 || d.RetainedHead != 0 || d.Nonce != newNonceV3 {
		t.Fatalf("reset result %+v", d)
	}
}

func TestResetV3PublishesBothSlotsAndOnlyThenHasAFloor(t *testing.T) {
	h := newDescHarnessV3(t)
	old := h.pub.current()
	d, err := h.pub.resetIncarnationV3(newNonceV3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if d.Nonce != newNonceV3 || d.SlotGen != old.SlotGen+2 || d.W != old.W || d.CheckpointLSN != old.CheckpointLSN {
		t.Fatalf("reset result %+v", d)
	}
	if floor, ok := h.pub.reuseFloor(); !ok || floor != 0 {
		t.Fatalf("after both publications: floor %d ok %v, want 0 true", floor, ok)
	}
	got, _ := h.onDisk()
	if got.Nonce != newNonceV3 || got.SlotGen != d.SlotGen {
		t.Fatal("reopen does not select the new incarnation")
	}
}

func TestResetV3CrashOutcomes(t *testing.T) {
	boom := errors.New("injected slot write error")
	t.Run("step 2 fails: the old image", func(t *testing.T) {
		h := newDescHarnessV3(t)
		old := h.pub.current()
		h.writeHook = func(int64, []byte) error { return boom }
		if _, err := h.pub.resetIncarnationV3(newNonceV3, 0); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the write error", err)
		}
		if got, _ := h.onDisk(); got != old {
			t.Fatal("a failed step 2 changed the selected image")
		}
	})
	t.Run("step 3 fails: the new image, mixed nonces, no reuse, hold", func(t *testing.T) {
		h := newDescHarnessV3(t)
		old := h.pub.current()
		n := 0
		h.writeHook = func(off int64, b []byte) error {
			n++
			if n == 2 {
				return boom
			}
			_, err := h.f.WriteAt(b, off)
			return err
		}
		if _, err := h.pub.resetIncarnationV3(newNonceV3, 0); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the step-3 write error (never a successful reset)", err)
		}
		if _, err := h.pub.publish(&h.storeMu, nil, nil); !errors.Is(err, ErrDescriptorPublishUncertain) {
			t.Fatalf("publication after a failed step 3: err = %v, want the latched uncertain failure", err)
		}
		// Reopen: the new incarnation is selected, the other slot is the old
		// incarnation, and no reuse floor exists across the two nonces.
		got, id := h.onDisk()
		if got.Nonce != newNonceV3 || got.SlotGen != old.SlotGen+1 {
			t.Fatalf("reopen after step 2 selected nonce %x gen %d", got.Nonce, got.SlotGen)
		}
		re := newDescriptorPublisherV3(got, id, &old, h.pub.io)
		if _, ok := re.reuseFloor(); ok {
			t.Fatal("a reuse floor was computed across the old and new incarnations")
		}
		if _, err := re.resetIncarnationV3([16]byte{0xEE}, 0); !errors.Is(err, errDescriptorTransition) {
			t.Fatalf("a second reset over a half-published one: err = %v, want refusal", err)
		}
	})
}

func TestResetV3OrdinaryPublicationsCannotChangeTheNonce(t *testing.T) {
	h := newDescHarnessV3(t)
	if _, err := h.pub.publish(&h.storeMu, nil, func(n *descriptorV3) error {
		n.Nonce = newNonceV3
		return nil
	}); !errors.Is(err, errDescriptorTransition) {
		t.Fatalf("nonce change outside the reset: err = %v, want errDescriptorTransition", err)
	}
}
