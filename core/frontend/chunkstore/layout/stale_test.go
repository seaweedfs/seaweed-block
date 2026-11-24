package layout

import "testing"

// Every pure transition refuses a caller that is not the holder at HEAD's
// epoch (Claim refuses an older epoch or the same epoch of another server).
func TestTransitions_RefuseAStaleHolder(t *testing.T) {
	stale := HolderID{Epoch: 2, Holder: "srv-a"}
	rn := RootName{Hash: Sum([]byte("r"))}
	for name, step := range map[string]func(Head) error{
		"Begin":      func(h Head) error { _, _, err := h.Begin(stale, "s"); return err },
		"Abort":      func(h Head) error { _, _, err := h.Abort(stale, "s"); return err },
		"Commit":     func(h Head) error { _, _, err := h.Commit(stale, "s", rn, 1, ""); return err },
		"BeginClean": func(h Head) error { _, _, err := h.BeginClean(stale, "c"); return err },
		"EndClean":   func(h Head) error { _, _, err := h.EndClean(stale, "c"); return err },
		"Claim":      func(h Head) error { _, _, err := h.Claim(stale); return err },
	} {
		for _, h := range []Head{idleHead(), snapHead("s"), cleanHead("c")} {
			if err := step(h); err == nil {
				t.Errorf("%s by epoch 2 over a HEAD at epoch 3 (%s) succeeded", name, h.Activity.Kind)
			}
		}
	}
}
