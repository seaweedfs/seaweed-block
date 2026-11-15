package storage

import (
	"context"
	"strings"
	"testing"
)

// checkIntentContract is the intent contract every chunkRemote meets (W5):
// ids are "<gen>-<nonce>" and unique per call, intents list back with what
// was written, and DeleteIntent removes only the named intent.
func checkIntentContract(t *testing.T, r chunkRemote) {
	t.Helper()
	ctx := context.Background()
	if got, err := r.ListIntents(ctx); err != nil || len(got) != 0 {
		t.Fatalf("intents before any PutIntent: %v %v", got, err)
	}
	a, err := r.PutIntent(ctx, chunkIntent{Gen: 7, Epoch: 2, Holder: "host-a", Keys: []string{"k1", "k2"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.PutIntent(ctx, chunkIntent{Gen: 7, Epoch: 2, Holder: "host-a", Keys: []string{"k3"}})
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !strings.HasPrefix(a, "7-") || !strings.HasPrefix(b, "7-") || len(a) <= len("7-") {
		t.Fatalf("intent ids %q and %q: want distinct \"7-<nonce>\"", a, b)
	}
	got, err := r.ListIntents(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListIntents: %v %v, want 2", got, err)
	}
	byID := map[string]chunkIntent{}
	for _, in := range got {
		byID[in.ID] = in
	}
	if in := byID[a]; in.Gen != 7 || in.Epoch != 2 || in.Holder != "host-a" || strings.Join(in.Keys, ",") != "k1,k2" || in.CreatedNs == 0 {
		t.Fatalf("intent %q listed as %+v", a, in)
	}
	if err := r.DeleteIntent(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteIntent(ctx, a); err != nil {
		t.Fatalf("deleting a missing intent: %v, want nil", err)
	}
	got, _ = r.ListIntents(ctx)
	if len(got) != 1 || got[0].ID != b {
		t.Fatalf("after deleting %q: %+v, want only %q", a, got, b)
	}
}

func TestMemChunkRemote_IntentContract(t *testing.T) { checkIntentContract(t, NewMemChunkRemote()) }

func TestFakeChunkRemote_IntentContract(t *testing.T) { checkIntentContract(t, newFakeChunkRemote()) }
