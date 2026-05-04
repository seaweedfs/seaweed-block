package master

// T4a-5.0 follow-up: boundary pins for generationFitsUint32. The
// SubscribeAssignments publisher packs (epoch, endpoint_version)
// into a single uint64 as peer_set_generation; each half must fit
// in uint32 or distinct authority lines alias to the same
// generation and break the monotonic stale-replay guard on the
// volume side. QA finding #2 required this be asserted with
// explicit max-valid + overflow-rejected tests rather than
// comment-only.

import "testing"

// --- Test 1: max-valid pair accepted ---

// TestGenerationFitsUint32_MaxValidAccepted — the largest pair that
// still packs without aliasing. Epoch = 2^32 - 1, EV = 2^32 - 1.
// Must pass. Any tightening of the bound (e.g., adding <= instead
// of <) would fail this test, so the max-valid edge is pinned.
func TestGenerationFitsUint32_MaxValidAccepted(t *testing.T) {
	const maxU32 = uint64(1<<32) - 1

	reason, ok := generationFitsUint32(maxU32, maxU32)
	if !ok {
		t.Fatalf("max-valid pair (epoch=%d, ev=%d) rejected: %s", maxU32, maxU32, reason)
	}
	if reason != "" {
		t.Fatalf("max-valid returned non-empty reason: %q", reason)
	}

	// Individually at max: also valid.
	if _, ok := generationFitsUint32(maxU32, 1); !ok {
		t.Fatal("(max, 1) rejected")
	}
	if _, ok := generationFitsUint32(1, maxU32); !ok {
		t.Fatal("(1, max) rejected")
	}

	// Zero is valid at this level (generation==0 has its own
	// semantics on the volume side; this assertion is purely
	// about alias safety).
	if _, ok := generationFitsUint32(0, 0); !ok {
		t.Fatal("(0, 0) rejected")
	}
}

// --- Test 2: overflow rejected ---

// TestGenerationFitsUint32_OverflowRejected — 2^32 is the smallest
// value that would alias. Must be rejected with a descriptive
// reason that names the offending field + value. If either field
// slips past the check, the aliasing hazard returns silently.
func TestGenerationFitsUint32_OverflowRejected(t *testing.T) {
	const over = uint64(1) << 32

	// Epoch overflow.
	reason, ok := generationFitsUint32(over, 0)
	if ok {
		t.Fatal("epoch=2^32 must be rejected")
	}
	if reason == "" {
		t.Fatal("overflow must return non-empty reason")
	}
	if !containsAll(reason, "epoch", "alias") {
		t.Fatalf("epoch-overflow reason should mention 'epoch' + 'alias', got: %q", reason)
	}

	// EndpointVersion overflow.
	reason, ok = generationFitsUint32(0, over)
	if ok {
		t.Fatal("endpoint_version=2^32 must be rejected")
	}
	if !containsAll(reason, "endpoint_version", "alias") {
		t.Fatalf("ev-overflow reason should mention 'endpoint_version' + 'alias', got: %q", reason)
	}

	// Both overflow: still rejected (epoch check fires first —
	// either check firing is acceptable, we just pin "rejected").
	if _, ok := generationFitsUint32(over, over); ok {
		t.Fatal("both-overflow must be rejected")
	}

	// Far above threshold still rejected.
	if _, ok := generationFitsUint32(uint64(1)<<40, 0); ok {
		t.Fatal("epoch=2^40 must be rejected")
	}
	if _, ok := generationFitsUint32(0, ^uint64(0)); ok {
		t.Fatal("endpoint_version=uint64 max must be rejected")
	}
}

// --- helpers ---

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !stringContains(s, sub) {
			return false
		}
	}
	return true
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
