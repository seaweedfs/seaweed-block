package ops

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// These tests prove the P11 single-node product-surface contract:
//
//   - the surface is self-describing via GET /
//   - the surface map lists exactly the endpoints the mux wires
//   - /diagnose reads only from accepted lower truth, never inventing new state
//   - every empty state is honest (not a falsely-confident response)
//   - no endpoint uses cluster-shaped vocabulary
//
// The tests are the P11 honesty proof — they reject any future change
// that quietly adds cluster-shaped wording or a new authority path.

// TestSurfaceIndex_SelfDescribing proves GET / returns a surface map
// that names every endpoint the operator can use, plus the scope and
// a note that the surface is read-only.
func TestSurfaceIndex_SelfDescribing(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, err := http.Get(url + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET /: got %d, want 200", resp.StatusCode)
	}
	var idx surfaceIndex
	if err := json.NewDecoder(resp.Body).Decode(&idx); err != nil {
		t.Fatal(err)
	}
	if idx.Scope == "" {
		t.Fatal("surface index: empty scope")
	}
	if idx.Note == "" {
		t.Fatal("surface index: empty note")
	}
	wantPaths := map[string]bool{
		"/status":     true,
		"/projection": true,
		"/trace":      true,
		"/watchdog":   true,
		"/diagnose":   true,
	}
	for _, ep := range idx.Endpoints {
		if !wantPaths[ep.Path] {
			t.Fatalf("surface index mentions unexpected path %q", ep.Path)
		}
		if ep.Owns == "" {
			t.Fatalf("endpoint %q has empty Owns string", ep.Path)
		}
		delete(wantPaths, ep.Path)
	}
	if len(wantPaths) > 0 {
		t.Fatalf("surface index missing endpoints: %v", wantPaths)
	}
}

// TestSurfaceIndex_WordingIsSingleNode proves the P11 wording rule:
// the surface map must declare itself single-node and must not carry
// cluster-shaped vocabulary. Any drift here trips this test and
// forces an explicit decision.
func TestSurfaceIndex_WordingIsSingleNode(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, _ := http.Get(url + "/")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := strings.ToLower(string(body))

	if !strings.Contains(text, "single-node") {
		t.Fatalf("surface index must call itself single-node; body:\n%s", body)
	}
	forbidden := []string{"cluster", "failover", "promotion", "topology authority", "rf3"}
	for _, bad := range forbidden {
		if strings.Contains(text, bad) {
			t.Fatalf("surface index contains forbidden cluster-shaped word %q; body:\n%s", bad, body)
		}
	}
}

// TestDiagnose_EmptyStateIsHonest proves /diagnose does not invent
// certainty when nothing has run. Every field is either empty or a
// zero-valued structured summary.
func TestDiagnose_EmptyStateIsHonest(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, err := http.Get(url + "/diagnose")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/diagnose empty: got %d, want 200", resp.StatusCode)
	}
	var body diagnoseBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.CurrentDemo != "" {
		t.Fatalf("empty diagnose: CurrentDemo=%q, want empty", body.CurrentDemo)
	}
	if body.LastTraceStep != "" {
		t.Fatalf("empty diagnose: LastTraceStep=%q, want empty", body.LastTraceStep)
	}
	if body.WatchdogSummary.Total != 0 {
		t.Fatalf("empty diagnose: watchdog summary total=%d, want 0", body.WatchdogSummary.Total)
	}
	if body.Note == "" {
		t.Fatal("empty diagnose: Note should always explain where the full data lives")
	}
}

// TestDiagnose_OnlyReadsLowerTruth proves /diagnose does not produce
// fields that aren't derivable from projection, trace, or watchdog.
// The small shape is the surface's honesty promise: if the lower
// layers don't know something, /diagnose cannot make it up.
//
// Enforced structurally — if someone later adds a field to
// diagnoseBody that isn't sourced from the snapshot, this test
// should be updated to cover it.
func TestDiagnose_OnlyReadsLowerTruth(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, _ := http.Get(url + "/diagnose")
	defer resp.Body.Close()
	var body diagnoseBody
	_ = json.NewDecoder(resp.Body).Decode(&body)

	// Every top-level field must be one of the known names.
	raw, _ := json.Marshal(body)
	var asMap map[string]json.RawMessage
	_ = json.Unmarshal(raw, &asMap)
	allowed := map[string]bool{
		"current_demo":     true,
		"mode":             true,
		"session_phase":    true,
		"decision":         true,
		"reason":           true,
		"last_trace_step":  true,
		"watchdog_summary": true,
		"note":             true,
	}
	for k := range asMap {
		if !allowed[k] {
			t.Fatalf("/diagnose exposes field %q not in the allowed single-node set", k)
		}
	}
}

// TestSurfaceMutations_IncludesRootAndDiagnose tightens the existing
// mutation rejection to cover the two new endpoints. Any accidental
// exposure of a write path would flip this red.
func TestSurfaceMutations_IncludesRootAndDiagnose(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	for _, p := range []string{"/", "/diagnose"} {
		for _, m := range mutationMethods {
			req, _ := http.NewRequest(m, url+p, strings.NewReader(`{}`))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", m, p, err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotImplemented {
				t.Fatalf("%s %s: got %d, want 501", m, p, resp.StatusCode)
			}
		}
	}
}
