package ops

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// P13 S4 closure tests. These pin the supported-set wording, the
// canonical entry points, and the read-only shape of the envelope
// so the frozen first-launch artifact cannot drift silently.
//
// Each test is short on purpose: it encodes one honesty invariant
// the final closure depends on. If any of these go red, someone
// has changed P13's frozen claim without an explicit edit.

// repoRoot walks up from the test file's directory until it finds
// go.mod. Keeps the doc-content tests portable across machines and
// temp directories.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found walking up from %s", wd)
		}
		dir = parent
	}
}

func readRepoFile(t *testing.T, relPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), relPath))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(b)
}

// TestP13Closure_ReadmeReferencesEnvelope proves the README names
// the bounded first-launch envelope and the validation workflow as
// the canonical entry points. If either link goes missing, a
// tester landing on README would fall back to design archaeology —
// exactly what S4 said must not happen.
func TestP13Closure_ReadmeReferencesEnvelope(t *testing.T) {
	body := readRepoFile(t, "README.md")
	for _, want := range []string{
		"docs/p13-support-envelope.md",
		"docs/first-launch-validation.md",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("README missing canonical link %q", want)
		}
	}
	if !strings.Contains(strings.ToLower(body), "bounded first-launch") {
		t.Fatal("README must carry the 'bounded first-launch' wording as the current product claim")
	}
}

// TestP13Closure_EnvelopeMarksClosed proves the envelope doc is
// stamped as the final P13 closure artifact. If someone later
// demotes it to "intermediate", this test fails and forces the
// status decision to be explicit.
func TestP13Closure_EnvelopeMarksClosed(t *testing.T) {
	body := readRepoFile(t, "docs/p13-support-envelope.md")
	if !strings.Contains(body, "P13 S4 release-gate artifact") {
		t.Fatal("envelope doc must declare itself the P13 S4 release-gate artifact")
	}
	if !strings.Contains(body, "frozen at P13 closure") {
		t.Fatal("envelope doc must mark supported / unsupported tables as frozen")
	}
	if !strings.Contains(body, "first-launch-validation.md") {
		t.Fatal("envelope doc must link to the validation workflow")
	}
}

// TestP13Closure_EnvelopeSupportedSetPinned proves every supported
// row referenced in the frozen envelope set is actually named. If
// a future edit quietly removes a supported row, this test fails.
func TestP13Closure_EnvelopeSupportedSetPinned(t *testing.T) {
	body := readRepoFile(t, "docs/p13-support-envelope.md")
	supported := []string{
		"Graceful persistent handoff convergence",
		"Graceful reopen + `Recover()` after handoff",
		"Catch-up branch",
		"Rebuild branch",
		"Abrupt stop after successful convergence",
		"Abrupt stop after catch-up branch",
		"Abrupt stop after rebuild branch",
		"Rejoin after abrupt stop",
		"Stale old-lineage traffic rejected after reopen",
		"Read-only diagnosis surface",
	}
	for _, row := range supported {
		if !strings.Contains(body, row) {
			t.Fatalf("envelope doc is missing frozen supported row %q", row)
		}
	}
}

// TestP13Closure_EnvelopeUnsupportedSetPinned proves the frozen
// unsupported items stay named. Demoting any of these to
// "supported" without a paired test row would drift the contract.
func TestP13Closure_EnvelopeUnsupportedSetPinned(t *testing.T) {
	body := readRepoFile(t, "docs/p13-support-envelope.md")
	unsupported := []string{
		"in-flight",                     // interrupted-in-flight abrupt stop
		"Caught-up handoff proactive fence",
		"Automated failover trigger",
		"Topology authority",
		"RF3",
		"Frontend protocols",
		"Operator governance",
		"Primary-side abrupt fault",
	}
	for _, row := range unsupported {
		if !strings.Contains(body, row) {
			t.Fatalf("envelope doc is missing frozen unsupported item %q", row)
		}
	}
}

// TestP13Closure_ValidationWorkflowIsOneDoc proves the canonical
// validation workflow lives at exactly one place and is
// self-contained enough to run without reading other docs first.
func TestP13Closure_ValidationWorkflowIsOneDoc(t *testing.T) {
	body := readRepoFile(t, "docs/first-launch-validation.md")
	for _, want := range []string{
		"TL;DR",
		"go test ./core/adapter",
		"go run ./cmd/sparrow",
		"/diagnose",
		"p13-support-envelope.md",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("validation workflow missing required anchor %q", want)
		}
	}
}

// TestP13Closure_DiagnoseSupportClaimIsStable pins the /diagnose
// support_claim wording. If someone widens the claim to imply
// topology/production/cluster closure, this fails.
func TestP13Closure_DiagnoseSupportClaimIsStable(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, _ := http.Get(url + "/diagnose")
	defer resp.Body.Close()
	var body diagnoseBody
	_ = json.NewDecoder(resp.Body).Decode(&body)

	claim := body.SupportClaim
	required := []string{"bounded", "RF2", "WALStore", "SmartWAL", "p13-support-envelope.md"}
	for _, s := range required {
		if !strings.Contains(claim, s) {
			t.Fatalf("/diagnose support_claim missing %q: %q", s, claim)
		}
	}
	forbidden := []string{"production", "complete", "topology", "failover", "RF3", "cluster"}
	low := strings.ToLower(claim)
	for _, s := range forbidden {
		if strings.Contains(low, strings.ToLower(s)) {
			t.Fatalf("/diagnose support_claim contains forbidden scope word %q: %q", s, claim)
		}
	}
}

// TestP13Closure_SurfaceEndpointSetIsStable pins the set of
// endpoints advertised by the surface map. Any accidental addition
// or removal trips this so the surface cannot silently expand (or
// shrink) the bounded first-launch contract.
func TestP13Closure_SurfaceEndpointSetIsStable(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, _ := http.Get(url + "/")
	defer resp.Body.Close()
	var idx surfaceIndex
	_ = json.NewDecoder(resp.Body).Decode(&idx)

	wantPaths := []string{"/status", "/projection", "/trace", "/watchdog", "/diagnose"}
	got := make(map[string]bool)
	for _, ep := range idx.Endpoints {
		got[ep.Path] = true
	}
	if len(got) != len(wantPaths) {
		t.Fatalf("surface endpoint count drifted: got %d (%v), want %d (%v)",
			len(got), got, len(wantPaths), wantPaths)
	}
	for _, p := range wantPaths {
		if !got[p] {
			t.Fatalf("surface map missing endpoint %q", p)
		}
	}
}
