package conformance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/v3mini/runtime"
	"github.com/seaweedfs/seaweed-block/v3mini/schema"
)

// TestConformance_YAMLCases loads conformance cases from cases.yaml
// and replays each through the mini engine. Every case must pass.
func TestConformance_YAMLCases(t *testing.T) {
	casesPath := filepath.Join("cases.yaml")
	if _, err := os.Stat(casesPath); err != nil {
		t.Skipf("cases.yaml not found: %v", err)
	}

	suite, err := schema.LoadCaseSuite(casesPath)
	if err != nil {
		t.Fatalf("load cases: %v", err)
	}

	for _, c := range suite.Cases {
		t.Run(c.Name, func(t *testing.T) {
			result := runtime.Replay(c)
			if !result.Passed {
				for _, f := range result.Failures {
					t.Errorf("  FAIL: %s", f)
				}
				// Print trace for diagnosis.
				t.Log("Trace:")
				for _, te := range result.AllTrace {
					t.Logf("  [%s] %s", te.Step, te.Detail)
				}
			}
		})
	}
}

// TestConformance_Determinism replays the same case twice and
// verifies identical outputs. This is the core determinism proof.
func TestConformance_Determinism(t *testing.T) {
	casesPath := filepath.Join("cases.yaml")
	if _, err := os.Stat(casesPath); err != nil {
		t.Skipf("cases.yaml not found: %v", err)
	}

	suite, err := schema.LoadCaseSuite(casesPath)
	if err != nil {
		t.Fatalf("load cases: %v", err)
	}

	for _, c := range suite.Cases {
		t.Run(c.Name, func(t *testing.T) {
			r1 := runtime.Replay(c)
			r2 := runtime.Replay(c)

			if r1.FinalProjection != r2.FinalProjection {
				t.Fatalf("projection differs:\n  run1: %+v\n  run2: %+v",
					r1.FinalProjection, r2.FinalProjection)
			}
			if len(r1.AllCommands) != len(r2.AllCommands) {
				t.Fatalf("command count: %d vs %d", len(r1.AllCommands), len(r2.AllCommands))
			}
			for i := range r1.AllCommands {
				if r1.AllCommands[i] != r2.AllCommands[i] {
					t.Fatalf("command %d: %s vs %s", i, r1.AllCommands[i], r2.AllCommands[i])
				}
			}
		})
	}
}

// TestConformance_LoaderRejectsMalformed verifies that the loader
// rejects malformed inputs instead of silently accepting them.
func TestConformance_LoaderRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{"missing_version", `cases: [{name: "x", events: [{kind: "ProbeSucceeded"}]}]`},
		{"missing_case_name", `version: v1\ncases: [{events: [{kind: "ProbeSucceeded"}]}]`},
		{"missing_event_kind", `version: v1\ncases: [{name: "x", events: [{}]}]`},
		{"empty_events", "version: v1\ncases:\n  - name: empty\n    events: []"},
		{"unknown_field", "version: v1\ncases:\n  - name: x\n    events:\n      - kind: ProbeSucceeded\n    expect:\n      must_emit_command: [ProbeReplica]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := schema.ParseCaseSuite([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected parse error for malformed input")
			}
		})
	}
}
