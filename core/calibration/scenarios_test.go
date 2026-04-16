package calibration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// TestAllScenarios_PassEndToEnd is the headline Phase 06 test: every
// scenario in the first calibration set runs green through the accepted
// V3 route. If this fails, a real calibration divergence has been found
// and belongs in docs/calibration/divergence-log.md — not in a test
// loosening.
func TestAllScenarios_PassEndToEnd(t *testing.T) {
	report := Runner{}.Run(AllScenarios())

	// Emit the JSON report to t.Logf so CI output carries the evidence.
	buf, _ := json.MarshalIndent(report, "", "  ")
	t.Logf("Phase 06 calibration report:\n%s", buf)

	if !report.Summary.AllPassed {
		t.Fatalf("calibration failed: %d/%d passed, %d diverged, %d errored",
			report.Summary.Passed, report.Summary.Total,
			report.Summary.Diverged, report.Summary.Errors)
	}
}

// TestC1_HealthyNoRecovery asserts the C1 claim in isolation so a
// failure points directly at family C1 without searching through the
// joint report.
func TestC1_HealthyNoRecovery(t *testing.T) {
	obs, err := (&scenarioC1Healthy{}).Run()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range obs.CommandPath {
		if c == "StartCatchUp" || c == "StartRebuild" {
			t.Fatalf("C1: expected no recovery commands, got %v", obs.CommandPath)
		}
	}
	if obs.FinalMode != "healthy" {
		t.Fatalf("C1: final mode %q, want healthy", obs.FinalMode)
	}
}

func TestC2_ShortGapCatchUp(t *testing.T) {
	obs, err := (&scenarioC2ShortGap{}).Run()
	if err != nil {
		t.Fatal(err)
	}
	sawCatchUp := false
	for _, c := range obs.CommandPath {
		if c == "StartRebuild" {
			t.Fatalf("C2: rebuild must not fire for short gap, got %v", obs.CommandPath)
		}
		if c == "StartCatchUp" {
			sawCatchUp = true
		}
	}
	if !sawCatchUp {
		t.Fatalf("C2: StartCatchUp missing from %v", obs.CommandPath)
	}
	if obs.FinalMode != "healthy" {
		t.Fatalf("C2: final mode %q, want healthy", obs.FinalMode)
	}
	if !obs.DataIntegrityOK {
		t.Fatal("C2: data integrity check failed")
	}
}

func TestC3_LongGapRebuild(t *testing.T) {
	obs, err := (&scenarioC3LongGap{}).Run()
	if err != nil {
		t.Fatal(err)
	}
	sawRebuild := false
	for _, c := range obs.CommandPath {
		if c == "StartCatchUp" {
			t.Fatalf("C3: catch-up must not fire for long gap, got %v", obs.CommandPath)
		}
		if c == "StartRebuild" {
			sawRebuild = true
		}
	}
	if !sawRebuild {
		t.Fatalf("C3: StartRebuild missing from %v", obs.CommandPath)
	}
	if obs.FinalMode != "healthy" {
		t.Fatalf("C3: final mode %q, want healthy", obs.FinalMode)
	}
	if !obs.DataIntegrityOK {
		t.Fatal("C3: data integrity check failed")
	}
}

func TestC4_TerminalOnlyFromSessionClose(t *testing.T) {
	obs, err := (&scenarioC4TerminalClose{}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !obs.TerminalFromClose {
		t.Fatalf("C4: terminal truth not attributable to session close; "+
			"command path=%v final=%q", obs.CommandPath, obs.FinalMode)
	}
	if obs.FinalMode != "healthy" {
		t.Fatalf("C4: final mode %q, want healthy", obs.FinalMode)
	}
}

func TestC5_OrderStableAcrossRuns(t *testing.T) {
	obs, err := (&scenarioC5OrderStable{}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if obs.Decision == "DIVERGED_ACROSS_RUNS" {
		t.Fatal("C5: decision varied across perturbed runs — A1/A4 leak")
	}
	if obs.FinalMode == "DIVERGED_ACROSS_RUNS" {
		t.Fatal("C5: final mode varied across perturbed runs — A1/A4 leak")
	}
	// Recovery-signature divergence is signaled by a sentinel element
	// prepended to CommandPath. The scenario run must not hit this.
	if len(obs.CommandPath) > 0 && obs.CommandPath[0] == "DIVERGED_ACROSS_RUNS_RECOVERY_SIG" {
		t.Fatalf("C5: recovery signature drifted across runs; first-run path=%v", obs.CommandPath[1:])
	}
	// CloseEventCount sentinel: negative is used to flag cross-run
	// drift in the number of SessionClosedCompleted events. A legit
	// count is always >= 0.
	if obs.CloseEventCount < 0 {
		t.Fatalf("C5: close-event count drifted across runs (sentinel %d)", obs.CloseEventCount)
	}
	if obs.Decision != "catch_up" {
		t.Fatalf("C5: expected catch_up, got %q", obs.Decision)
	}
	// For the short-gap catch-up scenario, exactly one close event
	// per run is the expected count. Lock this in so a future
	// refactor that silently changes the close shape is caught.
	if obs.CloseEventCount != 1 {
		t.Fatalf("C5: expected CloseEventCount=1 (one catch-up session per run), got %d",
			obs.CloseEventCount)
	}
}

// TestC5_RecoverySignatureCheckActuallyWorks is a meta-test for the
// cross-run compare logic. We construct two divergent paths (one with
// extra StartRebuild) and verify extractRecoverySignature catches it.
// This guards against a future refactor silently dropping the compare.
func TestC5_RecoverySignatureCheckActuallyWorks(t *testing.T) {
	pathA := []string{"ProbeReplica", "StartCatchUp", "PublishHealthy"}
	pathB := []string{"ProbeReplica", "StartCatchUp", "StartRebuild", "PublishHealthy"}
	pathDupCatchUp := []string{"ProbeReplica", "StartCatchUp", "StartCatchUp", "PublishHealthy"}

	sigA := extractRecoverySignature(pathA)
	sigB := extractRecoverySignature(pathB)
	sigDup := extractRecoverySignature(pathDupCatchUp)

	if sigA == sigB {
		t.Fatal("extractRecoverySignature: A and B should differ (B has extra StartRebuild)")
	}
	if sigA == sigDup {
		t.Fatal("extractRecoverySignature: A and Dup should differ (Dup has extra StartCatchUp)")
	}
	if sigA.NCatchUp != 1 || sigA.NRebuild != 0 {
		t.Fatalf("sigA unexpected: %+v", sigA)
	}
	if sigDup.NCatchUp != 2 {
		t.Fatalf("sigDup should count both catch-ups: %+v", sigDup)
	}
}

func TestCalibration_Handoff_StaleProbeFailureIgnoredAfterEpochAdvance(t *testing.T) {
	h, err := newHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()

	h.writeBlocks(8)
	h.mirrorToReplica()
	h.assign(1, 1)
	if p := h.waitForDecisionFinal(3 * time.Second); p.Mode != engine.ModeHealthy {
		t.Fatalf("precondition: epoch1 should converge healthy, got %s", p.Mode)
	}

	h.assign(2, 2)
	if p := h.waitForDecisionFinal(3 * time.Second); p.Mode != engine.ModeHealthy {
		t.Fatalf("precondition: epoch2 should converge healthy, got %s", p.Mode)
	}

	h.adapter.OnProbeResult(adapter.ProbeResult{
		ReplicaID:       "r1",
		Success:         false,
		EndpointVersion: 1,
		TransportEpoch:  1,
		FailReason:      "stale_old_primary",
	})
	time.Sleep(50 * time.Millisecond)

	if got := h.adapter.Projection().Mode; got != engine.ModeHealthy {
		t.Fatalf("stale old-primary probe failure should be ignored, got mode %s", got)
	}
}

func TestCompare_PassWhenEverythingMatches(t *testing.T) {
	exp := Expected{
		Decision:            "catch_up",
		CommandPathIncludes: []string{"StartCatchUp"},
		CommandPathExcludes: []string{"StartRebuild"},
		FinalMode:           "healthy",
	}
	obs := Observed{
		Decision:    "catch_up",
		CommandPath: []string{"Probe", "StartCatchUp", "PublishHealthy"},
		FinalMode:   "healthy",
	}
	pass, m := Compare(exp, obs)
	if !pass {
		t.Fatalf("expected pass, got mismatch %+v", m)
	}
	if m != nil {
		t.Fatalf("expected nil mismatch on pass, got %+v", m)
	}
}

func TestCompare_FailsOnEachMismatchDimension(t *testing.T) {
	base := Expected{
		Decision:              "catch_up",
		CommandPathIncludes:   []string{"StartCatchUp"},
		CommandPathExcludes:   []string{"StartRebuild"},
		FinalMode:             "healthy",
		TerminalFromClose:     true,
		DataIntegrityExpected: true,
	}
	cases := []struct {
		name    string
		obs     Observed
		fieldOK func(*Mismatch) bool
	}{
		{
			"decision", Observed{Decision: "rebuild", CommandPath: []string{"StartCatchUp"}, FinalMode: "healthy", TerminalFromClose: true, DataIntegrityOK: true},
			func(m *Mismatch) bool { return m.Decision },
		},
		{
			"command_path_missing_include", Observed{Decision: "catch_up", CommandPath: []string{"Probe"}, FinalMode: "healthy", TerminalFromClose: true, DataIntegrityOK: true},
			func(m *Mismatch) bool { return m.CommandPath },
		},
		{
			"command_path_has_excluded", Observed{Decision: "catch_up", CommandPath: []string{"StartCatchUp", "StartRebuild"}, FinalMode: "healthy", TerminalFromClose: true, DataIntegrityOK: true},
			func(m *Mismatch) bool { return m.CommandPath },
		},
		{
			"final_mode", Observed{Decision: "catch_up", CommandPath: []string{"StartCatchUp"}, FinalMode: "degraded", TerminalFromClose: true, DataIntegrityOK: true},
			func(m *Mismatch) bool { return m.FinalMode },
		},
		{
			"terminal_from_close", Observed{Decision: "catch_up", CommandPath: []string{"StartCatchUp"}, FinalMode: "healthy", TerminalFromClose: false, DataIntegrityOK: true},
			func(m *Mismatch) bool { return m.TerminalFromClose },
		},
		{
			"data_integrity", Observed{Decision: "catch_up", CommandPath: []string{"StartCatchUp"}, FinalMode: "healthy", TerminalFromClose: true, DataIntegrityOK: false},
			func(m *Mismatch) bool { return m.DataIntegrity },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pass, m := Compare(base, c.obs)
			if pass {
				t.Fatalf("expected fail, got pass")
			}
			if m == nil || !c.fieldOK(m) {
				t.Fatalf("expected %s flag set in mismatch, got %+v", c.name, m)
			}
		})
	}
}
