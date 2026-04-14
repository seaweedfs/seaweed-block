// Package calibration implements the Phase 06 calibration pass: a small
// set of scenarios (C1-C5) that drive the accepted V3 route and record
// expected versus observed outcomes. Evidence produced here feeds the
// docs/calibration/scenario-map.md and docs/calibration/divergence-log.md
// artifacts. The package MUST NOT redefine semantics; it only observes
// and compares.
//
// Inherited fixed boundaries (from v3-phase-06-checklist.md §1):
//
//   - all scenarios run through the accepted adapter/transport/storage route
//   - no scenario calls engine.Apply directly
//   - no scenario rewrites frozen targetLSN
//   - same-observation batch ingress at the adapter boundary is preserved
//   - mismatches are recorded as divergences, never hidden by expectation tweaks
package calibration

// Family is the scenario family label, one of C1-C5.
type Family string

const (
	FamilyC1 Family = "C1" // healthy remains healthy
	FamilyC2 Family = "C2" // short gap -> catch_up
	FamilyC3 Family = "C3" // long gap -> rebuild
	FamilyC4 Family = "C4" // terminal truth only from session close
	FamilyC5 Family = "C5" // timing/order perturbation stable
)

// Expected captures what a scenario claims the V3 route should produce.
// Kept narrow on purpose: command-path inclusion/exclusion plus final
// mode is enough for the first calibration pass. Richer expectations
// can be added only when a divergence shows they are missing.
type Expected struct {
	Decision              string   `json:"decision"`
	CommandPathIncludes   []string `json:"command_path_includes,omitempty"`
	CommandPathExcludes   []string `json:"command_path_excludes,omitempty"`
	FinalMode             string   `json:"final_mode"`
	TerminalFromClose     bool     `json:"terminal_from_close"`
	DataIntegrityExpected bool     `json:"data_integrity_expected,omitempty"`
}

// Observed captures what actually happened when the scenario ran.
// These fields are filled by the harness — never by the scenario itself.
// A scenario that edited Observed to make Expected match would be a
// classic hide-the-divergence anti-pattern.
type Observed struct {
	Decision          string   `json:"decision"`
	CommandPath       []string `json:"command_path"`
	CloseEventCount   int      `json:"close_event_count"` // count of "event: SessionClosedCompleted" in trace
	FinalMode         string   `json:"final_mode"`
	TerminalFromClose bool     `json:"terminal_from_close"`
	DataIntegrityOK   bool     `json:"data_integrity_ok"`
}

// Result is one scenario's complete record: metadata, the two snapshots,
// pass/fail, and — if they differ — a machine-readable divergence note.
type Result struct {
	ID         string    `json:"id"`
	Family     Family    `json:"family"`
	Intent     string    `json:"intent"`
	Expected   Expected  `json:"expected"`
	Observed   Observed  `json:"observed"`
	Pass       bool      `json:"pass"`
	Divergence *Mismatch `json:"divergence,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Mismatch enumerates the dimensions on which expected and observed
// diverged. Multiple can be true for one scenario.
type Mismatch struct {
	Decision          bool `json:"decision_diff,omitempty"`
	CommandPath       bool `json:"command_path_diff,omitempty"`
	FinalMode         bool `json:"final_mode_diff,omitempty"`
	TerminalFromClose bool `json:"terminal_from_close_diff,omitempty"`
	DataIntegrity     bool `json:"data_integrity_diff,omitempty"`
}

// Compare derives pass/fail and a Mismatch from Expected vs Observed.
// Pure function — the harness calls this after the scenario runs.
func Compare(exp Expected, obs Observed) (pass bool, m *Mismatch) {
	var d Mismatch
	any := false

	if exp.Decision != "" && exp.Decision != obs.Decision {
		d.Decision = true
		any = true
	}
	if !pathSatisfies(obs.CommandPath, exp.CommandPathIncludes, exp.CommandPathExcludes) {
		d.CommandPath = true
		any = true
	}
	if exp.FinalMode != "" && exp.FinalMode != obs.FinalMode {
		d.FinalMode = true
		any = true
	}
	if exp.TerminalFromClose && !obs.TerminalFromClose {
		d.TerminalFromClose = true
		any = true
	}
	if exp.DataIntegrityExpected && !obs.DataIntegrityOK {
		d.DataIntegrity = true
		any = true
	}

	if !any {
		return true, nil
	}
	return false, &d
}

func pathSatisfies(path, includes, excludes []string) bool {
	have := make(map[string]bool, len(path))
	for _, c := range path {
		have[c] = true
	}
	for _, inc := range includes {
		if !have[inc] {
			return false
		}
	}
	for _, exc := range excludes {
		if have[exc] {
			return false
		}
	}
	return true
}
