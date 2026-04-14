package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweed-block/core/calibration"
)

// runCalibration executes the Phase 06 first calibration pass and
// emits the Report. Text mode prints a per-scenario PASS/FAIL line
// plus the summary; --json emits the full machine-readable artifact
// suitable for piping into docs/calibration/ evidence review.
//
// Exit codes follow the sparrow contract:
//   0  all scenarios passed
//   1  one or more scenarios diverged or errored
//
// This is deliberately an addition to the sparrow binary rather than a
// new CLI — per v3-operations-design.md §1.3 and
// v3-phase-06-checklist.md §4 S1, Phase 06 reuses the accepted Phase 05
// bootstrap entry path.
func runCalibration(opts options) int {
	report := calibration.Runner{}.Run(calibration.AllScenarios())

	if opts.json {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	} else {
		fmt.Println("=== V3 Phase 06 Calibration ===")
		fmt.Println()
		for _, r := range report.Results {
			status := "PASS"
			if !r.Pass {
				status = "FAIL"
			}
			fmt.Printf("  [%s] %-28s %s — %s\n", r.Family, r.ID, status, r.Intent)
			if !r.Pass {
				printDivergenceText(r)
			}
		}
		fmt.Println()
		fmt.Printf("=== Summary: %d/%d passed, %d diverged, %d errored ===\n",
			report.Summary.Passed, report.Summary.Total,
			report.Summary.Diverged, report.Summary.Errors)
		if !report.Summary.AllPassed {
			fmt.Println()
			fmt.Println("Record each divergence in docs/calibration/divergence-log.md")
			fmt.Println("before changing the route or the expectations.")
		}
	}

	if report.Summary.AllPassed {
		return 0
	}
	return 1
}

func printDivergenceText(r calibration.Result) {
	if r.Error != "" {
		fmt.Printf("      error: %s\n", r.Error)
		return
	}
	fmt.Printf("      expected: decision=%s final_mode=%s includes=%v excludes=%v\n",
		r.Expected.Decision, r.Expected.FinalMode,
		r.Expected.CommandPathIncludes, r.Expected.CommandPathExcludes)
	fmt.Printf("      observed: decision=%s final_mode=%s command_path=%v\n",
		r.Observed.Decision, r.Observed.FinalMode, r.Observed.CommandPath)
	if r.Divergence != nil {
		fmt.Printf("      divergence: %+v\n", *r.Divergence)
	}
}
