package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// runS7RestartSmoke is the narrow test-only entry that drives the
// real-subprocess restart smoke. It is NOT advertised in --help and
// has no operator workflow; it exists only so the L2 process test
// can spawn a real `sparrow` binary twice against the same store
// directory and assert on structured stdout.
//
// Contract:
//
//  1. Bootstrap against --authority-store (acquire lock, open
//     file store, reload Publisher).
//  2. If the store is empty after reload (ReloadedRecords == 0),
//     this is the FIRST run: mint one Bind through the
//     Publisher so there is durable state for the next run to
//     observe. If the store already has records, this is the
//     SECOND run: report reloaded state without minting.
//  3. Emit exactly ONE structured JSON line to stdout matching
//     the s7RestartSmokeReport schema below, then exit 0.
//  4. On any error path, emit the structured line with Pass=""
//     and a non-empty Error, exit non-zero.
//
// "No backward mint" is enforced by construction: the smoke
// only mints on the first run (when the store is empty), so
// the second run cannot produce a lower Epoch than the first.
// The test still asserts backwardMint=false in the stdout line
// so a future bug that accidentally re-minted on reload would
// fail the test loudly.
func runS7RestartSmoke(opts options) int {
	if opts.authorityStore == "" {
		emitSmokeErr("s7-restart-smoke requires --authority-store <dir>", nil, false)
		return 2
	}

	directive := authority.NewStaticDirective(nil)
	boot, err := Bootstrap(opts.authorityStore, directive)
	if err != nil {
		emitSmokeErr(err.Error(), nil, false)
		return 1
	}
	defer func() {
		if cerr := boot.Close(); cerr != nil {
			log.Printf("sparrow: close durable bootstrap: %v", cerr)
		}
	}()

	var runLabel string
	var reportedLine *s7SmokeLine
	backwardMint := false

	if boot.ReloadedRecords == 0 {
		// First run: mint one Bind through the real Publisher +
		// durable store path so the next run has something to
		// reload. We drive the mint via the static directive so
		// the full publisher.Run loop (not a synthetic apply) is
		// what writes to the store.
		runLabel = "first"

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() { _ = boot.Publisher.Run(ctx) }()

		directive.Append(authority.AssignmentAsk{
			VolumeID:  "s7-smoke-vol",
			ReplicaID: "s7-smoke-r1",
			DataAddr:  "data-s7-smoke",
			CtrlAddr:  "ctrl-s7-smoke",
			Intent:    authority.IntentBind,
		})

		// Wait for the mint to land durably. We poll
		// VolumeAuthorityLine because the publisher's internal
		// apply is asynchronous relative to directive.Append.
		deadline := time.Now().Add(2 * time.Second)
		var line authority.AuthorityBasis
		ok := false
		for time.Now().Before(deadline) {
			if line, ok = boot.Publisher.VolumeAuthorityLine("s7-smoke-vol"); ok && line.Assigned {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !ok || !line.Assigned {
			emitSmokeErr("first run: mint did not reach publisher within deadline", nil, false)
			return 1
		}
		reportedLine = &s7SmokeLine{
			VolumeID:        "s7-smoke-vol",
			ReplicaID:       line.ReplicaID,
			Epoch:           line.Epoch,
			EndpointVersion: line.EndpointVersion,
		}
	} else {
		// Second (or subsequent) run: record the reloaded line.
		// No mint — the publisher's directive is empty; Run is
		// not even needed for the observation we return.
		runLabel = "second"
		if line, ok := boot.Publisher.VolumeAuthorityLine("s7-smoke-vol"); ok && line.Assigned {
			reportedLine = &s7SmokeLine{
				VolumeID:        "s7-smoke-vol",
				ReplicaID:       line.ReplicaID,
				Epoch:           line.Epoch,
				EndpointVersion: line.EndpointVersion,
			}
			// Epoch==1 is what the first run minted via IntentBind.
			// Anything < 1 or a missing line on the reload side
			// would indicate a reload regression.
			if line.Epoch < 1 {
				backwardMint = true
			}
		} else {
			emitSmokeErr("second run: reloaded line missing for s7-smoke-vol", nil, false)
			return 1
		}
	}

	emitSmokeOK(runLabel, boot.ReloadedRecords, len(boot.ReloadSkips), reportedLine, backwardMint)
	return 0
}

// s7RestartSmokeReport is the exact JSON schema the smoke
// emits on stdout. Pinned by sketch §8.4.
type s7RestartSmokeReport struct {
	Pass         string       `json:"pass"`
	Run          string       `json:"run"`
	Reloaded     int          `json:"reloaded"`
	Skips        int          `json:"skips"`
	Line         *s7SmokeLine `json:"line"`
	BackwardMint bool         `json:"backwardMint"`
	Error        string       `json:"error,omitempty"`
}

type s7SmokeLine struct {
	VolumeID        string `json:"vid"`
	ReplicaID       string `json:"rid"`
	Epoch           uint64 `json:"epoch"`
	EndpointVersion uint64 `json:"endpointVersion"`
}

func emitSmokeOK(run string, reloaded, skips int, line *s7SmokeLine, backwardMint bool) {
	r := s7RestartSmokeReport{
		Pass:         "s7-restart-smoke",
		Run:          run,
		Reloaded:     reloaded,
		Skips:        skips,
		Line:         line,
		BackwardMint: backwardMint,
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
}

func emitSmokeErr(msg string, line *s7SmokeLine, backwardMint bool) {
	r := s7RestartSmokeReport{
		Pass:         "",
		Run:          "",
		Line:         line,
		BackwardMint: backwardMint,
		Error:        msg,
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
}
