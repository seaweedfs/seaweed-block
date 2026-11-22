package main

import (
	"context"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"testing"
	"time"
)

func TestWorkspaceRetryNeedsPositiveOtherReplica(t *testing.T) {
	for _, tc := range []struct {
		name string
		fact *control.AssignmentFact
		code int
	}{
		{"absent", nil, 75},
		{"same replica higher epoch", &control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 99}, 75},
		{"another volume", &control.AssignmentFact{VolumeId: "other", ReplicaId: "other"}, 75},
		{"empty holder", &control.AssignmentFact{VolumeId: "v"}, 75},
		{"other replica", &control.AssignmentFact{VolumeId: "v", ReplicaId: "other"}, 78},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := workspaceRetryOutcome("v", "r", "assignment_timeout", tc.fact)
			if out.code != tc.code {
				t.Fatalf("outcome=%+v want %d", out, tc.code)
			}
		})
	}
}

func TestWorkspaceExitFirstOutcomeIsLatched(t *testing.T) {
	for _, first := range []workspaceExit{{75, "lease_expired"}, {78, "superseded"}, {0, "revoked"}} {
		e := newProcessExit(context.Background())
		e.choose(first)
		e.choose(workspaceExit{75, "assignment_timeout"})
		<-e.done
		if e.outcome != first {
			t.Fatalf("first=%+v changed to %+v", first, e.outcome)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := newProcessExit(ctx)
	e.choose(workspaceExit{78, "superseded"})
	if e.outcome.code != 0 || e.outcome.reason != "signal" {
		t.Fatalf("observed signal lost: %+v", e.outcome)
	}
}

func TestWorkspaceExitDoesNotWaitForBlockedFrontendClose(t *testing.T) {
	finished := make(chan int) // a frontend Close that never returns
	deadline := make(chan time.Time, 1)
	deadline <- time.Time{}
	if code := finishWorkspaceExit(workspaceExit{75, "lease_expired"}, finished, deadline); code != 75 {
		t.Fatalf("code=%d", code)
	}
	// Cleanup errors do not overwrite an already selected retry/no-retry status.
	done := make(chan int, 1)
	done <- 1
	if code := finishWorkspaceExit(workspaceExit{78, "superseded"}, done, make(chan time.Time)); code != 78 {
		t.Fatalf("code=%d", code)
	}
}

func TestWorkspacePlannedStopKeepsSuccess(t *testing.T) {
	code := superviseProcess(context.Background(), func(e *processExit) int {
		e.choose(workspaceExit{reason: "revoked"})
		return 0 // the close/ACK owner completed normally
	})
	if code != 0 {
		t.Fatalf("planned stop would restart: %d", code)
	}
}
