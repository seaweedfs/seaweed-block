package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A revocation ACK whose first answer is lost to a busy master (its call
// timed out, or the master was unavailable) is asked again, and the repeat's
// answer completes the stop with status 0, not 75. Any other first answer
// is final: one ACK, exit 75.
func TestWorkspaceRevocationAckIsAskedAgainAfterALostAnswer(t *testing.T) {
	for _, c := range []struct {
		name  string
		first error
		acks  int32
		code  int
	}{
		{"deadline exceeded once", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), 2, 0},
		{"unavailable once", status.Error(codes.Unavailable, "workspace ACK: read detached HEAD: busy"), 2, 0},
		{"refused (final)", status.Error(codes.FailedPrecondition, "workspace revoke ACK: stale or unsolicited"), 1, 75},
	} {
		t.Run(c.name, func(t *testing.T) {
			var stream *exitAssignmentStream
			h, send := exitAuthorityHost(t, func(s *exitAssignmentStream) {
				stream = s
				s.revoke = true
				s.ackFirstError = c.first
			})
			send("r", 1)
			ctx, cancel := context.WithCancel(context.Background())
			doneConsumed := false
			requests, done := startWorkspaceLease(ctx, h)
			t.Cleanup(func() {
				cancel()
				if !doneConsumed {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("lease worker did not join")
					}
				}
			})
			e := newProcessExit(ctx)
			f := flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v", replicaID: "r"}
			request, leaseDone := waitWorkspaceStop(e, f, h, requests, done)
			if request == nil || !h.WorkspaceLeaseRevoking() {
				t.Fatal("real planned drain not acquired")
			}
			me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
			st := layoutProofStore{h: layout.Head{Gen: 1, Epoch: 1, Holder: "s/r", ClaimID: "k", State: layout.StateDetached, Activity: layout.Activity{Kind: layout.ActIdle}}, found: true}
			gen, proofErr := workspaceLayoutDetachedGeneration(request.ctx, st, request.id, me)
			if proofErr != nil {
				t.Fatal(proofErr)
			}
			request.reply <- workspaceStopResult{gen: gen}
			finishWorkspaceLease(e, f, h, leaseDone)
			doneConsumed = true
			out := <-e.decision
			if got := stream.acks.Load(); got != c.acks {
				t.Fatalf("%d ACK calls; want %d", got, c.acks)
			}
			if out.code != c.code {
				t.Fatalf("stop classified %+v; want status %d", out, c.code)
			}
		})
	}
}

// The closeout (with its repeated ACK attempts) fits the shipped units' stop
// budget: the drain before it and superviseProcess's exit wait after it
// (each volume.WorkspaceDrainTimeout) plus the closeout stay under every
// blockvolume unit's TimeoutStopSec, and the closeout leaves room for at
// least two ACK attempts.
func TestWorkspaceCloseoutFitsTheUnitsStopBudget(t *testing.T) {
	if workspaceLeaseCloseoutTimeout < 2*volume.WorkspaceAckTimeout {
		t.Fatalf("closeout %v leaves no room for a second %v ACK attempt", workspaceLeaseCloseoutTimeout, volume.WorkspaceAckTimeout)
	}
	units, err := filepath.Glob(filepath.Join("..", "..", "deploy", "systemd", "sw-blockvolume*.service"))
	if err != nil || len(units) == 0 {
		t.Fatalf("no blockvolume units found: %v", err)
	}
	for _, unit := range units {
		raw, err := os.ReadFile(unit)
		if err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile(`(?m)^TimeoutStopSec=(\d+)s\s*$`).FindSubmatch(raw)
		if m == nil {
			t.Fatalf("%s: no TimeoutStopSec=<n>s", unit)
		}
		n, _ := strconv.Atoi(string(m[1]))
		stop := time.Duration(n) * time.Second
		if total := volume.WorkspaceDrainTimeout + workspaceLeaseCloseoutTimeout + volume.WorkspaceDrainTimeout; total >= stop {
			t.Fatalf("%s: drain %v + closeout %v + exit wait %v = %v, not under TimeoutStopSec %v", unit, volume.WorkspaceDrainTimeout, workspaceLeaseCloseoutTimeout, volume.WorkspaceDrainTimeout, total, stop)
		}
	}
}
