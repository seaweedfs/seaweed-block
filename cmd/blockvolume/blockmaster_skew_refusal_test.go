//go:build subprocess

package main_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The operator guide's clock-safety rule, run on the real binary: blockmaster
// refuses to start when half of --workspace-max-clock-skew does not exceed its
// fence-exit budget, and the refusal names the bound to use (above 10.1 s).
func TestBlockmasterRefusesASkewBoundAtOrBelowTheFenceExit(t *testing.T) {
	bins := buildG54Binaries(t)
	art := t.TempDir()
	topo := writeSingleSlotTopology(t, art)
	for _, skew := range []string{"8s", "10.1s"} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		out, err := exec.CommandContext(ctx, bins.master,
			"--authority-store", filepath.Join(art, "master-"+skew),
			"--listen", "127.0.0.1:0", "--topology", topo, "--expected-slots-per-volume", "1",
			"--workspace-max-clock-skew", skew).CombinedOutput()
		timedOut := ctx.Err() != nil
		cancel()
		var exit *exec.ExitError
		if timedOut || !errorsAsExit(err, &exit) || exit.ExitCode() == 0 {
			t.Fatalf("skew %s: blockmaster did not refuse to start (err %v, timed out %v):\n%s", skew, err, timedOut, out)
		}
		for _, want := range []string{"--workspace-max-clock-skew=" + skew, "10.1s"} {
			if !strings.Contains(string(out), want) {
				t.Fatalf("skew %s: the refusal does not name %q:\n%s", skew, want, out)
			}
		}
	}
}

func errorsAsExit(err error, exit **exec.ExitError) bool {
	e, ok := err.(*exec.ExitError)
	if ok {
		*exit = e
	}
	return ok
}
