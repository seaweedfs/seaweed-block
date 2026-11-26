package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
)

const opsFailoverName = "sw-block ops failover"

// failoverRuntime is blockmaster's admin-only FailbackService, the one call
// the operator's failover makes.
type failoverRuntime interface {
	ExecuteFailback(context.Context, ops.FailbackRuntimeRequest) (ops.FailbackRuntimeResult, error)
}

// opsFailoverRuntimeFactory dials blockmaster with the mutual-TLS client
// identity from SW_BLOCK_TLS_CERT / SW_BLOCK_TLS_KEY / SW_BLOCK_TLS_CA.
var opsFailoverRuntimeFactory = func(master string) failoverRuntime {
	return ops.NewGRPCFailbackRuntime(master)
}

const failoverUsage = `usage: sw-block ops failover --master ADDR --volume V --target R
         --target-data A --target-ctrl A --expect-current R@E --confirm-old-node-off V

The operator's failover of volume V's primary to replica R, for a standalone
(systemd) deployment. blockmaster decides: when the current primary is
unreachable it runs the fencing round (every other replica fenced at E+1) and
moves the line only to a ready replica holding the highest fenced end; a
reachable primary's move is a planned move under its guard. --target-data
and --target-ctrl must be blockmaster's record of the replica (its heartbeat);
other addresses are refused. A refusal changes nothing.

The fencing round stops the old primary's writes, not its reads: power the
old node off or cut it from its clients first, then confirm with
--confirm-old-node-off V. --expect-current is the line you saw (sw-block ops
volumes); a line that moved since is refused.

Needs an admin client certificate. Exit: 0 moved; 1 refused or failed; 2
bad arguments.
`

func runOpsFailover(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(opsFailoverName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, failoverUsage) }
	var master, volume, target, data, ctrl, expect, confirm string
	var timeout time.Duration
	fs.StringVar(&master, "master", "", "blockmaster gRPC address host:port")
	fs.StringVar(&volume, "volume", "", "volume id")
	fs.StringVar(&target, "target", "", "replica id to promote")
	fs.StringVar(&data, "target-data", "", "the target replica's data address host:port")
	fs.StringVar(&ctrl, "target-ctrl", "", "the target replica's control address host:port")
	fs.StringVar(&expect, "expect-current", "", "the current line, exactly <replica>@<epoch>")
	fs.StringVar(&confirm, "confirm-old-node-off", "", "exactly the volume id: the old primary's node is off or cut from its clients")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "bound for the call (the fencing round runs inside it)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || master == "" || volume == "" || target == "" || data == "" || ctrl == "" || timeout <= 0 {
		fmt.Fprint(stderr, failoverUsage)
		return 2
	}
	curReplica, curEpoch, ok := parseReplicaAtEpoch(expect)
	if !ok {
		fmt.Fprintf(stderr, "%s: --expect-current %q is not <replica>@<epoch>\n", opsFailoverName, expect)
		return 2
	}
	if curReplica == target {
		fmt.Fprintf(stderr, "%s: the target %s is the current primary\n", opsFailoverName, target)
		return 2
	}
	if confirm != volume {
		fmt.Fprintf(stderr, "%s: --confirm-old-node-off must be exactly %q (the old node is off or cut from its clients)\n", opsFailoverName, volume)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := opsFailoverRuntimeFactory(master).ExecuteFailback(ctx, ops.FailbackRuntimeRequest{
		VolumeID:                 volume,
		ReplicaID:                target,
		TargetDataAddr:           data,
		TargetCtrlAddr:           ctrl,
		ExpectedCurrentReplicaID: curReplica,
		ExpectedCurrentEpoch:     curEpoch,
		// Required by the runtime but decide nothing (they are not evidence):
		// blockmaster's gate (the fencing round for an unreachable primary)
		// decides the move, and the target is published at blockmaster's own
		// record of its addresses (a request naming others is refused).
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true, // --confirm-old-node-off
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"sw-block ops failover", "operator_confirmed_old_node_off=" + volume},
	})
	if err != nil {
		fmt.Fprintf(stdout, "failover volume=%s target=%s expected=%s@%d result=refused\n", volume, target, curReplica, curEpoch)
		fmt.Fprintf(stderr, "%s: %v\n", opsFailoverName, err)
		return 1
	}
	moved := res.FailbackStarted && res.AuthorityEpochAdvanced && res.SinglePrimaryAfterFailback
	fmt.Fprintf(stdout, "failover volume=%s target=%s expected=%s@%d result=%s epoch_advanced=%t single_primary=%t publish_target_swapped=%t\n",
		volume, target, curReplica, curEpoch, map[bool]string{true: "moved", false: "not_moved"}[moved],
		res.AuthorityEpochAdvanced, res.SinglePrimaryAfterFailback, res.PublishTargetSwappedAfterFailback)
	if !moved {
		return 1
	}
	return 0
}

// parseReplicaAtEpoch parses "<replica>@<epoch>" with a nonzero epoch.
func parseReplicaAtEpoch(s string) (string, uint64, bool) {
	i := strings.LastIndexByte(s, '@')
	if i <= 0 || i == len(s)-1 {
		return "", 0, false
	}
	e, err := strconv.ParseUint(s[i+1:], 10, 64)
	if err != nil || e == 0 {
		return "", 0, false
	}
	return s[:i], e, true
}
