package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/wsclient"
)

// M2 Acked -> Moved (clean), INV-WS-CLEAN-MOVE-001: encode only; do not
// synthesize a seal/ACK, release a lease or start the target here.
func runWorkspaceCleanMove(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops workspace move", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var q control.WorkspaceMove
	var master string
	var clean bool
	var timeout time.Duration
	fs.StringVar(&master, "master", "", "blockmaster gRPC address host:port")
	fs.StringVar(&q.VolumeId, "volume", "", "volume id")
	fs.StringVar(&q.ExpectedReplicaId, "replica", "", "current logical replica id")
	fs.Uint64Var(&q.ExpectedEpoch, "epoch", 0, "current holder epoch")
	fs.StringVar(&q.RequestId, "request-id", "", "operator-supplied move id; reuse on unknown outcome")
	fs.StringVar(&q.TargetServerId, "target-host", "", "target node; may be the source host once sealed (requires server support)")
	fs.StringVar(&q.TargetDataAddr, "target-data", "", "target data address host:port")
	fs.StringVar(&q.TargetCtrlAddr, "target-ctrl", "", "target control address host:port")
	fs.BoolVar(&clean, "clean", false, "required: move using the detached HEAD and matching recorded host ACK")
	fs.DurationVar(&timeout, "timeout", wsDefaultTimeout, "bound for the RPC")
	if err := fs.Parse(args); err != nil {
		return wsExitRefusedLocally
	}
	if fs.NArg() != 0 || !clean || master == "" || q.VolumeId == "" || q.ExpectedReplicaId == "" || q.ExpectedEpoch == 0 || q.RequestId == "" || q.TargetServerId == "" || q.TargetDataAddr == "" || q.TargetCtrlAddr == "" || timeout <= 0 {
		fmt.Fprintln(stderr, "workspace move: refused locally; --clean, --master, --volume, --replica, positive --epoch/--timeout, --request-id and target host/data/ctrl are required")
		return wsExitRefusedLocally
	}
	q.Kind, q.TargetReplicaId = "clean", q.ExpectedReplicaId
	c, err := workspaceDial(master)
	if err != nil {
		fmt.Fprintf(stderr, "workspace move: refused locally, nothing sent: %v\n", err)
		return wsExitRefusedLocally
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	g, err := c.CleanMove(ctx, &q)
	if err != nil {
		fmt.Fprintf(stderr, "workspace move: %v\n", err)
		if wsclient.OutcomeOf(err) == wsclient.OutcomeUnknown {
			fmt.Fprintf(stderr, "workspace move: outcome unknown; rerun with the SAME --request-id %s\n", q.RequestId)
		}
		return workspaceExit(wsclient.OutcomeOf(err))
	}
	fmt.Fprintf(stdout, "clean move issued: volume %s request %s: %s@%d -> %s@%d on %s (sealed HEAD gen %d, remote LSN %d)\n", g.GetVolumeId(), g.GetRequestId(), g.GetOldReplicaId(), g.GetOldEpoch(), g.GetNewReplicaId(), g.GetNewEpoch(), g.GetNewHost(), g.GetHeadGen(), g.GetRemoteLsn())
	fmt.Fprintf(stdout, "next: start volume %s on %s (this command moved authority only)\n", g.GetVolumeId(), g.GetNewHost())
	return wsExitOK
}
