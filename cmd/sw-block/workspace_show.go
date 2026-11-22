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

// Read-only M2 Granted -> Revoking -> Acked -> Moved (clean) facts,
// INV-WS-CLEAN-MOVE-001. Do not synthesize an ACK or resolve a physical host.
func runWorkspaceShow(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops workspace show", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var master, volume string
	var timeout time.Duration
	fs.StringVar(&master, "master", "", "blockmaster gRPC address host:port")
	fs.StringVar(&volume, "volume", "", "volume id")
	fs.DurationVar(&timeout, "timeout", wsDefaultTimeout, "bound for the read-only RPC")
	if err := fs.Parse(args); err != nil {
		return wsExitRefusedLocally
	}
	if fs.NArg() != 0 || master == "" || volume == "" || timeout <= 0 {
		fmt.Fprintln(stderr, "workspace show: --master, --volume and positive --timeout required; nothing sent")
		return wsExitRefusedLocally
	}
	c, err := workspaceDial(master)
	if err != nil {
		fmt.Fprintf(stderr, "workspace show: refused locally, nothing sent: %v\n", err)
		return wsExitRefusedLocally
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	view, err := c.Show(ctx, volume)
	if err != nil {
		fmt.Fprintf(stderr, "workspace show: %v\n", err)
		return workspaceExit(wsclient.OutcomeOf(err))
	}
	printWorkspaceView(stdout, volume, view)
	return wsExitOK
}

func printWorkspaceView(out io.Writer, volume string, view *control.WorkspaceStatus) {
	fmt.Fprintf(out, "volume: %s (query snapshot, not authority to serve)\n", volume)
	lease := view.GetLease()
	if lease == nil {
		fmt.Fprintln(out, "holder: none\nlease state: none")
	} else {
		state := "granted"
		if lease.GetValidForNs() <= 0 {
			state = "expired"
		} else if lease.GetRevoking() {
			state = "revoking"
		}
		fmt.Fprintf(out, "holder: %s@%d (logical replica; server not provided by lease view)\nlease state: %s\nlease expiry_ns: %d; host validity_ns at query: %d\n", lease.GetReplicaId(), lease.GetEpoch(), state, lease.GetExpiresAtNs(), lease.GetValidForNs())
	}
	ack := view.GetRevocationAck()
	if ack == nil {
		fmt.Fprintln(out, "recorded ACK: none")
	} else {
		id := ack.GetIdentity()
		fmt.Fprintf(out, "recorded ACK: %s@%d HEAD generation %d\n", id.GetReplicaId(), id.GetEpoch(), ack.GetHeadGen())
	}
	for _, g := range view.GetMoves() {
		fmt.Fprintf(out, "move: request=%s kind=%s issued=%t %s/%s@%d -> %s/%s@%d HEAD generation %d remote LSN %d\n", g.GetRequestId(), g.GetKind(), g.GetIssued(), g.GetOldHost(), g.GetOldReplicaId(), g.GetOldEpoch(), g.GetNewHost(), g.GetNewReplicaId(), g.GetNewEpoch(), g.GetHeadGen(), g.GetRemoteLsn())
	}
	for _, b := range view.GetBindings() {
		fmt.Fprintf(out, "binding: host=%s data=%s ctrl=%s retired=%t\n", b.GetServerId(), b.GetDataAddr(), b.GetCtrlAddr(), b.GetRetired())
	}
}
