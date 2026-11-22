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

// M2 Granted -> Revoking, INV-WS-CLEAN-MOVE-001. The seal choice is an
// operator statement only; no HEAD lookup or ACK is synthesized here.
func runWorkspaceRevoke(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops workspace revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var id control.WorkspaceIdentity
	var master string
	var sealedGen uint64
	var withoutSeal bool
	var timeout time.Duration
	fs.StringVar(&master, "master", "", "blockmaster gRPC address host:port")
	fs.StringVar(&id.VolumeId, "volume", "", "volume id")
	fs.StringVar(&id.ReplicaId, "replica", "", "current logical replica id")
	fs.Uint64Var(&id.Epoch, "epoch", 0, "current holder epoch")
	fs.Uint64Var(&sealedGen, "sealed-gen", 0, "HEAD generation printed by detach --plain; operator assertion, not verified here")
	fs.BoolVar(&withoutSeal, "without-seal", false, "explicitly abandon the unpublished tail; a clean move may not be possible")
	fs.DurationVar(&timeout, "timeout", wsDefaultTimeout, "bound for the RPC")
	if err := fs.Parse(args); err != nil {
		return wsExitRefusedLocally
	}
	if fs.NArg() != 0 || master == "" || id.VolumeId == "" || id.ReplicaId == "" || id.Epoch == 0 || timeout <= 0 || (sealedGen > 0) == withoutSeal {
		fmt.Fprintln(stderr, "workspace revoke: refused locally; require --master, --volume, --replica, positive --epoch/--timeout and exactly one of positive --sealed-gen or --without-seal")
		return wsExitRefusedLocally
	}
	c, err := workspaceDial(master)
	if err != nil {
		fmt.Fprintf(stderr, "workspace revoke: refused locally, nothing sent: %v\n", err)
		return wsExitRefusedLocally
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	lease, err := c.Revoke(ctx, &id)
	if err != nil {
		fmt.Fprintf(stderr, "workspace revoke: %v\n", err)
		if wsclient.OutcomeOf(err) == wsclient.OutcomeUnknown {
			fmt.Fprintln(stderr, "workspace revoke: outcome unknown; inspect workspace show before proceeding")
		}
		return workspaceExit(wsclient.OutcomeOf(err))
	}
	fmt.Fprintf(stdout, "revocation requested: volume %s holder %s@%d expiry_ns %d\n", id.VolumeId, id.ReplicaId, id.Epoch, lease.GetExpiresAtNs())
	if withoutSeal {
		fmt.Fprintln(stdout, "operator statement: abandoning unpublished tail (--without-seal)")
	} else {
		fmt.Fprintf(stdout, "operator-stated sealed HEAD generation: %d (not verified); workspace show must report ACK at this generation before move --clean\n", sealedGen)
	}
	return wsExitOK
}
