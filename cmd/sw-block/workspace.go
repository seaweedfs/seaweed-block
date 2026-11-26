package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/wsclient"
)

// Exit codes of `sw-block ops workspace`.
const (
	wsExitOK               = 0
	wsExitRefused          = 1 // the master refused; nothing moved
	wsExitRefusedLocally   = 2 // bad usage or confirmation; no RPC was sent
	wsExitUnknown          = 3 // outcome unknown: rerun with the SAME --request-id
	wsExitReadoutUnproven  = 4 // move issued (or ranges exist) but the lost range is not proven
	wsExitNotAuthorized    = 5 // blockmaster did not accept this caller; nothing was done
	wsDefaultTimeout       = 30 * time.Second
	workspaceForceMoveName = "sw-block ops workspace force-move"
	workspaceLostRangeName = "sw-block ops workspace lost-range"
)

const workspaceUsage = `usage:
  sw-block ops workspace planned-move --master ADDR --volume V --replica R --epoch E
      --source-host S --source-status URL --target-host T --target-status URL
      --request-id ID [--claim-id C] [--timeout 5m]
  sw-block ops workspace show --master ADDR --volume V [--timeout 30s]
  sw-block ops workspace revoke --master ADDR --volume V --replica R --epoch E
      (--sealed-gen G | --without-seal) [--timeout 30s]
  sw-block ops workspace move --clean --master ADDR --volume V --replica R --epoch E
      --target-host H --target-data A --target-ctrl A --request-id ID [--timeout 30s]
  sw-block ops workspace force-move --master ADDR --volume V --target-host H --target-data A --target-ctrl A
      --request-id ID --confirm-forced V@EPOCH [--dry-run] [--timeout 30s]
  sw-block ops workspace lost-range --master ADDR --volume V [--request-id ID] [--timeout 30s]

planned-move drives seal, revoke, recorded ACK and clean move for RF1 workspaces.
Quiesce the guest first and start the target unit waiting for its grant. A missing
live target binding is refused before any mutation. COMPLETE requires the target's
own status at the new epoch; a move receipt alone is not completion. Exit 6 means
MOVED-TARGET-NOT-SERVING: keep the target unit running and rerun the SAME id.
Guest reconnect is an operator step after COMPLETE; no local journal is written.

force-move FORCES an RF1 workspace volume's authority off a holder that is gone,
abandoning what it wrote after the last published HEAD. It moves AUTHORITY only:
starting the volume on the target host is the next step, done separately. The
blockmaster admits it only once the holder's lease horizon and its granted or
restart deadline, each plus the clock-skew bound, have passed; before that it is
refused and this command says when it becomes admissible. There is NO bypass
flag.

Planned hand-over order: stop the writer; ops chunk detach --plain; revoke
--sealed-gen G; show until the recorded ACK names the same replica, epoch and
HEAD generation G; move --clean; start blockvolume on the target separately.
Revoke only requests revocation: it does not prove an ACK. --sealed-gen is
an operator assertion, not checked against storage by this CLI. --without-seal
deliberately abandons the unpublished tail and may require a forced move.
The clean target may be the source host once sealed (requires server support).
On an unknown move outcome, rerun with the SAME --request-id.
Show is read-only. Lease state uses the master's reported host validity at
query time, not a comparison with this machine's clock; it is not a grant.

--confirm-forced must be exactly <volume>@<expected epoch>, retyped: the epoch
is the holder's epoch the move is sent against, and the master refuses it when
that is no longer current. --dry-run asks the master for an advisory preview
(nothing is written; admission is re-checked at the move); it is never a
confirmation. The lost range is stored before the move is issued and printed
from that record; the authority store and its lost-range files stay off the
volume hosts (layout rule).

lost-range prints the stored lost ranges of the volume's issued forced moves, in
issue order (all of them, or the one of --request-id). It never prints "no loss"
for a forced move whose record is missing or fails its checks.

exit: 0 done; 1 refused by the master (nothing moved); 2 refused locally (usage
or confirmation; nothing sent); 3 outcome unknown: rerun with the SAME
--request-id; 4 the move is issued but its lost range is not proven (or a
stored range is unreadable); 5 not authorized by blockmaster (no client
certificate where it requires mutual TLS, or a certificate or role its role
file does not admit; set SW_BLOCK_TLS_CERT, SW_BLOCK_TLS_KEY, SW_BLOCK_TLS_CA).
`

// workspaceDial is swapped by tests.
var workspaceDial = wsclient.Dial

// runOpsWorkspace is `sw-block ops workspace`.
func runOpsWorkspace(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, workspaceUsage)
		return wsExitRefusedLocally
	}
	switch args[0] {
	case "planned-move":
		return runWorkspacePlannedMove(args[1:], stdout, stderr)
	case "show":
		return runWorkspaceShow(args[1:], stdout, stderr)
	case "move":
		return runWorkspaceCleanMove(args[1:], stdout, stderr)
	case "revoke":
		return runWorkspaceRevoke(args[1:], stdout, stderr)
	case "force-move":
		return runWorkspaceForceMove(args[1:], stdout, stderr)
	case "lost-range":
		return runWorkspaceLostRange(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, workspaceUsage)
		return wsExitOK
	default:
		fmt.Fprintf(stderr, "sw-block ops workspace: unknown subcommand %q\n", args[0])
		fmt.Fprint(stderr, workspaceUsage)
		return wsExitRefusedLocally
	}
}

func workspaceExit(o wsclient.Outcome) int {
	switch o {
	case wsclient.OutcomeOK:
		return wsExitOK
	case wsclient.OutcomeRefusedLocally:
		return wsExitRefusedLocally
	case wsclient.OutcomeRefused:
		return wsExitRefused
	case wsclient.OutcomeReadoutUnproven:
		return wsExitReadoutUnproven
	case wsclient.OutcomeNotAuthorized:
		return wsExitNotAuthorized
	}
	return wsExitUnknown
}

func runWorkspaceForceMove(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(workspaceForceMoveName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, workspaceUsage) }
	var m wsclient.ForcedMove
	var master string
	var dryRun bool
	var timeout time.Duration
	fs.StringVar(&master, "master", "", "blockmaster gRPC address host:port")
	fs.StringVar(&m.VolumeID, "volume", "", "volume id")
	fs.StringVar(&m.TargetHost, "target-host", "", "target node server id")
	fs.StringVar(&m.TargetDataAddr, "target-data", "", "target data address host:port")
	fs.StringVar(&m.TargetCtrlAddr, "target-ctrl", "", "target control address host:port")
	fs.StringVar(&m.RequestID, "request-id", "", "this move's id; a rerun with the same id returns the stored grant")
	fs.StringVar(&m.Confirm, "confirm-forced", "", "exactly <volume>@<expected epoch>")
	fs.BoolVar(&dryRun, "dry-run", false, "advisory preview only; writes nothing")
	fs.DurationVar(&timeout, "timeout", wsDefaultTimeout, "bound for the RPCs")
	if err := fs.Parse(args); err != nil {
		return wsExitRefusedLocally
	}
	if fs.NArg() != 0 || master == "" || m.VolumeID == "" || m.TargetHost == "" || m.TargetDataAddr == "" || m.TargetCtrlAddr == "" || m.RequestID == "" || timeout <= 0 {
		fmt.Fprintf(stderr, "%s: --master, --volume, --target-host, --target-data, --target-ctrl, --request-id and --confirm-forced are required (refused locally; nothing sent)\n", workspaceForceMoveName)
		return wsExitRefusedLocally
	}
	// Row Parse -> Refused: checked before any connection or RPC.
	if _, err := wsclient.ParseConfirmation(m.Confirm, m.VolumeID); err != nil {
		fmt.Fprintf(stderr, "%s: refused locally, nothing sent: %v\n", workspaceForceMoveName, err)
		return wsExitRefusedLocally
	}
	c, err := workspaceDial(master)
	if err != nil {
		fmt.Fprintf(stderr, "%s: refused locally, nothing sent: master address: %v\n", workspaceForceMoveName, err)
		return wsExitRefusedLocally
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if dryRun {
		return workspacePreview(ctx, c, m, stdout, stderr)
	}
	g, err := c.ForceMove(ctx, m)
	switch o := wsclient.OutcomeOf(err); o {
	case wsclient.OutcomeOK:
		printForcedGrant(stdout, g)
		fmt.Fprintln(stdout, wsclient.Readout(m.VolumeID, g.GetLostRange()))
		fmt.Fprintf(stdout, "next: start volume %s on %s (this command moved authority only)\n", m.VolumeID, g.GetNewHost())
		return wsExitOK
	case wsclient.OutcomeReadoutUnproven:
		if g != nil {
			printForcedGrant(stdout, g)
		}
		fmt.Fprintf(stderr, "%s: %v\n", workspaceForceMoveName, err)
		fmt.Fprintf(stderr, "%s: the move IS issued; do not assume no loss. Check the master's load errors, and read the range with:\n", workspaceForceMoveName)
		fmt.Fprintf(stderr, "%s%s\n", workspaceRecoveryPrefix, shellCommand("sw-block", "ops", "workspace", "lost-range", "--master", master, "--volume", m.VolumeID, "--request-id", m.RequestID))
		return wsExitReadoutUnproven
	case wsclient.OutcomeRefused:
		fmt.Fprintf(stderr, "%s: refused by the master (nothing moved): %v\n", workspaceForceMoveName, err)
		if at, ok := c.AdmissibleAt(ctx, m); ok {
			fmt.Fprintf(stderr, "%s: admissible at %s, master clock (the lease horizon plus clock skew); wait, there is no bypass\n", workspaceForceMoveName, wsclient.FormatTime(at.UnixNano()))
		}
		return wsExitRefused
	case wsclient.OutcomeUnknown:
		fmt.Fprintf(stderr, "%s: %v\n", workspaceForceMoveName, err)
		fmt.Fprintf(stderr, "%s: outcome unknown: rerun with the SAME --request-id %s\n", workspaceForceMoveName, m.RequestID)
		return wsExitUnknown
	default:
		fmt.Fprintf(stderr, "%s: %v\n", workspaceForceMoveName, err)
		return workspaceExit(o)
	}
}

// workspaceRecoveryPrefix starts the line that carries a recovery command.
const workspaceRecoveryPrefix = "  "

// shellCommand joins args for a POSIX shell, single-quoting any argument
// that is not plainly safe, so a printed command can be pasted as is.
func shellCommand(args ...string) string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = shellQuote(a)
	}
	return strings.Join(out, " ")
}

func shellQuote(a string) string {
	if a != "" && strings.Trim(a, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@%+=:,./_-") == "" {
		return a
	}
	return "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
}

func printForcedGrant(w io.Writer, g *control.WorkspaceGrant) {
	fmt.Fprintf(w, "forced move issued: volume %s request %s: %s@%d on %s -> %s@%d on %s (kept HEAD gen %d, remote LSN %d)\n",
		g.GetVolumeId(), g.GetRequestId(), g.GetOldReplicaId(), g.GetOldEpoch(), g.GetOldHost(), g.GetNewReplicaId(), g.GetNewEpoch(), g.GetNewHost(), g.GetHeadGen(), g.GetRemoteLsn())
}

// workspacePreview is the --dry-run row: advisory, writes nothing, exit 0
// whatever the answer (a failed preview call is reported by its outcome).
func workspacePreview(ctx context.Context, c *wsclient.Client, m wsclient.ForcedMove, stdout, stderr io.Writer) int {
	p, err := c.PreviewForcedMove(ctx, m)
	if err != nil {
		fmt.Fprintf(stderr, "%s --dry-run: %v\n", workspaceForceMoveName, err)
		if wsclient.OutcomeOf(err) == wsclient.OutcomeUnknown {
			fmt.Fprintf(stderr, "%s --dry-run: the preview failed; nothing was written\n", workspaceForceMoveName)
		}
		return workspaceExit(wsclient.OutcomeOf(err))
	}
	verdict := "not admitted"
	if p.GetAdmitted() {
		verdict = "admitted"
	}
	fmt.Fprintf(stdout, "preview (advisory): at master time %s, may change; admission is re-checked at the move. Nothing was written; this is not a confirmation.\n", wsclient.FormatTime(p.GetMasterTimeNs()))
	if p.GetReason() != "" {
		fmt.Fprintf(stdout, "forced move %s: %s\n", verdict, p.GetReason())
	} else {
		fmt.Fprintf(stdout, "forced move %s\n", verdict)
	}
	if !p.GetAdmitted() && p.GetLostUntilNs() > p.GetMasterTimeNs() {
		fmt.Fprintf(stdout, "admissible at %s, master clock\n", wsclient.FormatTime(p.GetLostUntilNs()))
	}
	if p.GetHeadRead() {
		at := "unknown"
		if p.GetHeadTimeKnown() {
			at = wsclient.FormatTime(p.GetHeadTimeNs()) + " " + wsclient.HeadTimeClock
		}
		fmt.Fprintf(stdout, "HEAD now: gen %d, epoch %d, holder %s, remote LSN %d, published at %s\n", p.GetHeadGen(), p.GetHeadEpoch(), p.GetHeadHolder(), p.GetHeadRemoteLsn(), at)
		if p.GetAdmitted() {
			fmt.Fprintf(stdout, "if moved now: writes with LSN <= %d are kept; writes acknowledged after that and before %s, master clock, may be lost\n", p.GetHeadRemoteLsn(), wsclient.FormatTime(p.GetLostUntilNs()))
		}
	} else if p.GetHeadError() != "" {
		fmt.Fprintf(stdout, "HEAD read failed: %s\n", p.GetHeadError())
	}
	return wsExitOK
}

func runWorkspaceLostRange(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(workspaceLostRangeName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, workspaceUsage) }
	var master, volume, requestID string
	var timeout time.Duration
	fs.StringVar(&master, "master", "", "blockmaster gRPC address host:port")
	fs.StringVar(&volume, "volume", "", "volume id")
	fs.StringVar(&requestID, "request-id", "", "only this forced move's range (default: all, in issue order)")
	fs.DurationVar(&timeout, "timeout", wsDefaultTimeout, "bound for the RPC")
	if err := fs.Parse(args); err != nil {
		return wsExitRefusedLocally
	}
	if fs.NArg() != 0 || master == "" || volume == "" || timeout <= 0 {
		fmt.Fprintf(stderr, "%s: --master and --volume are required\n", workspaceLostRangeName)
		return wsExitRefusedLocally
	}
	c, err := workspaceDial(master)
	if err != nil {
		fmt.Fprintf(stderr, "%s: master address: %v\n", workspaceLostRangeName, err)
		return wsExitRefusedLocally
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ranges, err := c.LostRanges(ctx, volume, requestID)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", workspaceLostRangeName, err)
		if wsclient.OutcomeOf(err) == wsclient.OutcomeReadoutUnproven {
			fmt.Fprintf(stderr, "%s: a forced move's lost range is missing or unreadable: the readout is NOT proven; do not assume no loss\n", workspaceLostRangeName)
		}
		return workspaceExit(wsclient.OutcomeOf(err))
	}
	if len(ranges) == 0 {
		fmt.Fprintf(stdout, "volume %s has no issued forced move\n", volume)
		return wsExitOK
	}
	for i, r := range ranges {
		fmt.Fprintf(stdout, "range %d of %d: request %s, %s@%d -> epoch %d, lost after LSN %d, until %s (master clock)\n",
			i+1, len(ranges), r.GetRequestId(), r.GetOldReplicaId(), r.GetOldEpoch(), r.GetNewEpoch(), r.GetLostAfterLsn(), wsclient.FormatTime(r.GetLostUntilNs()))
		fmt.Fprintf(stdout, "  %s\n", wsclient.Readout(volume, r))
	}
	return wsExitOK
}
