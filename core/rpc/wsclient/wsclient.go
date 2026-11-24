// Package wsclient is the operator client of the blockmaster workspace
// service for FORCED moves: the move itself, its read-only preview, and the
// lost-range readout. The sw-block CLI and other operator tools share it so
// the confirmation rule and the readout wording are the same everywhere.
//
// A forced move transfers AUTHORITY only. Starting the volume on the target
// is the next step, and nothing here bypasses the master's admission (lease
// horizon plus clock skew): there is no bypass.
package wsclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Outcome classifies a call's failure for the operator.
type Outcome int

const (
	// OutcomeOK: the call succeeded.
	OutcomeOK Outcome = iota
	// OutcomeRefusedLocally: refused before any RPC (bad confirmation).
	OutcomeRefusedLocally
	// OutcomeRefused: the master answered with a definite refusal; nothing
	// was moved by this call.
	OutcomeRefused
	// OutcomeUnknown: transport error or deadline; the move may or may not
	// have been issued. Rerun with the SAME request id: the master replays
	// an issued move's stored grant and never mints a second one.
	OutcomeUnknown
	// OutcomeReadoutUnproven: the lost range cannot be read (missing record
	// or failed checks); it must never be reported as "no loss".
	OutcomeReadoutUnproven
	// OutcomeNotAuthorized: blockmaster did not accept this caller (no
	// certificate, one not in its role file, or a role that may not do this);
	// nothing was done.
	OutcomeNotAuthorized
)

// ErrConfirmation refuses a forced move locally: the confirmation must be
// exactly "<volume>@<expected epoch>".
var ErrConfirmation = errors.New("forced move confirmation")

// Error carries a classified failure.
type Error struct {
	Outcome Outcome
	Op      string
	Err     error
}

func (e *Error) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// OutcomeOf returns err's outcome (OutcomeOK for nil).
func OutcomeOf(err error) Outcome {
	if err == nil {
		return OutcomeOK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Outcome
	}
	return OutcomeUnknown
}

// classify maps a gRPC error: the master's definite answers are refusals,
// DataLoss is an unproven readout, anything else leaves the outcome unknown.
func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return &Error{Outcome: OutcomeNotAuthorized, Op: op, Err: fmt.Errorf("not authorized by blockmaster: %s (its role file must list this certificate's fingerprint with a role allowed to do this)", status.Convert(err).Message())}
	case codes.Unavailable:
		// A plaintext client against blockmaster with mutual TLS: the server
		// closes the connection before its HTTP/2 preface.
		if os.Getenv(link.EnvTLSCert) == "" && strings.Contains(status.Convert(err).Message(), "error reading server preface") {
			return &Error{Outcome: OutcomeNotAuthorized, Op: op, Err: fmt.Errorf("blockmaster refused this plaintext connection: it requires mutual TLS; set %s, %s and %s to a certificate in its role file and its CA", link.EnvTLSCert, link.EnvTLSKey, link.EnvTLSCA)}
		}
	case codes.FailedPrecondition, codes.InvalidArgument, codes.NotFound, codes.Unimplemented:
		return &Error{Outcome: OutcomeRefused, Op: op, Err: errors.New(status.Convert(err).Message())}
	case codes.DataLoss:
		return &Error{Outcome: OutcomeReadoutUnproven, Op: op, Err: errors.New(status.Convert(err).Message())}
	}
	return &Error{Outcome: OutcomeUnknown, Op: op, Err: err}
}

// ParseConfirmation checks that confirm is exactly "<volume>@<epoch>" for
// volume, with a positive decimal epoch, and returns that epoch: it is the
// expected epoch the move is sent with (the master refuses it unless it is
// the current one).
func ParseConfirmation(confirm, volume string) (uint64, error) {
	if confirm == "" {
		return 0, fmt.Errorf("%w: --confirm-forced %s@<expected epoch> is required", ErrConfirmation, volume)
	}
	at := strings.LastIndexByte(confirm, '@')
	if at < 0 || confirm[:at] != volume {
		return 0, fmt.Errorf("%w: %q does not name volume %q as %s@<expected epoch>", ErrConfirmation, confirm, volume, volume)
	}
	digits := confirm[at+1:]
	epoch, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != digits {
		return 0, fmt.Errorf("%w: %q: the epoch must be a positive decimal number", ErrConfirmation, confirm)
	}
	return epoch, nil
}

// ForcedMove names a forced move. TargetHost is the target node's server id.
type ForcedMove struct {
	RequestID, VolumeID                        string
	TargetHost, TargetDataAddr, TargetCtrlAddr string
	// Confirm must be exactly "<VolumeID>@<expected epoch>".
	Confirm string
}

// Client calls the workspace service.
type Client struct {
	rpc  control.WorkspaceServiceClient
	conn *grpc.ClientConn
}

// New wraps an existing service client.
func New(rpc control.WorkspaceServiceClient) *Client { return &Client{rpc: rpc} }

// Dial connects to the blockmaster gRPC address (plaintext, as the other
// operator commands).
func Dial(addr string) (*Client, error) {
	conn, err := link.DialMasterFromEnv(addr)
	if err != nil {
		return nil, err
	}
	return &Client{rpc: control.NewWorkspaceServiceClient(conn), conn: conn}, nil
}

// Close closes a Dial connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// request checks the confirmation (no RPC on failure), resolves the
// holder's logical replica id at the confirmed epoch from QueryWorkspace
// (v1 keeps the replica id across moves) and builds the forced request.
func (c *Client) request(ctx context.Context, op string, m ForcedMove) (*control.WorkspaceMove, error) {
	epoch, err := ParseConfirmation(m.Confirm, m.VolumeID)
	if err != nil {
		return nil, &Error{Outcome: OutcomeRefusedLocally, Op: op, Err: err}
	}
	if m.RequestID == "" || m.TargetHost == "" || m.TargetDataAddr == "" || m.TargetCtrlAddr == "" {
		return nil, &Error{Outcome: OutcomeRefusedLocally, Op: op, Err: errors.New("a request id, target host and target data/ctrl addresses are required")}
	}
	st, err := c.rpc.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: m.VolumeID})
	if err != nil {
		e := classify(op+": read the volume's holder", err)
		var ce *Error
		if errors.As(e, &ce) && ce.Outcome == OutcomeUnknown {
			ce.Err = fmt.Errorf("no move was sent: %w", ce.Err)
		}
		return nil, e
	}
	replica := holderAt(st, epoch)
	if replica == "" {
		cur := "none"
		if l := st.GetLease(); l != nil {
			cur = fmt.Sprintf("%s@%d", l.GetReplicaId(), l.GetEpoch())
		}
		return nil, &Error{Outcome: OutcomeRefused, Op: op, Err: fmt.Errorf("the master knows no holder of volume %q at epoch %d (current lease: %s); nothing was sent", m.VolumeID, epoch, cur)}
	}
	return &control.WorkspaceMove{RequestId: m.RequestID, VolumeId: m.VolumeID, ExpectedReplicaId: replica, ExpectedEpoch: epoch, TargetReplicaId: replica,
		TargetServerId: m.TargetHost, TargetDataAddr: m.TargetDataAddr, TargetCtrlAddr: m.TargetCtrlAddr, Kind: "forced", Force: true}, nil
}

// holderAt returns the logical replica id that held the volume at epoch:
// the current lease, or the journal (an issued move from or to that epoch,
// e.g. on a rerun after the move issued).
func holderAt(st *control.WorkspaceStatus, epoch uint64) string {
	if l := st.GetLease(); l != nil && l.GetEpoch() == epoch && l.GetReplicaId() != "" {
		return l.GetReplicaId()
	}
	for _, g := range st.GetMoves() {
		if g.GetOldEpoch() == epoch && g.GetOldReplicaId() != "" {
			return g.GetOldReplicaId()
		}
		if g.GetIssued() && g.GetNewEpoch() == epoch && g.GetNewReplicaId() != "" {
			return g.GetNewReplicaId()
		}
	}
	return ""
}

// ForceMove sends the forced move (kind forced, force true) only when the
// confirmation matches. On success the grant is issued; its stored lost
// range is grant.LostRange, and when that is absent the error is
// OutcomeReadoutUnproven with the grant still returned.
func (c *Client) ForceMove(ctx context.Context, m ForcedMove) (*control.WorkspaceGrant, error) {
	const op = "forced move"
	q, err := c.request(ctx, op, m)
	if err != nil {
		return nil, err
	}
	g, err := c.rpc.MoveWorkspace(ctx, q)
	if err != nil {
		return nil, classify(op, err)
	}
	if err := checkGrant(g, q); err != nil {
		return g, &Error{Outcome: OutcomeUnknown, Op: op, Err: fmt.Errorf("%v; rerun with the same request id", err)}
	}
	if err := checkRange(g.GetLostRange(), g); err != nil {
		return g, &Error{Outcome: OutcomeReadoutUnproven, Op: op, Err: err}
	}
	return g, nil
}

// checkGrant binds a successful answer to the request that was sent: an
// issued grant of the requested kind for this request id and volume, from the expected
// holder at the expected epoch to the requested target, at a higher epoch.
// Anything else leaves the outcome unknown.
func checkGrant(g *control.WorkspaceGrant, q *control.WorkspaceMove) error {
	if !g.GetIssued() || g.GetKind() != q.GetKind() || g.GetRequestId() != q.GetRequestId() || g.GetVolumeId() != q.GetVolumeId() ||
		g.GetOldReplicaId() != q.GetExpectedReplicaId() || g.GetOldEpoch() != q.GetExpectedEpoch() ||
		g.GetNewReplicaId() != q.GetTargetReplicaId() || g.GetNewHost() != q.GetTargetServerId() || g.GetNewEpoch() <= g.GetOldEpoch() {
		return fmt.Errorf("the answer does not match the request (issued=%t kind=%q request=%q volume=%q old=%s@%d new=%s@%d on %q)",
			g.GetIssued(), g.GetKind(), g.GetRequestId(), g.GetVolumeId(), g.GetOldReplicaId(), g.GetOldEpoch(), g.GetNewReplicaId(), g.GetNewEpoch(), g.GetNewHost())
	}
	return nil
}

// checkRange: a forced grant must carry its own stored range, naming the
// same move and the same kept HEAD (generation and frontier).
func checkRange(r *control.WorkspaceLostRange, g *control.WorkspaceGrant) error {
	if r == nil {
		why := g.GetLostRangeError()
		if why == "" {
			why = "the master returned no lost range"
		}
		return fmt.Errorf("move issued (volume %s epoch %d -> %d) but the lost range is not proven: %s", g.GetVolumeId(), g.GetOldEpoch(), g.GetNewEpoch(), why)
	}
	if r.GetRequestId() != g.GetRequestId() || r.GetVolumeId() != g.GetVolumeId() || r.GetNewEpoch() != g.GetNewEpoch() || r.GetOldEpoch() != g.GetOldEpoch() || r.GetOldReplicaId() != g.GetOldReplicaId() {
		return fmt.Errorf("move issued (volume %s epoch %d -> %d) but its lost range names another move (volume %q request %q epoch %d)", g.GetVolumeId(), g.GetOldEpoch(), g.GetNewEpoch(), r.GetVolumeId(), r.GetRequestId(), r.GetNewEpoch())
	}
	if r.GetHeadGen() != g.GetHeadGen() || r.GetLostAfterLsn() != g.GetRemoteLsn() {
		return fmt.Errorf("move issued (volume %s epoch %d -> %d) but its lost range disagrees with the kept HEAD (range gen %d LSN %d, grant gen %d LSN %d)", g.GetVolumeId(), g.GetOldEpoch(), g.GetNewEpoch(), r.GetHeadGen(), r.GetLostAfterLsn(), g.GetHeadGen(), g.GetRemoteLsn())
	}
	return nil
}

// PreviewForcedMove asks the master whether the forced move is admitted
// now, writing nothing. The confirmation is checked as for the move (the
// preview is evaluated at its epoch), but a preview never stands in for it.
func (c *Client) PreviewForcedMove(ctx context.Context, m ForcedMove) (*control.WorkspaceMovePreview, error) {
	const op = "forced move preview"
	q, err := c.request(ctx, op, m)
	if err != nil {
		return nil, err
	}
	p, err := c.rpc.PreviewWorkspaceMove(ctx, q)
	return p, classify(op, err)
}

// previewMove is PreviewForcedMove for an already built request.
func (c *Client) previewMove(ctx context.Context, q *control.WorkspaceMove) (*control.WorkspaceMovePreview, error) {
	p, err := c.rpc.PreviewWorkspaceMove(ctx, q)
	return p, classify("forced move preview", err)
}

// AdmissibleAt asks the preview for when a refused forced move becomes
// admissible; ok is false when the preview cannot tell (admitted already,
// no lease, or the preview failed).
func (c *Client) AdmissibleAt(ctx context.Context, m ForcedMove) (at time.Time, ok bool) {
	q, err := c.request(ctx, "forced move preview", m)
	if err != nil {
		return time.Time{}, false
	}
	p, err := c.previewMove(ctx, q)
	if err != nil || p.GetLostUntilNs() == 0 || p.GetLostUntilNs() <= p.GetMasterTimeNs() {
		return time.Time{}, false
	}
	return time.Unix(0, p.GetLostUntilNs()), true
}

// LostRanges returns the stored lost ranges of the volume's issued forced
// moves in issue order, or requestID's only. A missing or unreadable record
// is OutcomeReadoutUnproven.
func (c *Client) LostRanges(ctx context.Context, volume, requestID string) ([]*control.WorkspaceLostRange, error) {
	r, err := c.rpc.QueryWorkspaceLostRange(ctx, &control.WorkspaceLostRangeQuery{VolumeId: volume, RequestId: requestID})
	if err != nil {
		return nil, classify("lost range", err)
	}
	for _, x := range r.GetRanges() {
		if x.GetVolumeId() != volume || (requestID != "" && x.GetRequestId() != requestID) {
			return nil, &Error{Outcome: OutcomeReadoutUnproven, Op: "lost range", Err: fmt.Errorf("the answer names another range (volume %q request %q)", x.GetVolumeId(), x.GetRequestId())}
		}
	}
	return r.GetRanges(), nil
}

// HeadTimeClock labels a HEAD time: the filer entry's mtime, which the
// host that published the HEAD sets from its own clock.
const HeadTimeClock = "(publishing host clock as recorded in the filer, 1 s resolution)"

// FormatTime prints a master or filer time.
func FormatTime(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }

// Readout is the operator sentence for a stored lost range of volume.
func Readout(volume string, r *control.WorkspaceLostRange) string {
	old := fmt.Sprintf("%s@%d", r.GetOldReplicaId(), r.GetOldEpoch())
	if r.GetHeadGen() == 0 {
		return fmt.Sprintf("no remote image exists: everything written by %s is lost.", old)
	}
	at := "an unknown time (no filer time was recorded)"
	if r.GetHeadTimeKnown() {
		at = FormatTime(r.GetHeadTimeNs()) + " " + HeadTimeClock
	}
	return fmt.Sprintf("volume %s restored to the last published image at %s (gen %d, LSN %d): writes with LSN <= %d are preserved; "+
		"writes acknowledged by %s after that and before %s, master clock, may be lost; the upper bound is a time, not an LSN.",
		volume, at, r.GetHeadGen(), r.GetLostAfterLsn(), r.GetLostAfterLsn(), old, FormatTime(r.GetLostUntilNs()))
}
