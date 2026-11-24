package authority

// WorkspaceHolderVerdict is the one answer to "is there a live holder of V"
// (TERM-LIVE-HOLDER-001). The line's holder at epoch e may have acknowledged
// guest writes and may publish exactly when the verdict is Live.
type WorkspaceHolderVerdict int

const (
	// HolderUnknown: HEAD was not read or could not be read, names a higher
	// epoch, or names another holder at the line's epoch. Always refused.
	HolderUnknown WorkspaceHolderVerdict = iota
	// HolderNoLine: there is no authority line.
	HolderNoLine
	// HolderDrained: a clean revocation ACK is recorded for the line.
	HolderDrained
	// HolderSealed: HEAD names the line's holder at its epoch, detached.
	HolderSealed
	// HolderNeverStarted: HEAD is below the line's epoch. No claim at that
	// epoch had applied when HEAD was read, so nothing was acknowledged or
	// published under it (no I/O is admitted before the claim), whatever
	// lease exists. The delete's HEAD compare-and-swap to deleting fences a
	// claim that lands after the read.
	HolderNeverStarted
	// HolderLive: HEAD names the line's holder at its epoch, active.
	HolderLive
)

func (v WorkspaceHolderVerdict) String() string {
	switch v {
	case HolderNoLine:
		return "no line"
	case HolderDrained:
		return "drained"
	case HolderSealed:
		return "sealed"
	case HolderNeverStarted:
		return "never started"
	case HolderLive:
		return "live"
	}
	return "unknown"
}

// WorkspaceHolderState decides the verdict from durable inputs only: the
// authority line (present, epoch, replica, and its owner "<server>/<replica>"
// by the single owner decision; "" when unknown), the recorded revocation
// ACK, and HEAD as read (nil when it was not read or could not be read).
// Pure: no I/O, no state, no clock. It does not depend on master memory, so
// it holds across a blockmaster restart.
func WorkspaceHolderState(linePresent bool, lineEpoch uint64, lineReplica, owner string, ack *WorkspaceRevokeAck, head *WorkspaceHead) WorkspaceHolderVerdict {
	switch {
	case !linePresent:
		return HolderNoLine
	case ack != nil && ack.Epoch == lineEpoch && ack.ReplicaID == lineReplica:
		return HolderDrained
	case head == nil:
		return HolderUnknown
	case head.Epoch < lineEpoch:
		return HolderNeverStarted
	case head.Epoch > lineEpoch || owner == "" || head.Holder != owner:
		return HolderUnknown
	case head.Detached:
		return HolderSealed
	}
	return HolderLive
}
