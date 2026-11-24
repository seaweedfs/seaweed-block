package authority

import "testing"

// TERM-LIVE-HOLDER-001: one pure verdict from the line, the ACK and HEAD.
// "Never started" is HEAD below the line's epoch, not "never leased"; every
// doubtful case is Unknown (refused).
func TestWorkspaceHolderState(t *testing.T) {
	const owner = "node-a/r1"
	head := func(epoch uint64, holder string, detached bool) *WorkspaceHead {
		return &WorkspaceHead{Gen: 3, Epoch: epoch, Holder: holder, Detached: detached}
	}
	for _, c := range []struct {
		name  string
		line  bool
		ack   *WorkspaceRevokeAck
		head  *WorkspaceHead
		owner string
		want  WorkspaceHolderVerdict
	}{
		{"no line", false, nil, nil, owner, HolderNoLine},
		{"ACK for the line", true, &WorkspaceRevokeAck{ReplicaID: "r1", Epoch: 2, HeadGen: 3}, nil, owner, HolderDrained},
		{"ACK for an older epoch", true, &WorkspaceRevokeAck{ReplicaID: "r1", Epoch: 1}, head(2, owner, true), owner, HolderSealed},
		{"HEAD not read", true, nil, nil, owner, HolderUnknown},
		{"HEAD never claimed (epoch 0)", true, nil, head(0, "", false), owner, HolderNeverStarted},
		{"HEAD at an older epoch", true, nil, head(1, "node-b/r1", false), owner, HolderNeverStarted},
		{"HEAD claimed at the line's epoch, active", true, nil, head(2, owner, false), owner, HolderLive},
		{"HEAD claimed at the line's epoch, sealed", true, nil, head(2, owner, true), owner, HolderSealed},
		{"another holder at the line's epoch", true, nil, head(2, "node-b/r1", false), owner, HolderUnknown},
		{"HEAD at a higher epoch", true, nil, head(3, owner, false), owner, HolderUnknown},
		{"owner unknown at the line's epoch", true, nil, head(2, owner, false), "", HolderUnknown},
	} {
		if got := WorkspaceHolderState(c.line, 2, "r1", c.owner, c.ack, c.head); got != c.want {
			t.Errorf("%s: %s; want %s", c.name, got, c.want)
		}
	}
}
