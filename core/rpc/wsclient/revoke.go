package wsclient

import (
	"context"
	"errors"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// Revoke implements M2 Granted -> Revoking (INV-WS-CLEAN-MOVE-001).
// Its reply proves the request was recorded, not that the host has ACKed.
func (c *Client) Revoke(ctx context.Context, id *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	const op = "workspace revoke"
	if id.GetVolumeId() == "" || id.GetReplicaId() == "" || id.GetEpoch() == 0 {
		return nil, &Error{Outcome: OutcomeRefusedLocally, Op: op, Err: errors.New("volume, replica and positive epoch are required")}
	}
	lease, err := c.rpc.RevokeWorkspace(ctx, id)
	if err != nil {
		return nil, classify(op, err)
	}
	if lease.GetVolumeId() != id.GetVolumeId() || lease.GetReplicaId() != id.GetReplicaId() || lease.GetEpoch() != id.GetEpoch() || !lease.GetRevoking() {
		return nil, &Error{Outcome: OutcomeUnknown, Op: op, Err: errors.New("reply does not match the requested revoking identity")}
	}
	return lease, nil
}
