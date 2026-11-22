package wsclient

import (
	"context"
	"errors"
	"fmt"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// CleanMove implements M2 Acked -> Moved (clean), INV-WS-CLEAN-MOVE-001.
// The master checks the actual detached HEAD and recorded ACK.
func (c *Client) CleanMove(ctx context.Context, q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
	const op = "clean move"
	if q.GetKind() != "clean" || q.GetForce() || q.GetVolumeId() == "" || q.GetRequestId() == "" || q.GetExpectedReplicaId() == "" || q.GetExpectedEpoch() == 0 || q.GetTargetReplicaId() != q.GetExpectedReplicaId() || q.GetTargetServerId() == "" || q.GetTargetDataAddr() == "" || q.GetTargetCtrlAddr() == "" {
		return nil, &Error{Outcome: OutcomeRefusedLocally, Op: op, Err: errors.New("clean move requires explicit identity, request id and target endpoints; force is not allowed")}
	}
	g, err := c.rpc.MoveWorkspace(ctx, q)
	if err != nil {
		return nil, classify(op, err)
	}
	if err := checkGrant(g, q); err != nil {
		return nil, &Error{Outcome: OutcomeUnknown, Op: op, Err: fmt.Errorf("%v; rerun with the same request id", err)}
	}
	return g, nil
}
