package wsclient

import (
	"context"
	"errors"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// Show reads the existing M2 lease/ACK/move view. It grants no authority.
func (c *Client) Show(ctx context.Context, volume string) (*control.WorkspaceStatus, error) {
	const op = "workspace show"
	if volume == "" {
		return nil, &Error{Outcome: OutcomeRefusedLocally, Op: op, Err: errors.New("volume is required")}
	}
	view, err := c.rpc.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: volume})
	if err != nil {
		return nil, classify(op, err)
	}
	if view == nil {
		return nil, &Error{Outcome: OutcomeUnknown, Op: op, Err: errors.New("missing workspace reply")}
	}
	return view, nil
}
