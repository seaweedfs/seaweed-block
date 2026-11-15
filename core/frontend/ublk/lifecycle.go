package ublk

import (
	"fmt"
	"time"
)

// controller runs one ublk control command (the device's control ring in
// production; a recorder in tests).
type controller interface {
	command(cmdOp uint32, devID uint32, buf []byte, data uint64) (int32, error)
}

// reclaimStale handles a device already present at id before this server
// creates its own. RED placeholder: removes it unconditionally.
func reclaimStale(c controller, id uint32, exists bool, alive func(pid int32) (bool, error)) error {
	if !exists {
		return nil
	}
	_, _ = c.command(uCmdStopDev, id, nil, 0)
	if _, err := c.command(uCmdDelDev, id, nil, 0); err != nil {
		return fmt.Errorf("ublk: delete stale device %d: %w", id, err)
	}
	return nil
}

// settler is a queue being shut down: done closes when its thread has
// exited, release frees the memory the kernel and the thread share.
type settler interface {
	doneCh() <-chan struct{}
	release()
	name() string
}

// settleQueues waits for the queues to stop. RED placeholder: releases
// every queue even when it did not stop in time.
func settleQueues(qs []settler, timeout time.Duration) error {
	var firstErr error
	for _, q := range qs {
		select {
		case <-q.doneCh():
		case <-time.After(timeout):
			if firstErr == nil {
				firstErr = fmt.Errorf("ublk: %s did not stop", q.name())
			}
		}
		q.release()
	}
	return firstErr
}
