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
// creates its own. It is deleted only with proof that its server is dead:
// the kernel's record of the server pid (set by START_DEV) names a process
// that no longer exists. A device whose owner may be alive, never started
// (no pid), or cannot be read is refused untouched; an operator removes it
// once it is proven unused. A reused pid reads as alive: refused (safe).
func reclaimStale(c controller, id uint32, exists bool, alive func(pid int32) (bool, error)) error {
	if !exists {
		return nil
	}
	var info devInfo
	if _, err := c.command(uCmdGetDevInfo, id, bytesOf(&info), 0); err != nil {
		return fmt.Errorf("ublk: device %d exists and its owner cannot be read (%v): not deleting it", id, err)
	}
	if info.SrvPID <= 0 {
		return fmt.Errorf("ublk: device %d exists with no recorded server (never started, or starting): not deleting it; remove it once proven unused", id)
	}
	live, err := alive(info.SrvPID)
	if err != nil {
		return fmt.Errorf("ublk: device %d: liveness of its server pid %d unknown (%v): not deleting it", id, info.SrvPID, err)
	}
	if live {
		return fmt.Errorf("ublk: device %d belongs to the live server pid %d: refusing", id, info.SrvPID)
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

// settleQueues waits, until one shared deadline, for the queues to stop
// and releases each one that did. A queue that did not stop keeps its
// mappings: its thread may still read descriptors or write buffers, so
// they are leaked (fail closed) and the failure is returned.
func settleQueues(qs []settler, timeout time.Duration) error {
	var firstErr error
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	expired := false
	for _, q := range qs {
		if !expired {
			select {
			case <-q.doneCh():
				q.release()
				continue
			case <-deadline.C:
				expired = true
			}
		}
		select {
		case <-q.doneCh(): // stopped by now: safe to release
			q.release()
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("ublk: %s did not stop; its mappings are kept (leaked) while its thread may run", q.name())
			}
		}
	}
	return firstErr
}
