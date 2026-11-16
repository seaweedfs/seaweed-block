package ublk

import (
	"fmt"
	"time"
)

// refusePreExisting refuses a device id that is already present. There is
// no automatic reclaim: deciding that another server's device is dead (a
// device-info read plus a pid probe, then a numeric STOP/DEL) races a
// replacement device created in between, and a pid probe from another PID
// namespace proves nothing. Nothing is sent to the kernel here; removing a
// leftover device is a separate operator step (see the package doc).
func refusePreExisting(id uint32, exists bool) error {
	if !exists {
		return nil
	}
	return fmt.Errorf("ublk: device %d already exists (%s%d); refusing to start: it may belong to another server. "+
		"Remove it as a separate operator step once it is proven unused", id, charDevicePrefix, id)
}

// controller runs one ublk control command (the device's control ring in
// production; a recorder in tests).
type controller interface {
	command(cmdOp uint32, devID uint32, buf []byte, data uint64) (int32, error)
}

// settler is a queue being shut down: done closes when its thread has
// exited, release frees the memory the kernel and the thread share.
type settler interface {
	doneCh() <-chan struct{}
	release()
	name() string
	result() error // read only after doneCh is closed
}

// settleQueues waits, until one shared deadline, for the queues to stop
// and releases each one that did, returning the first error a stopped
// queue's thread ended with. A queue that did not stop keeps its mappings:
// its thread may still read descriptors or write buffers, so they are
// leaked (fail closed) and the failure is returned.
func settleQueues(qs []settler, timeout time.Duration) (leaked bool, err error) {
	var firstErr error
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	expired := false
	for _, q := range qs {
		if !expired {
			select {
			case <-q.doneCh():
				q.release()
				if err := q.result(); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("ublk: %s: %w", q.name(), err)
				}
				continue
			case <-deadline.C:
				expired = true
			}
		}
		select {
		case <-q.doneCh(): // stopped by now: safe to release
			q.release()
			if err := q.result(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("ublk: %s: %w", q.name(), err)
			}
		default:
			leaked = true
			if firstErr == nil {
				firstErr = fmt.Errorf("ublk: %s did not stop; its mappings are kept (leaked) while its thread may run", q.name())
			}
		}
	}
	return leaked, firstErr
}
