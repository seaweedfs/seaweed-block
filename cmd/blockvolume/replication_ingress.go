package main

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// replicationIngress owns both inbound lanes. Lock order: Host.selfFenceMu ->
// mu -> listener role gate. Stop never calls Host. No socket is created until
// an assignment permits a replica role, and each listener is fenced before Serve.
type replicationIngress struct {
	mu                        sync.Mutex
	addr, volumeID, floorPath string
	store                     storage.LogicalStorage
	dualLane                  bool
	// rf1Only, when set, refuses every role with replica slots with this reason
	// (walstore-chunk). The host latches the error: no role is adopted, not Healthy.
	rf1Only  string
	listener *transport.ReplicaListener
	dual     net.Listener
	cancel   context.CancelFunc
	closed   bool
}

// newReplicationIngress is the volume's ingress from its flags; an RF1-only
// volume (walstore-chunk) refuses every role with replica slots.
func newReplicationIngress(f flags, store storage.LogicalStorage) *replicationIngress {
	r := &replicationIngress{addr: f.dataAddr, store: store, volumeID: f.volumeID,
		floorPath: replicaFloorPath(f), dualLane: f.recoveryMode == "dual-lane"}
	r.rf1Only = rf1Only(f)
	return r
}

// rf1Only is the single-replica rule of this volume, "" when replication is
// allowed. The host refuses assignments with replica slots (installed at
// construction, before any fact is applied or replayed); the ingress gate
// refuses the same roles again.
func rf1Only(f flags) string {
	if f.durableImpl == "walstore-chunk" {
		return chunkRF1Reason
	}
	return ""
}

func (r *replicationIngress) FenceAsPrimary(epoch uint64) error {
	return r.FenceAsPrimaryWithPeers(epoch, true)
}
func (r *replicationIngress) FenceAsPrimaryWithPeers(epoch uint64, hasPeers bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("replication ingress closed")
	}
	if !hasPeers {
		if r.listener != nil {
			if err := r.listener.FenceAsPrimary(epoch); err != nil {
				return err
			}
		}
		r.stopLocked(true)
		return nil
	}
	if r.rf1Only != "" {
		return fmt.Errorf("volume %s refuses a primary role with replica peers at epoch %d: %s", r.volumeID, epoch, r.rf1Only)
	}
	return r.roleLocked(epoch, true)
}
func (r *replicationIngress) AdmitAsReplica(epoch uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("replication ingress closed")
	}
	if r.rf1Only != "" {
		return fmt.Errorf("volume %s refuses a replica role at epoch %d: %s", r.volumeID, epoch, r.rf1Only)
	}
	return r.roleLocked(epoch, false)
}
func (r *replicationIngress) roleLocked(epoch uint64, primary bool) error {
	listener := r.listener
	fresh := listener == nil
	if fresh {
		var err error
		listener, err = transport.NewReplicaListenerWithFloor(r.addr, r.store, r.volumeID, r.floorPath)
		if err != nil {
			return err
		}
	}
	var err error
	if primary {
		err = listener.FenceAsPrimary(epoch)
	} else {
		err = listener.AdmitAsReplica(epoch)
	}
	if err != nil {
		if fresh {
			listener.Stop()
		}
		return err
	}
	if !fresh {
		return nil
	}
	var dl net.Listener
	if r.dualLane {
		var addr string
		addr, err = deriveDualLaneAddr(r.addr)
		if err == nil {
			dl, err = net.Listen("tcp", addr)
		}
		if err != nil {
			listener.Stop()
			return err
		}
	}
	r.listener, r.dual = listener, dl
	listener.Serve()
	if dl != nil {
		ctx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		bridge := recovery.NewReplicaBridgeWithGate(r.store, listener)
		go bridge.AcceptDualLaneLoop(ctx, dl)
	}
	return nil
}
func (r *replicationIngress) stopLocked(withdrawn bool) {
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	if r.dual != nil {
		_ = r.dual.Close()
		r.dual = nil
	}
	if r.listener != nil {
		if withdrawn {
			// FenceAsPrimary already drained admitted mutations. The role has
			// been revoked, so disconnect idle peers instead of waiting for
			// them to close a connection to an endpoint they no longer own.
			r.listener.StopHard()
		} else {
			r.listener.Stop()
		}
		r.listener = nil
	}
}
func (r *replicationIngress) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.stopLocked(false)
}
