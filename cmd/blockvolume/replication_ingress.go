package main

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/replication"
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
	applyGate                 *replication.ReplicaApplyGate
	dualLane                  bool
	// rf1Only, when set, refuses every role with replica slots with this reason
	// (walstore-chunk). The host latches the error: no role is adopted, not Healthy.
	rf1Only  string
	listener *transport.ReplicaListener
	// epochs is the volume's epoch table, kept across roles; epochsErr a load
	// failure, which refuses every role.
	epochs    *transport.EpochTable
	epochsErr error
	dual      net.Listener
	cancel    context.CancelFunc
	closed    bool
}

// newReplicationIngress is the volume's ingress from its flags; an RF1-only
// volume (walstore-chunk) refuses every role with replica slots.
func newReplicationIngress(f flags, store storage.LogicalStorage) *replicationIngress {
	r := &replicationIngress{addr: f.dataAddr, store: store, volumeID: f.volumeID,
		floorPath: replicaFloorPath(f), dualLane: f.recoveryMode == "dual-lane"}
	r.rf1Only = rf1Only(f)
	r.epochs, r.epochsErr = transport.LoadEpochTable(epochTablePath(f), f.volumeID)
	if r.epochsErr == nil {
		// Rows above the recovered store's next LSN name a lost tail.
		r.epochsErr = r.epochs.TrimAbove(store.NextLSN())
	}
	return r
}

// EpochFloor is the volume's durable epoch floor (the highest epoch it took a
// role at), read from the floor record.
func (r *replicationIngress) EpochFloor() (uint64, error) {
	if r.floorPath == "" {
		return 0, fmt.Errorf("volume %s keeps no floor record", r.volumeID)
	}
	return transport.LoadFloorRecord(r.floorPath, r.volumeID)
}

// EpochTable is the volume's epoch table (nil when it failed to load).
func (r *replicationIngress) EpochTable() *transport.EpochTable { return r.epochs }

// notePrimaryEpoch makes the primary's row (epoch, the store's next LSN)
// durable before the role is adopted: the frontend is still gated, so the
// first write at this epoch takes exactly that LSN and no entry of the epoch
// can be durable before its row. NextLSN does not sync, so adoption never
// waits on an in-flight store effect.
func (r *replicationIngress) notePrimaryEpoch(epoch uint64) error {
	if r.epochsErr != nil {
		return fmt.Errorf("volume %s epoch table: %w", r.volumeID, r.epochsErr)
	}
	if r.epochs.Invalid() {
		// Promoted (forced) over a store whose history is unknown: the table
		// becomes exactly this epoch from the next LSN, so every survivor
		// that rejoins holds an epoch this table never had and is rebuilt.
		if err := r.epochs.Replace([]transport.EpochRow{{Epoch: epoch, FirstLSN: r.store.NextLSN()}}); err != nil {
			return fmt.Errorf("volume %s epoch table: %w", r.volumeID, err)
		}
		return nil
	}
	if err := r.epochs.Note(epoch, r.store.NextLSN()); err != nil {
		return fmt.Errorf("volume %s epoch table: %w", r.volumeID, err)
	}
	return nil
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
	if r.epochsErr != nil {
		return fmt.Errorf("volume %s epoch table: %w", r.volumeID, r.epochsErr)
	}
	// The primary's row is noted after the fence has drained every inbound
	// apply (so the local sync cannot wait on one) and before the role call
	// returns: no write at this epoch is accepted before the row is durable.
	if !hasPeers {
		if r.listener != nil {
			if err := r.listener.FenceAsPrimary(epoch); err != nil {
				return err
			}
		}
		r.stopLocked(true)
		return r.notePrimaryEpoch(epoch)
	}
	if r.rf1Only != "" {
		return fmt.Errorf("volume %s refuses a primary role with replica peers at epoch %d: %s", r.volumeID, epoch, r.rf1Only)
	}
	if err := r.roleLocked(epoch, true); err != nil {
		return err
	}
	return r.notePrimaryEpoch(epoch)
}
func (r *replicationIngress) AdmitAsReplica(epoch uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("replication ingress closed: %w", volume.ErrRoleRefused)
	}
	if r.rf1Only != "" {
		return fmt.Errorf("volume %s refuses a replica role at epoch %d: %s: %w", r.volumeID, epoch, r.rf1Only, volume.ErrRoleRefused)
	}
	if r.epochsErr != nil {
		return fmt.Errorf("volume %s epoch table: %w", r.volumeID, r.epochsErr)
	}
	return r.roleLocked(epoch, false)
}

// FenceFloor is a fencing round's promise on this replica: the listener
// makes epoch its durable floor (refusing every frame and ACK below it)
// before the round is answered. Without a listener (no replica role yet)
// there is nothing to fence and no promise to give.
func (r *replicationIngress) FenceFloor(epoch uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.listener == nil {
		return fmt.Errorf("volume %s has no replica listener to fence at epoch %d", r.volumeID, epoch)
	}
	return r.listener.FenceFloor(epoch)
}

// DurableFloor is the listener's durable floor (0 without a listener).
func (r *replicationIngress) DurableFloor() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listener == nil {
		return 0
	}
	return r.listener.DurableFloor()
}

func (r *replicationIngress) roleLocked(epoch uint64, primary bool) error {
	listener := r.listener
	fresh := listener == nil
	if fresh {
		// One version map per replica store, surviving listener role transitions.
		if r.applyGate == nil {
			r.applyGate = replication.NewReplicaApplyGate(r.store)
		}
		var err error
		listener, err = transport.NewReplicaListenerWithFloorAndApplyHook(r.addr, r.store, r.volumeID, r.floorPath, r.applyGate)
		if err != nil {
			return err
		}
		listener.SetEpochTable(r.epochs)
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
