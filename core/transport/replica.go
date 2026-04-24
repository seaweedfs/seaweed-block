package transport

import (
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ReplicaListener accepts connections from the primary and handles
// WAL shipping, probe requests, and rebuild streams against a
// replica-side LogicalStorage.
type ReplicaListener struct {
	store    storage.LogicalStorage
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup

	mu            sync.Mutex
	activeLineage RecoveryLineage
}

// NewReplicaListener creates a listener on the given address.
func NewReplicaListener(addr string, store storage.LogicalStorage) (*ReplicaListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &ReplicaListener{
		store:    store,
		listener: ln,
		stopCh:   make(chan struct{}),
	}, nil
}

// Addr returns the listener's address.
func (r *ReplicaListener) Addr() string {
	return r.listener.Addr().String()
}

// Serve starts accepting connections.
func (r *ReplicaListener) Serve() {
	r.wg.Add(1)
	go r.acceptLoop()
}

// Stop shuts down the listener. Safe to call multiple times.
func (r *ReplicaListener) Stop() {
	select {
	case <-r.stopCh:
		return // already stopped
	default:
	}
	close(r.stopCh)
	r.listener.Close()
	r.wg.Wait()
}

func (r *ReplicaListener) acceptLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			select {
			case <-r.stopCh:
				return
			default:
				log.Printf("replica: accept error: %v", err)
				return
			}
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.handleConn(conn)
		}()
	}
}

func (r *ReplicaListener) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		msgType, payload, err := ReadMsg(conn)
		if err != nil {
			return // connection closed
		}

		switch msgType {
		case MsgShipEntry:
			entry, err := DecodeShipEntry(payload)
			if err != nil {
				log.Printf("replica: decode ship entry: %v", err)
				return
			}
			if !r.acceptMutationLineage(entry.Lineage) {
				log.Printf("replica: reject stale ship session=%d epoch=%d endpointVersion=%d",
					entry.Lineage.SessionID, entry.Lineage.Epoch, entry.Lineage.EndpointVersion)
				return
			}
			if err := r.store.ApplyEntry(entry.LBA, entry.Data, entry.LSN); err != nil {
				log.Printf("replica: apply entry: %v", err)
			}

		case MsgProbeReq:
			R, S, H := r.store.Boundaries()
			resp := EncodeProbeResp(ProbeResponse{
				SyncedLSN: R,
				WalTail:   S,
				WalHead:   H,
			})
			if err := WriteMsg(conn, MsgProbeResp, resp); err != nil {
				return
			}

		case MsgRebuildBlock:
			lineage, lba, data, err := DecodeRebuildBlock(payload)
			if err != nil {
				log.Printf("replica: decode rebuild block: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale rebuild block session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			// Rebuild blocks carry the engine's frozen targetLSN in their
			// lineage. Apply that real LSN immediately so any future
			// LSN-aware ApplyEntry guard still treats rebuild data as current.
			if err := r.store.ApplyEntry(lba, data, lineage.TargetLSN); err != nil {
				log.Printf("replica: apply rebuild block: %v", err)
			}

		case MsgRebuildDone:
			lineage, err := DecodeLineage(payload)
			if err != nil {
				log.Printf("replica: decode rebuild done: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale rebuild done session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			// The done message carries the engine's frozen target LSN.
			// Advance the replica's frontier metadata without touching
			// any block data — AdvanceFrontier only updates nextLSN/walHead.
			r.store.AdvanceFrontier(lineage.TargetLSN)
			frontier, _ := r.store.Sync()
			// Echo the request's full lineage in the rebuild-done ack
			// per T4b-1 wire extension. T4b scope does not yet validate
			// this lineage on the primary (catch-up/rebuild paths are
			// T4c / T5), but the wire must already carry it so those
			// validators can consume it when they land.
			resp := EncodeBarrierResp(BarrierResponse{
				Lineage:     lineage,
				AchievedLSN: frontier,
			})
			if err := WriteMsg(conn, MsgBarrierResp, resp); err != nil {
				return
			}
			return

		case MsgBarrierReq:
			lineage, err := DecodeLineage(payload)
			if err != nil {
				log.Printf("replica: decode barrier req: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale barrier session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			frontier, _ := r.store.Sync()
			// Echo the request's full lineage in the barrier ack per
			// T4b-1 wire extension (round-21 uniform rule + H5 LOCK).
			// The primary's DurabilityCoordinator (T4b-2/T4b-3)
			// validates this tuple against the session it is awaiting
			// before counting the ack toward quorum.
			resp := EncodeBarrierResp(BarrierResponse{
				Lineage:     lineage,
				AchievedLSN: frontier,
			})
			WriteMsg(conn, MsgBarrierResp, resp)

		default:
			log.Printf("replica: unknown message type 0x%02x", msgType)
			return
		}
	}
}

func (r *ReplicaListener) acceptMutationLineage(incoming RecoveryLineage) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if incoming.SessionID == 0 || incoming.Epoch == 0 || incoming.EndpointVersion == 0 {
		return false
	}

	active := r.activeLineage
	if active.SessionID == 0 {
		r.activeLineage = incoming
		return true
	}

	switch compareLineage(incoming, active) {
	case 1:
		r.activeLineage = incoming
		return true
	case 0:
		return incoming.TargetLSN == active.TargetLSN
	default:
		return false
	}
}

func compareLineage(a, b RecoveryLineage) int {
	switch {
	case a.Epoch < b.Epoch:
		return -1
	case a.Epoch > b.Epoch:
		return 1
	}
	switch {
	case a.EndpointVersion < b.EndpointVersion:
		return -1
	case a.EndpointVersion > b.EndpointVersion:
		return 1
	}
	switch {
	case a.SessionID < b.SessionID:
		return -1
	case a.SessionID > b.SessionID:
		return 1
	default:
		return 0
	}
}
