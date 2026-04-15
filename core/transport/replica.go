package transport

import (
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ReplicaListener accepts connections from the primary and serves
// the replica side of the data-sync institution: WAL ship apply,
// probe response, rebuild block + done apply, and barrier exchange.
//
// One activeLineage tracks the most-authoritative recovery lineage
// the replica has currently admitted; mutations are gated through
// acceptMutationLineage so stale or out-of-order traffic fails
// closed at the data plane without ever touching the store.
type ReplicaListener struct {
	store    *storage.BlockStore
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup

	mu            sync.Mutex
	activeLineage RecoveryLineage
}

// NewReplicaListener creates a listener on the given address.
func NewReplicaListener(addr string, store *storage.BlockStore) (*ReplicaListener, error) {
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

// handleConn dispatches one frame at a time to a per-message-type
// handler. Each handler returns true if the connection should keep
// reading the next frame, false if the conn must be closed (decode
// failure, lineage rejection, or terminal MsgRebuildDone).
func (r *ReplicaListener) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		msgType, payload, err := ReadMsg(conn)
		if err != nil {
			return // connection closed
		}

		var keepGoing bool
		switch msgType {
		case MsgShipEntry:
			keepGoing = r.handleShipEntry(payload)
		case MsgProbeReq:
			keepGoing = r.handleProbeReq(conn)
		case MsgRebuildBlock:
			keepGoing = r.handleRebuildBlock(payload)
		case MsgRebuildDone:
			r.handleRebuildDone(conn, payload)
			return // RebuildDone is terminal — close the conn
		case MsgBarrierReq:
			keepGoing = r.handleBarrierReq(conn, payload)
		default:
			log.Printf("replica: unknown message type 0x%02x", msgType)
			return
		}
		if !keepGoing {
			return
		}
	}
}

// handleShipEntry applies one MsgShipEntry frame. Returns false if
// decode or lineage check fails — the connection must be closed
// without partial application.
func (r *ReplicaListener) handleShipEntry(payload []byte) bool {
	entry, err := DecodeShipEntry(payload)
	if err != nil {
		log.Printf("replica: decode ship entry: %v", err)
		return false
	}
	if !r.acceptMutationLineage(entry.Lineage) {
		log.Printf("replica: reject stale ship session=%d epoch=%d endpointVersion=%d",
			entry.Lineage.SessionID, entry.Lineage.Epoch, entry.Lineage.EndpointVersion)
		return false
	}
	if err := r.store.ApplyEntry(entry.LBA, entry.Data, entry.LSN); err != nil {
		log.Printf("replica: apply entry: %v", err)
	}
	return true
}

// handleProbeReq replies with the replica's R/S/H boundaries. Probe
// is observation only — no lineage check, no state mutation.
func (r *ReplicaListener) handleProbeReq(conn net.Conn) bool {
	R, S, H := r.store.Boundaries()
	resp := EncodeProbeResp(ProbeResponse{
		SyncedLSN: R,
		WalTail:   S,
		WalHead:   H,
	})
	if err := WriteMsg(conn, MsgProbeResp, resp); err != nil {
		return false
	}
	return true
}

// handleRebuildBlock applies one MsgRebuildBlock frame. Rebuild
// blocks carry the engine's frozen targetLSN in their lineage; we
// apply that real LSN immediately so any future LSN-aware
// ApplyEntry guard still treats rebuild data as current.
func (r *ReplicaListener) handleRebuildBlock(payload []byte) bool {
	lineage, lba, data, err := DecodeRebuildBlock(payload)
	if err != nil {
		log.Printf("replica: decode rebuild block: %v", err)
		return false
	}
	if !r.acceptMutationLineage(lineage) {
		log.Printf("replica: reject stale rebuild block session=%d epoch=%d endpointVersion=%d",
			lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
		return false
	}
	if err := r.store.ApplyEntry(lba, data, lineage.TargetLSN); err != nil {
		log.Printf("replica: apply rebuild block: %v", err)
	}
	return true
}

// handleRebuildDone is the terminal frame of a rebuild stream. It
// advances the replica's frontier to the engine's target, syncs,
// and replies with the achieved frontier as a typed BarrierResponse.
// The achieved value is what the store reports — never the target
// the primary asked for.
func (r *ReplicaListener) handleRebuildDone(conn net.Conn, payload []byte) {
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
	r.store.AdvanceFrontier(lineage.TargetLSN)
	frontier, _ := r.store.Sync()
	if err := WriteMsg(conn, MsgBarrierResp, EncodeBarrierResp(BarrierResponse{AchievedLSN: frontier})); err != nil {
		log.Printf("replica: write rebuild barrier resp: %v", err)
	}
}

// handleBarrierReq syncs the store and replies with the achieved
// frontier as a typed BarrierResponse. Used by catch-up to confirm
// durability of the entries that just shipped.
func (r *ReplicaListener) handleBarrierReq(conn net.Conn, payload []byte) bool {
	lineage, err := DecodeLineage(payload)
	if err != nil {
		log.Printf("replica: decode barrier req: %v", err)
		return false
	}
	if !r.acceptMutationLineage(lineage) {
		log.Printf("replica: reject stale barrier session=%d epoch=%d endpointVersion=%d",
			lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
		return false
	}
	frontier, _ := r.store.Sync()
	if err := WriteMsg(conn, MsgBarrierResp, EncodeBarrierResp(BarrierResponse{AchievedLSN: frontier})); err != nil {
		return false
	}
	return true
}

// acceptMutationLineage gates every state-changing frame. Returns
// true when incoming lineage is at least as authoritative as the
// currently active one; updates the active lineage on a strict
// upgrade. Zero-valued fields are rejected as defense-in-depth —
// the engine ingress also enforces this, but a wire-layer guard
// keeps the replica honest even against a malformed sender.
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

// compareLineage orders two lineages by semantic authority:
// epoch first, then endpointVersion, then sessionID. Returns 1 if
// a is strictly more authoritative, -1 if b is, 0 if equal in all
// three fields (TargetLSN is NOT part of authority — same lineage
// must agree on target).
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
