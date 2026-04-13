package transport

import (
	"encoding/binary"
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ReplicaListener accepts connections from the primary and handles
// WAL shipping, probe requests, and rebuild streams.
type ReplicaListener struct {
	store    *storage.BlockStore
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
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
			lba, data, err := DecodeRebuildBlock(payload)
			if err != nil {
				log.Printf("replica: decode rebuild block: %v", err)
				return
			}
			// Apply with LSN=1 as placeholder; the real LSN comes in
			// MsgRebuildDone to advance the frontier correctly.
			if err := r.store.ApplyEntry(lba, data, 1); err != nil {
				log.Printf("replica: apply rebuild block: %v", err)
			}

		case MsgRebuildDone:
			// The done message carries the primary's head LSN.
			// Advance the replica's frontier metadata without touching
			// any block data — AdvanceFrontier only updates nextLSN/walHead.
			if len(payload) >= 8 {
				achievedLSN := binary.BigEndian.Uint64(payload)
				r.store.AdvanceFrontier(achievedLSN)
			}
			r.store.Sync()
			return

		case MsgBarrierReq:
			frontier := r.store.Sync()
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, frontier)
			WriteMsg(conn, MsgBarrierResp, resp)

		default:
			log.Printf("replica: unknown message type 0x%02x", msgType)
			return
		}
	}
}
