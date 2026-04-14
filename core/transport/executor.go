package transport

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// BlockExecutor implements adapter.CommandExecutor using real TCP
// transport and in-memory storage. It is the "muscle" layer — it
// executes commands but never decides policy.
type BlockExecutor struct {
	primaryStore    *storage.BlockStore
	replicaAddr     string // replica's TCP address
	endpointVersion uint64 // must match the assignment's endpoint version

	onSessionClose adapter.OnSessionClose
}

// NewBlockExecutor creates an executor for one primary → replica pair.
// endpointVersion must match the assignment's endpoint version so probes
// are not rejected as stale by the engine.
func NewBlockExecutor(primaryStore *storage.BlockStore, replicaAddr string, endpointVersion uint64) *BlockExecutor {
	return &BlockExecutor{
		primaryStore:    primaryStore,
		replicaAddr:     replicaAddr,
		endpointVersion: endpointVersion,
	}
}

func (e *BlockExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {
	e.onSessionClose = fn
}

// Probe dials the replica, sends a probe request, and returns the
// replica's R/S/H boundaries. Returns facts only — never decides policy.
func (e *BlockExecutor) Probe(replicaID, dataAddr, ctrlAddr string) adapter.ProbeResult {
	addr := e.replicaAddr
	if dataAddr != "" {
		addr = dataAddr
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return adapter.ProbeResult{
			ReplicaID:  replicaID,
			Success:    false,
			FailReason: fmt.Sprintf("dial: %v", err),
		}
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	if err := WriteMsg(conn, MsgProbeReq, nil); err != nil {
		return adapter.ProbeResult{
			ReplicaID:  replicaID,
			Success:    false,
			FailReason: fmt.Sprintf("send probe: %v", err),
		}
	}

	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgProbeResp {
		return adapter.ProbeResult{
			ReplicaID:  replicaID,
			Success:    false,
			FailReason: fmt.Sprintf("read probe resp: %v", err),
		}
	}

	resp, err := DecodeProbeResp(payload)
	if err != nil {
		return adapter.ProbeResult{
			ReplicaID:  replicaID,
			Success:    false,
			FailReason: fmt.Sprintf("decode probe: %v", err),
		}
	}

	// Get primary's boundaries for R/S/H.
	_, primaryS, primaryH := e.primaryStore.Boundaries()

	log.Printf("executor: probe %s success R=%d S=%d H=%d",
		replicaID, resp.SyncedLSN, primaryS, primaryH)
	return adapter.ProbeResult{
		ReplicaID:         replicaID,
		Success:           true,
		EndpointVersion:   e.endpointVersion, // must match assignment
		TransportEpoch:    e.endpointVersion,
		ReplicaFlushedLSN: resp.SyncedLSN, // R
		PrimaryTailLSN:    primaryS,        // S
		PrimaryHeadLSN:    primaryH,         // H
	}
}

// StartCatchUp ships WAL entries from the primary to the replica.
// Runs asynchronously — calls OnSessionClose when done.
func (e *BlockExecutor) StartCatchUp(replicaID string, sessionID uint64, targetLSN uint64) error {
	go func() {
		achieved, err := e.doCatchUp(targetLSN)
		if e.onSessionClose != nil {
			if err != nil {
				e.onSessionClose(adapter.SessionCloseResult{
					ReplicaID:  replicaID,
					SessionID:  sessionID,
					Success:    false,
					FailReason: err.Error(),
				})
			} else {
				e.onSessionClose(adapter.SessionCloseResult{
					ReplicaID:   replicaID,
					SessionID:   sessionID,
					Success:     true,
					AchievedLSN: achieved,
				})
			}
		}
	}()
	return nil
}

func (e *BlockExecutor) doCatchUp(targetLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("catch-up dial: %w", err)
	}
	defer conn.Close()

	// Ship all blocks from primary store with the target LSN so the
	// replica's store advances its boundaries correctly.
	blocks := e.primaryStore.AllBlocks()
	for lba, data := range blocks {
		entry := EncodeShipEntry(ShipEntry{LBA: lba, LSN: targetLSN, Data: data})
		if err := WriteMsg(conn, MsgShipEntry, entry); err != nil {
			return 0, fmt.Errorf("catch-up ship LBA %d: %w", lba, err)
		}
	}

	// Send barrier to confirm durability. The barrier response carries
	// the replica's actual synced frontier — use THAT as achievedLSN,
	// not the intent target.
	if err := WriteMsg(conn, MsgBarrierReq, nil); err != nil {
		return 0, fmt.Errorf("catch-up barrier: %w", err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgBarrierResp {
		return 0, fmt.Errorf("catch-up barrier resp: %v", err)
	}
	achievedLSN := binary.BigEndian.Uint64(payload)
	log.Printf("executor: catch-up complete, replica synced to %d", achievedLSN)
	return achievedLSN, nil
}

// StartRebuild streams all base blocks from primary to replica.
// Runs asynchronously — calls OnSessionClose when done.
func (e *BlockExecutor) StartRebuild(replicaID string, sessionID uint64, targetLSN uint64) error {
	go func() {
		achieved, err := e.doRebuild(targetLSN)
		if e.onSessionClose != nil {
			if err != nil {
				e.onSessionClose(adapter.SessionCloseResult{
					ReplicaID:  replicaID,
					SessionID:  sessionID,
					Success:    false,
					FailReason: err.Error(),
				})
			} else {
				e.onSessionClose(adapter.SessionCloseResult{
					ReplicaID:   replicaID,
					SessionID:   sessionID,
					Success:     true,
					AchievedLSN: achieved,
				})
			}
		}
	}()
	return nil
}

func (e *BlockExecutor) doRebuild(targetLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("rebuild dial: %w", err)
	}
	defer conn.Close()

	// Send all blocks. The target is the engine's frozen targetLSN,
	// not the primary's current head. The executor must not silently
	// upgrade the target — that would violate the engine's contract.
	blocks := e.primaryStore.AllBlocks()
	for lba, data := range blocks {
		payload := EncodeRebuildBlock(lba, data)
		if err := WriteMsg(conn, MsgRebuildBlock, payload); err != nil {
			return 0, fmt.Errorf("rebuild block LBA %d: %w", lba, err)
		}
	}

	// Send done with the engine's frozen targetLSN.
	doneBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(doneBuf, targetLSN)
	if err := WriteMsg(conn, MsgRebuildDone, doneBuf); err != nil {
		return 0, fmt.Errorf("rebuild done: %w", err)
	}

	// Wait for replica ack. Rebuild is not complete until the replica has
	// actually applied the blocks, called AdvanceFrontier, and Sync'd. The
	// ack carries the replica's real synced frontier; returning before this
	// would let OnSessionClose fire while the replica is still catching up,
	// producing a race where callers see "success" on a not-yet-applied
	// store. Mirrors the catch-up barrier contract.
	msgType, ackPayload, err := ReadMsg(conn)
	if err != nil {
		return 0, fmt.Errorf("rebuild ack: %w", err)
	}
	if msgType != MsgBarrierResp {
		return 0, fmt.Errorf("rebuild ack: unexpected message type 0x%02x", msgType)
	}
	if len(ackPayload) < 8 {
		return 0, fmt.Errorf("rebuild ack: short payload (%d)", len(ackPayload))
	}
	replicaFrontier := binary.BigEndian.Uint64(ackPayload)

	log.Printf("executor: rebuild complete, sent %d blocks (targetLSN=%d, replicaFrontier=%d)",
		len(blocks), targetLSN, replicaFrontier)
	// Report the replica's actual frontier as achievedLSN — this is the
	// truthful durability boundary, not the primary's intent.
	return replicaFrontier, nil
}

func (e *BlockExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {
	log.Printf("executor: invalidate session %d for %s: %s", sessionID, replicaID, reason)
}

func (e *BlockExecutor) PublishHealthy(replicaID string) {
	log.Printf("executor: publish healthy for %s", replicaID)
}

func (e *BlockExecutor) PublishDegraded(replicaID string, reason string) {
	log.Printf("executor: publish degraded for %s: %s", replicaID, reason)
}
