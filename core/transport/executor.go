package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// BlockExecutor implements adapter.CommandExecutor using real TCP
// transport and in-memory storage. It is the "muscle" layer — it
// executes commands but never decides policy.
type BlockExecutor struct {
	primaryStore *storage.BlockStore
	replicaAddr  string // replica's TCP address

	mu             sync.Mutex
	onSessionClose adapter.OnSessionClose
	sessions       map[uint64]*activeSession
	stepDelay      time.Duration
}

type activeSession struct {
	lineage RecoveryLineage
	cancel  chan struct{}
	conn    net.Conn
}

var errSessionInvalidated = errors.New("session invalidated")

const recoveryConnTimeout = 5 * time.Second

// NewBlockExecutor creates an executor for one primary → replica pair.
func NewBlockExecutor(primaryStore *storage.BlockStore, replicaAddr string) *BlockExecutor {
	return &BlockExecutor{
		primaryStore: primaryStore,
		replicaAddr:  replicaAddr,
		sessions:     make(map[uint64]*activeSession),
	}
}

func (e *BlockExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {
	e.onSessionClose = fn
}

// Probe dials the replica, sends a probe request, and returns the
// replica's R/S/H boundaries. Returns facts only — never decides policy.
func (e *BlockExecutor) Probe(replicaID, dataAddr, ctrlAddr string, epoch, endpointVersion uint64) adapter.ProbeResult {
	addr := e.replicaAddr
	if dataAddr != "" {
		addr = dataAddr
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("dial: %v", err),
		}
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	if err := WriteMsg(conn, MsgProbeReq, nil); err != nil {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("send probe: %v", err),
		}
	}

	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgProbeResp {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("read probe resp: %v", err),
		}
	}

	resp, err := DecodeProbeResp(payload)
	if err != nil {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("decode probe: %v", err),
		}
	}

	// Get primary's boundaries for R/S/H.
	_, primaryS, primaryH := e.primaryStore.Boundaries()

	log.Printf("executor: probe %s success R=%d S=%d H=%d",
		replicaID, resp.SyncedLSN, primaryS, primaryH)
	return adapter.ProbeResult{
		ReplicaID:         replicaID,
		Success:           true,
		EndpointVersion:   endpointVersion,
		TransportEpoch:    epoch,
		ReplicaFlushedLSN: resp.SyncedLSN, // R
		PrimaryTailLSN:    primaryS,        // S
		PrimaryHeadLSN:    primaryH,         // H
	}
}

// StartCatchUp ships WAL entries from the primary to the replica.
// Runs asynchronously — calls OnSessionClose when done.
func (e *BlockExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	lineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		TargetLSN:       targetLSN,
	}
	session, err := e.registerSession(lineage)
	if err != nil {
		return err
	}

	go func() {
		achieved, err := e.doCatchUp(session, targetLSN)
		e.finishSession(replicaID, session, achieved, err)
	}()
	return nil
}

func (e *BlockExecutor) doCatchUp(session *activeSession, targetLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("catch-up dial: %w", err)
	}
	if err := e.attachConn(session, conn); err != nil {
		_ = conn.Close()
		return 0, err
	}
	defer func() {
		e.detachConn(session, conn)
		_ = conn.Close()
	}()

	// Ship all blocks from primary store with the target LSN so the
	// replica's store advances its boundaries correctly.
	blocks := e.primaryStore.AllBlocks()
	lbas := make([]int, 0, len(blocks))
	for lba := range blocks {
		lbas = append(lbas, int(lba))
	}
	sort.Ints(lbas)
	for _, lbaInt := range lbas {
		select {
		case <-session.cancel:
			return 0, errSessionInvalidated
		default:
		}
		lba := uint32(lbaInt)
		entry := EncodeShipEntry(ShipEntry{
			Lineage: session.lineage,
			LBA:     lba,
			LSN:     targetLSN,
			Data:    blocks[lba],
		})
		if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
			return 0, fmt.Errorf("catch-up set deadline: %w", err)
		}
		if err := WriteMsg(conn, MsgShipEntry, entry); err != nil {
			return 0, fmt.Errorf("catch-up ship LBA %d: %w", lba, err)
		}
		if e.stepDelay > 0 {
			time.Sleep(e.stepDelay)
		}
	}

	// Send barrier to confirm durability. The barrier response carries
	// the replica's actual synced frontier — use THAT as achievedLSN,
	// not the intent target.
	select {
	case <-session.cancel:
		return 0, errSessionInvalidated
	default:
	}
	if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
		return 0, fmt.Errorf("catch-up set deadline: %w", err)
	}
	if err := WriteMsg(conn, MsgBarrierReq, EncodeLineage(session.lineage)); err != nil {
		return 0, fmt.Errorf("catch-up barrier: %w", err)
	}
	if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
		return 0, fmt.Errorf("catch-up set deadline: %w", err)
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
func (e *BlockExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	lineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		TargetLSN:       targetLSN,
	}
	session, err := e.registerSession(lineage)
	if err != nil {
		return err
	}

	go func() {
		achieved, err := e.doRebuild(session, targetLSN)
		e.finishSession(replicaID, session, achieved, err)
	}()
	return nil
}

func (e *BlockExecutor) doRebuild(session *activeSession, targetLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("rebuild dial: %w", err)
	}
	if err := e.attachConn(session, conn); err != nil {
		_ = conn.Close()
		return 0, err
	}
	defer func() {
		e.detachConn(session, conn)
		_ = conn.Close()
	}()

	// Send all blocks. The target is the engine's frozen targetLSN,
	// not the primary's current head. The executor must not silently
	// upgrade the target — that would violate the engine's contract.
	blocks := e.primaryStore.AllBlocks()
	lbas := make([]int, 0, len(blocks))
	for lba := range blocks {
		lbas = append(lbas, int(lba))
	}
	sort.Ints(lbas)
	for _, lbaInt := range lbas {
		select {
		case <-session.cancel:
			return 0, errSessionInvalidated
		default:
		}
		lba := uint32(lbaInt)
		payload := EncodeRebuildBlock(session.lineage, lba, blocks[lba])
		if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
			return 0, fmt.Errorf("rebuild set deadline: %w", err)
		}
		if err := WriteMsg(conn, MsgRebuildBlock, payload); err != nil {
			return 0, fmt.Errorf("rebuild block LBA %d: %w", lba, err)
		}
		if e.stepDelay > 0 {
			time.Sleep(e.stepDelay)
		}
	}

	// Send done with the engine's frozen targetLSN.
	select {
	case <-session.cancel:
		return 0, errSessionInvalidated
	default:
	}
	if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
		return 0, fmt.Errorf("rebuild set deadline: %w", err)
	}
	if err := WriteMsg(conn, MsgRebuildDone, EncodeLineage(session.lineage)); err != nil {
		return 0, fmt.Errorf("rebuild done: %w", err)
	}
	if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
		return 0, fmt.Errorf("rebuild set deadline: %w", err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgBarrierResp {
		return 0, fmt.Errorf("rebuild ack resp: %v", err)
	}
	if len(payload) < 8 {
		return 0, fmt.Errorf("rebuild ack resp: short payload")
	}
	achievedLSN := binary.BigEndian.Uint64(payload)

	log.Printf("executor: rebuild complete, sent %d blocks (targetLSN=%d)", len(blocks), targetLSN)
	return achievedLSN, nil
}

func (e *BlockExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {
	var conn net.Conn
	e.mu.Lock()
	session, ok := e.sessions[sessionID]
	if ok {
		delete(e.sessions, sessionID)
		conn = session.conn
		session.conn = nil
		close(session.cancel)
	}
	e.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	log.Printf("executor: invalidate session %d for %s: %s", sessionID, replicaID, reason)
}

func (e *BlockExecutor) PublishHealthy(replicaID string) {
	log.Printf("executor: publish healthy for %s", replicaID)
}

func (e *BlockExecutor) PublishDegraded(replicaID string, reason string) {
	log.Printf("executor: publish degraded for %s: %s", replicaID, reason)
}

func (e *BlockExecutor) registerSession(lineage RecoveryLineage) (*activeSession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.sessions[lineage.SessionID]; exists {
		return nil, fmt.Errorf("executor: session %d already active", lineage.SessionID)
	}
	session := &activeSession{
		lineage: lineage,
		cancel:  make(chan struct{}),
	}
	e.sessions[lineage.SessionID] = session
	return session, nil
}

func (e *BlockExecutor) attachConn(session *activeSession, conn net.Conn) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	current, ok := e.sessions[session.lineage.SessionID]
	if !ok || current != session {
		return errSessionInvalidated
	}
	session.conn = conn
	return nil
}

func (e *BlockExecutor) detachConn(session *activeSession, conn net.Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()

	current, ok := e.sessions[session.lineage.SessionID]
	if !ok || current != session {
		return
	}
	if session.conn == conn {
		session.conn = nil
	}
}

func (e *BlockExecutor) finishSession(replicaID string, session *activeSession, achieved uint64, err error) {
	if errors.Is(err, errSessionInvalidated) {
		return
	}

	e.mu.Lock()
	current, ok := e.sessions[session.lineage.SessionID]
	if !ok || current != session {
		e.mu.Unlock()
		return
	}
	delete(e.sessions, session.lineage.SessionID)
	cb := e.onSessionClose
	e.mu.Unlock()

	if cb == nil {
		return
	}
	if err != nil {
		cb(adapter.SessionCloseResult{
			ReplicaID:  replicaID,
			SessionID:  session.lineage.SessionID,
			Success:    false,
			FailReason: err.Error(),
		})
		return
	}
	cb(adapter.SessionCloseResult{
		ReplicaID:   replicaID,
		SessionID:   session.lineage.SessionID,
		Success:     true,
		AchievedLSN: achieved,
	})
}
