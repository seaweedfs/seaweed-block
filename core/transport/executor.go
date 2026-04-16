package transport

import (
	"errors"
	"fmt"
	"log"
	"net"
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
	onSessionStart adapter.OnSessionStart
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

func (e *BlockExecutor) SetOnSessionStart(fn adapter.OnSessionStart) {
	e.onSessionStart = fn
}

func (e *BlockExecutor) SetStepDelay(d time.Duration) {
	e.mu.Lock()
	e.stepDelay = d
	e.mu.Unlock()
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

func (e *BlockExecutor) signalSessionStart(replicaID string, sessionID uint64) {
	e.mu.Lock()
	cb := e.onSessionStart
	_, ok := e.sessions[sessionID]
	e.mu.Unlock()
	if cb == nil || !ok {
		return
	}
	cb(adapter.SessionStartResult{
		ReplicaID: replicaID,
		SessionID: sessionID,
	})
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
