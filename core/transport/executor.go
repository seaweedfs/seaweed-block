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
//
// The executor hosts the session lifecycle (register / attach conn /
// invalidate / finish) and the probe path. The two byte-movement
// paths — catch-up and rebuild — live in catchup_sender.go and
// rebuild_sender.go respectively, so each protocol's wire shape and
// terminator stay distinct. The barrier exchange and typed
// BarrierResponse live in barrier.go.
type BlockExecutor struct {
	primaryStore *storage.BlockStore
	replicaAddr  string // replica's TCP address

	mu             sync.Mutex
	onSessionClose adapter.OnSessionClose
	sessions       map[uint64]*activeSession
	stepDelay      time.Duration
}

// activeSession is the executor's per-recovery handle. The cancel
// channel unblocks goroutines parked in select; the conn pointer
// lets InvalidateSession close in-flight I/O so a parked ReadMsg
// returns immediately instead of waiting for TCP timeout.
type activeSession struct {
	lineage RecoveryLineage
	cancel  chan struct{}
	conn    net.Conn
}

var errSessionInvalidated = errors.New("session invalidated")

// recoveryConnTimeout bounds every send/receive on a recovery conn.
// Small enough that a silent peer cannot park the sender for more
// than a few seconds; large enough that a healthy peer always meets
// it on a single block exchange.
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
// replica's R/S/H boundaries. Returns facts only — never decides
// policy. Probe is observation, not byte movement: it carries no
// lineage and never mutates replica state.
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
		PrimaryTailLSN:    primaryS,       // S
		PrimaryHeadLSN:    primaryH,       // H
	}
}

// InvalidateSession cancels the session and closes its conn so any
// goroutine parked in ReadMsg unblocks immediately. The matching
// finishSession call sees the session removed from the map and skips
// the close callback — by the time invalidation reaches the executor
// the engine has already moved on, so a delayed callback would race
// with whatever recovery decision the engine made next.
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

// registerSession reserves a session slot under mu. Returns an error
// if the same SessionID is already in flight — duplicates indicate a
// caller bug, not a transient condition, so we fail fast.
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

// attachConn binds a live conn to a session under mu. Returns
// errSessionInvalidated if the session was removed between
// registerSession and attachConn — in that case the caller closes
// the conn immediately and returns without sending anything.
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

// detachConn clears the conn pointer when the sender finishes its
// own work, so InvalidateSession after a clean finish doesn't try
// to close an already-closed conn.
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

// finishSession is the single completion path for catch-up and
// rebuild. Drops the close callback if the session was invalidated
// (the engine already moved on) or if a different session pointer
// occupies the slot (shouldn't happen with monotonic SessionIDs but
// the check is cheap and prevents a stale callback from racing a
// fresh one).
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
