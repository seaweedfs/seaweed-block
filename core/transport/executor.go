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
// transport and a primary-side LogicalStorage. It is the "muscle"
// layer — it executes commands but never decides policy.
type BlockExecutor struct {
	primaryStore storage.LogicalStorage
	replicaAddr  string // replica's TCP address

	mu               sync.Mutex
	onSessionStart   adapter.OnSessionStart
	onSessionClose   adapter.OnSessionClose
	onFenceComplete  adapter.OnFenceComplete
	sessions         map[uint64]*activeSession
	stepDelay        time.Duration
}

type activeSession struct {
	lineage RecoveryLineage
	cancel  chan struct{}
	conn    net.Conn
}

var errSessionInvalidated = errors.New("session invalidated")

const recoveryConnTimeout = 5 * time.Second

// NewBlockExecutor creates an executor for one primary -> replica pair.
func NewBlockExecutor(primaryStore storage.LogicalStorage, replicaAddr string) *BlockExecutor {
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

func (e *BlockExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) {
	e.onFenceComplete = fn
}

// Fence sends one MsgBarrierReq at the given lineage to the
// replica and awaits MsgBarrierResp. Does NOT ship any blocks and
// does NOT go through the session registry — fence is a single
// barrier exchange, not a recovery session.
//
// On success or failure, fires the registered OnFenceComplete
// callback exactly once. Retry is the adapter/engine's
// responsibility (probe-driven); this method never retries.
func (e *BlockExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	lineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		// TargetLSN is not semantically meaningful for fence (fence
		// doesn't declare a recovery target), but post-T4b-1 the
		// barrier-ack wire demands every lineage field be non-zero
		// (architect round-21 uniform rule applied at decode). Using
		// the liveShipTargetLSN sentinel (1) keeps Fence composable
		// with the stricter wire while preserving the "not a recovery
		// target" intent — no TargetLSN-sensitive code in the fence
		// path reads this value.
		TargetLSN: fenceSentinelTargetLSN,
	}
	go e.doFence(replicaID, lineage)
	return nil
}

// fenceSentinelTargetLSN is the lineage.TargetLSN value used for
// fence requests. Arbitrary non-zero value; matches the
// liveShipTargetLSN convention on the peer-level live ship path.
// See Fence() godoc for why this is non-zero.
const fenceSentinelTargetLSN uint64 = 1

func (e *BlockExecutor) doFence(replicaID string, lineage RecoveryLineage) {
	result := adapter.FenceResult{
		ReplicaID:       replicaID,
		SessionID:       lineage.SessionID,
		Epoch:           lineage.Epoch,
		EndpointVersion: lineage.EndpointVersion,
	}

	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		result.Success = false
		result.FailReason = fmt.Sprintf("fence dial: %v", err)
		e.fireFenceComplete(result)
		return
	}
	defer conn.Close()

	if err := sendBarrierReq(conn, lineage, recoveryConnTimeout); err != nil {
		result.Success = false
		result.FailReason = fmt.Sprintf("fence barrier send: %v", err)
		e.fireFenceComplete(result)
		return
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		// Short / zero-valued / field-order-malformed payloads are
		// rejected at the T4b-1 decode layer as errors, so they
		// surface here and fail the fence — NOT a silent accept.
		// Covers the uniform-rule failure class at the first
		// existing consumer of the extended BarrierResponse wire
		// (architect round 22).
		result.Success = false
		result.FailReason = fmt.Sprintf("fence barrier resp: %v", err)
		e.fireFenceComplete(result)
		return
	}
	// T4b-2 round-22 lineage validation: the replica's echoed lineage
	// MUST match the lineage we sent. This is the "valid-decode but
	// wrong-session" case — decode was fine, but the ack is for a
	// different authority tuple than the one we're awaiting.
	// AchievedLSN is still not semantically meaningful for fence;
	// the validation is purely about confirming the replica acked
	// OUR request, not some concurrent / stale one.
	if resp.Lineage != lineage {
		result.Success = false
		result.FailReason = fmt.Sprintf("fence barrier resp lineage mismatch: expected=%+v actual=%+v",
			lineage, resp.Lineage)
		log.Printf("executor: fence lineage mismatch replica=%s expected=%+v actual=%+v",
			replicaID, lineage, resp.Lineage)
		e.fireFenceComplete(result)
		return
	}

	result.Success = true
	log.Printf("executor: fence complete replica=%s epoch=%d", replicaID, lineage.Epoch)
	e.fireFenceComplete(result)
}

// Barrier issues a per-peer barrier round-trip carrying the session
// lineage. Blocks until the response arrives or the deadline fires.
// Returns the validated BarrierAck on success, or error on transport
// failure / ack rejection.
//
// Architect round-21 uniform rule applies end-to-end:
//   - decode rejects short / zeroed / field-order-malformed acks
//     (T4b-1 wire)
//   - this method rejects valid-decode-but-wrong-session acks via
//     ErrBarrierLineageMismatch (T4b-2 ack-consumer validation)
// A future optimization MUST NOT weaken this to epoch-only; the
// full-lineage rule is load-bearing for H5 LOCK.
//
// Called by: DurabilityCoordinator.SyncLocalAndReplicas (T4b-3)
// per-peer fan-out.
// Owns: per-call conn deadline (recoveryConnTimeout); lineage binding;
// ack validation against the session's registered lineage.
// Borrows: session by replicaID + lineage.SessionID; BarrierAck
// payload fields are value-copied into the returned struct.
func (e *BlockExecutor) Barrier(replicaID string, lineage RecoveryLineage, targetLSN uint64) (BarrierAck, error) {
	// Session lookup + epoch-== fence (same pattern as Ship).
	e.mu.Lock()
	session, ok := e.sessions[lineage.SessionID]
	if !ok || session == nil {
		e.mu.Unlock()
		return BarrierAck{}, fmt.Errorf("transport: barrier: no session for replica=%s sessionID=%d",
			replicaID, lineage.SessionID)
	}
	if lineage.Epoch != session.lineage.Epoch {
		sessionEpoch := session.lineage.Epoch
		e.mu.Unlock()
		log.Printf("transport: barrier: dropping targetLSN=%d replica=%s stale epoch %d (session epoch %d)",
			targetLSN, replicaID, lineage.Epoch, sessionEpoch)
		return BarrierAck{}, fmt.Errorf("transport: barrier: stale epoch %d (session %d)",
			lineage.Epoch, sessionEpoch)
	}
	conn := session.conn
	e.mu.Unlock()

	// Lazy-dial if no conn (same pattern as Ship, architect round-11
	// Option B). Attach-under-mu with "first winner keeps" races.
	if conn == nil {
		dialed, err := net.DialTimeout("tcp", e.replicaAddr, shipDialTimeout)
		if err != nil {
			return BarrierAck{}, fmt.Errorf("transport: barrier: dial replica=%s sessionID=%d: %w",
				replicaID, lineage.SessionID, err)
		}
		e.mu.Lock()
		current, stillActive := e.sessions[lineage.SessionID]
		switch {
		case !stillActive || current != session:
			e.mu.Unlock()
			_ = dialed.Close()
			return BarrierAck{}, fmt.Errorf("transport: barrier: session %d invalidated during dial",
				lineage.SessionID)
		case session.conn != nil:
			conn = session.conn
			e.mu.Unlock()
			_ = dialed.Close()
		default:
			session.conn = dialed
			conn = dialed
			e.mu.Unlock()
		}
	}

	// Send request + receive response with deadlines on both sides
	// (sendBarrierReq / recvBarrierResp already set conn deadline).
	if err := sendBarrierReq(conn, lineage, recoveryConnTimeout); err != nil {
		return BarrierAck{}, fmt.Errorf("transport: barrier: send replica=%s: %w", replicaID, err)
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		return BarrierAck{}, fmt.Errorf("transport: barrier: recv replica=%s: %w", replicaID, err)
	}

	// Full-lineage validation (architect round-21 uniform rule).
	// Stale or cross-session acks fail here. Diagnostic log format
	// matches architect's round-21 text: peer ID + full expected /
	// actual lineage tuple.
	if resp.Lineage != lineage {
		log.Printf("transport: barrier: lineage mismatch replica=%s expected=%+v actual=%+v",
			replicaID, lineage, resp.Lineage)
		return BarrierAck{}, ErrBarrierLineageMismatch
	}

	return BarrierAck{
		Lineage:     resp.Lineage,
		AchievedLSN: resp.AchievedLSN,
		Success:     true,
	}, nil
}

func (e *BlockExecutor) fireFenceComplete(result adapter.FenceResult) {
	e.mu.Lock()
	cb := e.onFenceComplete
	e.mu.Unlock()
	if cb != nil {
		cb(result)
	}
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
