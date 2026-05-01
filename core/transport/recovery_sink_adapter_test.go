package transport

// P2c-B-2 follow-up — RecoverySink adapter integration tests for
// architect P1 review (2026-04-29) rules 1+2.
//
// Scope (Option 2 guardrail per architect ratification):
//   - Lifecycle ordering only: emit context set BEFORE StartSession
//     (rule 1); emit context restored AFTER EndSession (rule 2).
//   - NO MsgShipEntry vs frameWALEntry branching. Tests use a
//     recording EmitFunc that captures (conn, lineage) at emit
//     time without decoding wire bytes. Wire-body format is gated
//     by P2d.
//
// Verification approach:
//   - Replace the per-replica WalShipperEntry's shipper with a fresh
//     one whose EmitFunc records (conn, lineage) into a slice. The
//     recording EmitFunc reads emitConn/emitLineage from the entry
//     under emitCtxMu — same pattern as production EmitFunc — so
//     recorded contexts reflect the at-emit-time state.
//   - Drive the adapter through StartSession / DrainBacklog /
//     NotifyAppend / EndSession; assert recorded contexts.

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// recordedEmit captures one emit observation from the test's
// recording EmitFunc. Intentionally records (conn, lineage, profile,
// kind, lba, lsn) but NOT data — the test pins lifecycle ordering,
// not wire format.
type recordedEmit struct {
	conn    net.Conn
	lineage RecoveryLineage
	profile EmitProfile
	kind    EmitKind
	lba     uint32
	lsn     uint64
}

// installRecordingShipper swaps the per-replica WalShipperEntry's
// shipper for a fresh one whose EmitFunc records emit observations.
// The recording EmitFunc reads emitConn/emitLineage from the entry
// under emitCtxMu — same lock discipline as production — so the
// captured context reflects what was active at the moment of emit.
//
// Returns a pointer to a slice the test consumes; callers must
// hold the test's mutex while reading.
//
// TEST-ONLY / single-goroutine: the entry.shipper assignment below
// does NOT acquire walShipperMu. Production paths (WalShipperFor,
// updateWalShipperEmitContext) take that mutex; this helper bypasses
// it deliberately because tests run before any concurrent access.
// Do not lift this pattern into production code or concurrent test
// scenarios without re-introducing the lock.
func installRecordingShipper(t *testing.T, e *BlockExecutor, replicaID string) (*[]recordedEmit, *sync.Mutex) {
	t.Helper()

	// Ensure entry exists.
	_ = e.WalShipperFor(replicaID)

	e.walShipperMu.Lock()
	entry, ok := e.walShippers[replicaID]
	e.walShipperMu.Unlock()
	if !ok {
		t.Fatalf("walShipperEntry for %q not found after WalShipperFor", replicaID)
	}

	var recMu sync.Mutex
	recorded := []recordedEmit{}

	rec := func(kind EmitKind, lba uint32, lsn uint64, data []byte) error {
		entry.emitCtxMu.Lock()
		conn := entry.emitConn
		lineage := entry.emitLineage
		profile := entry.emitProfile
		entry.emitCtxMu.Unlock()
		recMu.Lock()
		recorded = append(recorded, recordedEmit{
			conn: conn, lineage: lineage, profile: profile,
			kind: kind, lba: lba, lsn: lsn,
		})
		recMu.Unlock()
		return nil
	}

	// Replace the shipper with a fresh one wired to the recording
	// EmitFunc. Activate to Realtime so subsequent NotifyAppend
	// behaves like production.
	s := NewWalShipper(replicaID, HeadSourceFromStorage(e.primaryStore), e.primaryStore, rec)
	if err := s.Activate(0); err != nil {
		t.Fatalf("Activate recording shipper: %v", err)
	}
	entry.shipper = s

	return &recorded, &recMu
}

// readEntryEmitContext reads the per-replica entry's emitConn /
// emitLineage under the same mutex production code uses. White-box
// helper for the post-condition checks.
func readEntryEmitContext(t *testing.T, e *BlockExecutor, replicaID string) (net.Conn, RecoveryLineage) {
	t.Helper()
	e.walShipperMu.Lock()
	entry, ok := e.walShippers[replicaID]
	e.walShipperMu.Unlock()
	if !ok {
		t.Fatalf("walShipperEntry for %q not found", replicaID)
	}
	entry.emitCtxMu.Lock()
	defer entry.emitCtxMu.Unlock()
	return entry.emitConn, entry.emitLineage
}

// TestRecoverySink_StartSession_InstallsSessionContext — direct
// post-condition: after StartSession returns, the per-replica
// emitConn/emitLineage are the session values. This is the
// rule-1 surface (set BEFORE StartSession) checked at the only
// observable boundary post-call.
func TestRecoverySink_StartSession_InstallsSessionContext(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 42, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
	}

	// Pre-state: install steady context. This mimics Ship() having
	// been the previous user of the shipper.
	_ = e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	pre, preLin := readEntryEmitContext(t, e, replicaID)
	if pre != steadyConn || preLin != steadyLineage {
		t.Fatalf("pre-state context not steady: conn=%p lineage=%+v", pre, preLin)
	}

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(50); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	got, gotLin := readEntryEmitContext(t, e, replicaID)
	if got != sessionConn {
		t.Errorf("after StartSession: emit conn=%p want sessionConn=%p", got, sessionConn)
	}
	if gotLin != sessionLineage {
		t.Errorf("after StartSession: emit lineage=%+v want %+v", gotLin, sessionLineage)
	}

	// WalShipper must be in Backlog mode.
	if mode := e.WalShipperFor(replicaID).Mode(); mode != ModeBacklog {
		t.Errorf("WalShipper mode=%s want Backlog", mode)
	}
}

// TestRecoverySink_DrainBacklogEmits_UseSessionContext — rule 1
// indirect proof. Seed the substrate with WAL entries; run
// StartSession + DrainBacklog. Every recorded emit MUST have the
// session lineage. If rule 1 were violated (context updated AFTER
// StartSession), the first few drain emits would record the steady
// (or empty) lineage instead.
func TestRecoverySink_DrainBacklogEmits_UseSessionContext(t *testing.T) {
	primary := memorywal.NewStore(16, 64)
	// Seed substrate with 5 WAL entries.
	for lba := uint32(0); lba < 5; lba++ {
		buf := bytes.Repeat([]byte{byte(0xA0 + lba)}, 64)
		if _, err := primary.Write(lba, buf); err != nil {
			t.Fatalf("seed Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("seed Sync: %v", err)
	}

	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	// Install recording shipper FIRST (so the recording EmitFunc
	// is in place when StartSession transitions to Backlog).
	recorded, recMu := installRecordingShipper(t, e, replicaID)

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 99, Epoch: 2, EndpointVersion: 3, TargetLSN: 1000,
	}
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sink.DrainBacklog(ctx); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}

	recMu.Lock()
	defer recMu.Unlock()

	if len(*recorded) == 0 {
		t.Fatal("no emits recorded — DrainBacklog did not pump anything")
	}

	for i, r := range *recorded {
		if r.conn != sessionConn {
			t.Errorf("emit %d: conn=%p want sessionConn=%p (rule 1 violated: emit ran under stale context)",
				i, r.conn, sessionConn)
		}
		if r.lineage != sessionLineage {
			t.Errorf("emit %d: lineage=%+v want %+v (rule 1 violated)",
				i, r.lineage, sessionLineage)
		}
	}
	t.Logf("rule 1 confirmed across %d backlog emits", len(*recorded))
}

// TestRecoverySink_EndSession_RestoresSteadyContext — rule 2 direct
// post-condition. After EndSession, emit context MUST be the
// steady values, not the session values.
func TestRecoverySink_EndSession_RestoresSteadyContext(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 42, Epoch: 1, EndpointVersion: 1, TargetLSN: 200,
	}

	_ = e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Confirm session context active mid-session.
	mid, midLin := readEntryEmitContext(t, e, replicaID)
	if mid != sessionConn || midLin != sessionLineage {
		t.Fatalf("mid-session context wrong: conn=%p lineage=%+v", mid, midLin)
	}

	sink.EndSession()

	got, gotLin := readEntryEmitContext(t, e, replicaID)
	if got != steadyConn {
		t.Errorf("after EndSession: emit conn=%p want steadyConn=%p (rule 2 violated)",
			got, steadyConn)
	}
	if gotLin != steadyLineage {
		t.Errorf("after EndSession: emit lineage=%+v want %+v (rule 2 violated)",
			gotLin, steadyLineage)
	}

	// WalShipper transitioned back to Realtime.
	if mode := e.WalShipperFor(replicaID).Mode(); mode != ModeRealtime {
		t.Errorf("WalShipper mode=%s want Realtime after EndSession", mode)
	}
}

// TestRecoverySink_NotifyAppendAfterEndSession_UsesSteadyContext —
// rule 2 indirect proof. After EndSession, a NotifyAppend that
// emits (Realtime mode) MUST capture the steady context at emit
// time, not the session context.
func TestRecoverySink_NotifyAppendAfterEndSession_UsesSteadyContext(t *testing.T) {
	primary := memorywal.NewStore(8, 64)
	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	recorded, recMu := installRecordingShipper(t, e, replicaID)

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 42, Epoch: 1, EndpointVersion: 1, TargetLSN: 200,
	}

	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if err := sink.DrainBacklog(context.Background()); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}
	sink.EndSession()

	// Post-EndSession: sink itself is sealed (rejects NotifyAppend
	// with ErrSinkSealed — verified separately in
	// TestRecoverySink_NotifyAppendAfterEndSession_Sealed). The
	// rule-2 architectural assertion is "emit context restored to
	// steady" which is independent of sink seal — verify by calling
	// the underlying WalShipper directly so the recording EmitFunc
	// captures the post-EndSession context state.
	shipper := e.WalShipperFor(replicaID)
	// §6.3 migration: lsn = cursor+1 so fast-path tail-emit fires.
	// Post-EndSession cursor is wherever DrainBacklog left it (0 for
	// empty substrate). Test's claim is rule-2 context restore, not
	// LSN-sequence semantics — distinct LSN value not required.
	postLSN := shipper.Cursor() + 1
	if err := shipper.NotifyAppend(7, postLSN, []byte{0xCC}); err != nil {
		t.Fatalf("WalShipper.NotifyAppend post-EndSession: %v", err)
	}

	recMu.Lock()
	defer recMu.Unlock()

	if len(*recorded) == 0 {
		t.Fatal("no emits recorded")
	}
	last := (*recorded)[len(*recorded)-1]
	if last.lba != 7 || last.lsn != postLSN {
		t.Fatalf("last emit unexpected: lba=%d lsn=%d (want lba=7 lsn=%d)", last.lba, last.lsn, postLSN)
	}
	if last.conn != steadyConn {
		t.Errorf("post-EndSession emit: conn=%p want steadyConn=%p (rule 2 violated)",
			last.conn, steadyConn)
	}
	if last.lineage != steadyLineage {
		t.Errorf("post-EndSession emit: lineage=%+v want %+v (rule 2 violated)",
			last.lineage, steadyLineage)
	}
}

// TestRecoverySink_FullLifecycle — end-to-end ordering: across
// the full StartSession → DrainBacklog → EndSession bracket plus
// post-session NotifyAppend, every recorded emit must use the
// expected context for its phase. Backlog drain emits → session
// context. Post-EndSession Realtime emit → steady context.
func TestRecoverySink_FullLifecycle(t *testing.T) {
	primary := memorywal.NewStore(16, 64)
	for lba := uint32(0); lba < 3; lba++ {
		buf := bytes.Repeat([]byte{byte(0xB0 + lba)}, 64)
		if _, err := primary.Write(lba, buf); err != nil {
			t.Fatalf("seed Write: %v", err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("seed Sync: %v", err)
	}

	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	recorded, recMu := installRecordingShipper(t, e, replicaID)

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 99, Epoch: 5, EndpointVersion: 7, TargetLSN: 5000,
	}

	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if err := sink.DrainBacklog(context.Background()); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}

	// Snapshot count of session-phase emits.
	recMu.Lock()
	sessionEmitCount := len(*recorded)
	recMu.Unlock()
	if sessionEmitCount == 0 {
		t.Fatal("expected at least one backlog emit during session")
	}

	sink.EndSession()

	// Post-session Realtime emit. Sink is sealed; bypass it and
	// call the underlying WalShipper directly to verify the post-
	// EndSession emit context is the restored steady context.
	shipper := e.WalShipperFor(replicaID)
	// §6.3 migration: lsn = cursor+1 so fast-path fires.
	postLSN := shipper.Cursor() + 1
	if err := shipper.NotifyAppend(15, postLSN, []byte{0xFF}); err != nil {
		t.Fatalf("WalShipper.NotifyAppend post-EndSession: %v", err)
	}

	recMu.Lock()
	defer recMu.Unlock()

	totalEmits := len(*recorded)
	if totalEmits <= sessionEmitCount {
		t.Fatalf("expected post-session emit to be recorded (total=%d session-phase=%d)",
			totalEmits, sessionEmitCount)
	}

	// Session-phase emits (indices [0, sessionEmitCount)) → session ctx.
	for i := 0; i < sessionEmitCount; i++ {
		r := (*recorded)[i]
		if r.conn != sessionConn || r.lineage != sessionLineage {
			t.Errorf("session-phase emit %d: conn=%p lineage=%+v want session", i, r.conn, r.lineage)
		}
	}
	// Post-session emits (indices [sessionEmitCount, totalEmits)) → steady ctx.
	for i := sessionEmitCount; i < totalEmits; i++ {
		r := (*recorded)[i]
		if r.conn != steadyConn || r.lineage != steadyLineage {
			t.Errorf("post-session emit %d: conn=%p lineage=%+v want steady", i, r.conn, r.lineage)
		}
	}

	t.Logf("full lifecycle: %d session-phase emits + %d post-session emits, all contexts correct",
		sessionEmitCount, totalEmits-sessionEmitCount)
}

// TestRecoverySink_NotifyAppendAfterEndSession_Sealed — post-
// EndSession the sink itself rejects NotifyAppend with ErrSinkSealed.
// Mirrors senderBacklogSink's post-flushAndSeal contract so callers
// (PrimaryBridge.PushLiveWrite) get a clean "session is closing"
// signal — without this gate, races between EndSession and bridge-
// map-removal would produce confusing "no conn" errors from the
// underlying WalShipper.
func TestRecoverySink_NotifyAppendAfterEndSession_Sealed(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")
	const replicaID = "r1"

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 42, Epoch: 1, EndpointVersion: 1, TargetLSN: 200,
	}

	_ = e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	sink.EndSession()

	// Post-EndSession NotifyAppend MUST return ErrSinkSealed.
	err := sink.NotifyAppend(0, 1, []byte{0x42})
	if err == nil {
		t.Fatal("post-EndSession NotifyAppend: want error, got nil")
	}
	if !errors.Is(err, ErrSinkSealed) {
		t.Errorf("post-EndSession NotifyAppend err=%v want ErrSinkSealed", err)
	}
}

// TestRecoverySink_SatisfiesWalShipperSink — compile-time check
// that *RecoverySink implements the recovery.WalShipperSink
// interface by duck typing. The interface lives in `recovery`
// (not `transport`) to avoid an import cycle; this test confirms
// our method set matches.
//
// We can't directly assert against recovery.WalShipperSink without
// importing recovery (which would create a cycle if recovery ever
// imports transport — currently it doesn't, but the duck-typing
// design preserves the option). Instead we re-declare the
// interface locally for the assertion.
func TestRecoverySink_SatisfiesWalShipperSink(t *testing.T) {
	type walShipperSinkShape interface {
		StartSession(fromLSN uint64) error
		DrainBacklog(ctx context.Context) error
		EndSession()
		NotifyAppend(lba uint32, lsn uint64, data []byte) error
	}
	var _ walShipperSinkShape = (*RecoverySink)(nil)
}
