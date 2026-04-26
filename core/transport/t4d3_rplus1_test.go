package transport

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// runCatchUpFrom drives StartCatchUp with explicit fromLSN +
// targetLSN; T4d-3 tests need this control over the standard
// runCatchUp helper which hardcodes fromLSN=1.
func runCatchUpFrom(t *testing.T, exec *BlockExecutor, fromLSN, targetLSN uint64) adapter.SessionCloseResult {
	t.Helper()
	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartCatchUp("r1", 1, 1, 1, fromLSN, targetLSN); err != nil {
		t.Fatal(err)
	}
	select {
	case <-startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for start")
	}
	select {
	case r := <-closeCh:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for close")
	}
	return adapter.SessionCloseResult{}
}

// T4d-3 transport-side tests per G-1 §7 test parity matrix.

// observedScanStore wraps a substrate to count ScanLBAs invocations
// and capture the fromLSN argument for assertion.
type observedScanStore struct {
	storage.LogicalStorage
	fromLSNs []uint64
	emitted  atomic.Int32
}

func (o *observedScanStore) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	o.fromLSNs = append(o.fromLSNs, fromLSN)
	intercept := func(e storage.RecoveryEntry) error {
		if err := fn(e); err != nil {
			return err
		}
		o.emitted.Add(1)
		return nil
	}
	return o.LogicalStorage.ScanLBAs(fromLSN, intercept)
}

// TestT4d3_CatchUp_ScansFromReplicaR_NotGenesis pins
// INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1 at the
// transport surface: when StartCatchUp is called with fromLSN=51,
// the substrate's ScanLBAs is invoked with 51, NOT 1.
func TestT4d3_CatchUp_ScansFromReplicaR_NotGenesis(t *testing.T) {
	primaryRaw, _, listener := setupPrimaryReplica(t)
	// Wrap to intercept fromLSN.
	primary := &observedScanStore{LogicalStorage: primaryRaw}
	for i := uint32(0); i < 10; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		primaryRaw.Write(i, data)
	}
	primaryRaw.Sync()

	exec := NewBlockExecutor(primary, listener.Addr())
	// Catch-up with fromLSN=51 (simulating engine emit Recovery.R=50 + 1).
	runCatchUpFrom(t, exec, 51, 100)

	if len(primary.fromLSNs) == 0 {
		t.Fatal("ScanLBAs never called")
	}
	if primary.fromLSNs[0] != 51 {
		t.Errorf("ScanLBAs[0] called with fromLSN=%d, want 51 (caller-supplied; sender does NOT add +1)",
			primary.fromLSNs[0])
	}
}

// TestT4d3_CatchUp_StartCatchUpSignature_FromLSNRequired — compile-
// time fence: signature must include fromLSN. Tests the call site
// shape; if a future change removes the parameter, this fails to
// compile.
func TestT4d3_CatchUp_StartCatchUpSignature_FromLSNRequired(t *testing.T) {
	primary, _, listener := setupPrimaryReplica(t)
	exec := NewBlockExecutor(primary, listener.Addr())
	// Compile fence: 6-arg StartCatchUp signature.
	err := exec.StartCatchUp("r1", 1, 1, 1, 1, 100)
	_ = err // outcome irrelevant; signature is the pin
}

// TestT4d3_CatchUp_NonEmptyReplica_BlockStoreOverShipsExpected pins
// the §9 BlockStore documented limitation as an EXPECTED behavior:
// BlockStore's synthesis is fromLSN-agnostic (always emits all
// stored LBAs regardless of fromLSN). This regression fence
// surfaces immediately if BlockStore behavior ever changes — the
// bandwidth-narrative tests for production substrates would silently
// no-op against BlockStore; this opposing-assertion test prevents
// that.
//
// Per round-46 QA ADDITION 2: substrate-bound naming prevents the
// silent-failure mode where a future test refactor swaps walstore→
// BlockStore in the matrix and the bandwidth assertion silently
// passes a no-op test.
func TestT4d3_CatchUp_NonEmptyReplica_BlockStoreOverShipsExpected(t *testing.T) {
	primaryRaw, _, listener := setupPrimaryReplica(t)
	primary := &observedScanStore{LogicalStorage: primaryRaw}
	// Seed 5 LBAs.
	for i := uint32(0); i < 5; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		primaryRaw.Write(i, data)
	}
	primaryRaw.Sync()

	exec := NewBlockExecutor(primary, listener.Addr())
	// Catch-up with fromLSN=4 (replica supposedly has LSNs 1..3
	// already; needs 4..5). On a real fromLSN-honoring substrate,
	// emit count would be ~2. On BlockStore (synthesis), all 5
	// LBAs ship.
	runCatchUpFrom(t, exec, 4, 5)

	emitted := primary.emitted.Load()
	if emitted < 5 {
		t.Fatalf("BlockStore over-ship limitation: expected >= 5 emissions (BlockStore is fromLSN-agnostic per §9 caveat); got %d. If this fails, BlockStore behavior changed — verify production substrates' fromLSN honoring still holds at %s",
			emitted, "core/storage/store.go")
	}
}
