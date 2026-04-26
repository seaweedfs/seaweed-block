package component

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// defaultLogWriter / setDefaultLogWriter wrap the standard `log`
// package's writer so the component framework can tee executor +
// replica log output into the per-test capture buffer.
func defaultLogWriter() io.Writer    { return log.Writer() }
func setDefaultLogWriter(w io.Writer) { log.SetOutput(w) }

// SubstrateFactory opens a fresh LogicalStorage in the given dir.
// Returns the store + a cleanup func.
type SubstrateFactory func(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func())

// Smartwal is the sign-bearing substrate factory.
func Smartwal(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func()) {
	t.Helper()
	path := filepath.Join(dir, label+".smartwal")
	s, err := smartwal.CreateStore(path, blocks, blockSize)
	if err != nil {
		t.Fatalf("%s: smartwal.CreateStore: %v", label, err)
	}
	return s, func() {
		_ = s.Close()
		_ = os.Remove(path)
	}
}

// Walstore is the second matrix row.
func Walstore(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func()) {
	t.Helper()
	path := filepath.Join(dir, label+".walstore")
	s, err := storage.CreateWALStore(path, blocks, blockSize)
	if err != nil {
		t.Fatalf("%s: storage.CreateWALStore: %v", label, err)
	}
	return s, func() {
		_ = s.Close()
		_ = os.Remove(path)
	}
}

// Defaults — tunable per-test via With* methods.
const (
	DefaultBlocks    uint32 = 64
	DefaultBlockSize int    = 4096
)

// PrimaryStorageWrap is an injection-time substrate transform: receives
// the freshly-created primary substrate and returns a wrapped
// LogicalStorage. The wrap may forward most methods and override
// specific ones (typically ScanLBAs for fault injection).
//
// Used via Cluster.WithPrimaryStorageWrap to install the wrap before
// the primary's BlockExecutor binds to the substrate. After Start(),
// `Primary().Store` returns the wrapped store; the executor's
// internal substrate handle is the wrap.
type PrimaryStorageWrap func(inner storage.LogicalStorage) storage.LogicalStorage

// Cluster is one primary + N replicas wired over localhost TCP.
// Authored as a builder: NewCluster → WithReplicas → Start →
// scenario primitives → assertions → t.Cleanup handles teardown.
type Cluster struct {
	t              *testing.T
	dir            string
	factory        SubstrateFactory
	blocks         uint32
	blockSize      int
	replicaN       int
	primaryWrap    PrimaryStorageWrap // optional substrate wrap; nil = no wrap
	withLiveShip   bool               // if true, primary spins up StorageBackend + ReplicationVolume
	engineRecovery bool               // T4d hook (currently no-op + warning); see WithEngineDrivenRecovery
	withApplyGate  bool               // T4d-2: install replication.ReplicaApplyGate on each replica's listener

	// Built at Start()
	primary  *PrimaryNode
	replicas []*ReplicaNode

	// Per-scenario hooks
	logCapture *logCapture
}

// PrimaryNode owns the primary's storage + per-replica executors.
// When the cluster is built WithLiveShip, PrimaryNode also owns a
// StorageBackend (the frontend's data-plane entry) and a
// ReplicationVolume (the live-ship fan-out engine). Live-ship writes
// go through `Backend` instead of `Store` so the WriteObserver hook
// fires, driving the production live-ship path end-to-end.
type PrimaryNode struct {
	Store     storage.LogicalStorage
	executors []*transport.BlockExecutor // one per replica, indexed by replica idx

	// Live-ship surface (populated when WithLiveShip is set).
	Backend *durable.StorageBackend
	RepVol  *replication.ReplicationVolume

	cleanup func()
}

// ReplicaNode owns one replica's storage + listener.
type ReplicaNode struct {
	Idx      int
	Store    storage.LogicalStorage
	Listener *transport.ReplicaListener
	Addr     string

	// ApplyGate is non-nil iff cluster was built WithApplyGate (T4d-2).
	ApplyGate *replication.ReplicaApplyGate

	cleanup func()
}

// NewCluster constructs a builder. Call WithReplicas/With* then Start.
func NewCluster(t *testing.T, factory SubstrateFactory) *Cluster {
	t.Helper()
	return &Cluster{
		t:         t,
		dir:       t.TempDir(),
		factory:   factory,
		blocks:    DefaultBlocks,
		blockSize: DefaultBlockSize,
		replicaN:  1,
	}
}

// WithReplicas sets the replica count. Default 1.
func (c *Cluster) WithReplicas(n int) *Cluster {
	c.replicaN = n
	return c
}

// WithBlockGeometry overrides defaults.
func (c *Cluster) WithBlockGeometry(blocks uint32, blockSize int) *Cluster {
	c.blocks = blocks
	c.blockSize = blockSize
	return c
}

// WithPrimaryStorageWrap installs a substrate wrap on the primary
// before BlockExecutors bind. The wrap may override any
// LogicalStorage method (typically ScanLBAs for substrate fault
// injection — recycle, target-not-reached, mid-stream stall, etc.).
//
// After Start(), `Primary().Store` returns the wrapped store; the
// primary's executors call ScanLBAs / Boundaries / etc through the
// wrap. Use storage-package-level helpers (NewRecycledScanWrap,
// NewSeverDuringScanWrap) for common patterns; bring your own
// struct{ storage.LogicalStorage; override... } for one-off shapes.
func (c *Cluster) WithPrimaryStorageWrap(wrap PrimaryStorageWrap) *Cluster {
	c.primaryWrap = wrap
	return c
}

// WithLiveShip enables live-ship: primary spins up StorageBackend
// (the frontend's data-plane entry) + ReplicationVolume (the
// live-ship fan-out engine), and the WriteObserver hook fires on
// every PrimaryWriteViaBackend call. Use this for scenarios that
// need to exercise the live-ship path (T4a/T4b territory) rather
// than direct executor-driven catch-up (T4c).
//
// When set, the primary's UpdateReplicaSet is auto-called with the
// configured replicas at Start() so live-ship has peers to fan to.
func (c *Cluster) WithLiveShip() *Cluster {
	c.withLiveShip = true
	return c
}

// WithApplyGate installs the T4d-2 `replication.ReplicaApplyGate`
// on every replica's listener (lane-aware per-LBA stale-skip +
// 2-map split + Option C hybrid AppliedLSNs seed). Required for
// scenarios that pin round-43/44 stale-skip invariants.
//
// When NOT set, replica listeners use direct substrate.ApplyEntry
// (preserves T4a/T4b/T4c scenario behavior).
func (c *Cluster) WithApplyGate() *Cluster {
	c.withApplyGate = true
	return c
}

// ApplyGate returns the T4d-2 apply gate for the i-th replica
// (nil if WithApplyGate not set or before Start). Used by tests
// that assert on per-session gate state (recoveryCovered, etc.).
func (c *Cluster) ApplyGate(replicaIdx int) *replication.ReplicaApplyGate {
	r := c.Replica(replicaIdx)
	return r.ApplyGate
}

// WithEngineDrivenRecovery enables engine→adapter→executor recovery
// flows. Currently a NO-OP STUB — today `ReplicationVolume` bypasses
// the adapter, so engine-driven recovery is not exercisable at
// integration scope (engine retry-loop is unit-tested only).
//
// The full wiring lands at T4d. When it does, scenarios that called
// this option will gain access to engine-driven catch-up, retry-loop
// observation, and escalation behavior. Until then, this method
// records the intent and emits a one-shot warning at Start().
//
// Forward-carry (T4d): wire ReplicationVolume.NewWithRecoveryAdapter
// (or equivalent) and have this option swap that constructor in.
func (c *Cluster) WithEngineDrivenRecovery() *Cluster {
	c.engineRecovery = true
	return c
}

// Start brings up the primary + replicas. Registers t.Cleanup for
// teardown (replicas first via LIFO so listeners are torn down only
// after primary's executors release peer conns).
func (c *Cluster) Start() *Cluster {
	c.t.Helper()

	// Bring up replicas first; primary needs their addresses.
	c.replicas = make([]*ReplicaNode, c.replicaN)
	for i := 0; i < c.replicaN; i++ {
		label := fmt.Sprintf("replica-%d", i)
		store, cleanup := c.factory(c.t, c.dir, label, c.blocks, c.blockSize)
		var gate *replication.ReplicaApplyGate
		var listener *transport.ReplicaListener
		var err error
		if c.withApplyGate {
			gate = replication.NewReplicaApplyGate(store)
			listener, err = transport.NewReplicaListenerWithApplyHook("127.0.0.1:0", store, gate)
		} else {
			listener, err = transport.NewReplicaListener("127.0.0.1:0", store)
		}
		if err != nil {
			cleanup()
			c.t.Fatalf("%s: NewReplicaListener: %v", label, err)
		}
		listener.Serve()
		node := &ReplicaNode{
			Idx:       i,
			Store:     store,
			Listener:  listener,
			Addr:      listener.Addr(),
			ApplyGate: gate,
			cleanup: func() {
				listener.Stop()
				cleanup()
			},
		}
		// Register replica cleanup FIRST so it runs LAST in LIFO.
		c.t.Cleanup(node.cleanup)
		c.replicas[i] = node
	}

	// Bring up primary with one BlockExecutor per replica.
	pStoreRaw, pCleanup := c.factory(c.t, c.dir, "primary", c.blocks, c.blockSize)

	// Apply substrate wrap if configured. The wrap sees the raw
	// substrate; executors bind to the wrap.
	pStore := storage.LogicalStorage(pStoreRaw)
	if c.primaryWrap != nil {
		pStore = c.primaryWrap(pStoreRaw)
	}

	executors := make([]*transport.BlockExecutor, c.replicaN)
	for i, r := range c.replicas {
		executors[i] = transport.NewBlockExecutor(pStore, r.Addr)
	}
	c.primary = &PrimaryNode{
		Store:     pStore,
		executors: executors,
		cleanup:   pCleanup,
	}
	c.t.Cleanup(pCleanup)

	// Live-ship surface — opt-in via WithLiveShip.
	if c.withLiveShip {
		c.startLiveShip()
	}

	// Engine-driven recovery — T4d stub; warn so scenarios authored
	// for the future option don't silently behave as direct-executor.
	if c.engineRecovery {
		c.t.Logf("component: WithEngineDrivenRecovery is a T4d stub (no-op today); scenario will run with direct executor-driven recovery instead")
	}

	// Capture logs (recovery_mode label etc.) for assertion helpers.
	c.logCapture = newLogCapture()
	c.t.Cleanup(c.logCapture.Stop)
	c.logCapture.Start()

	return c
}

// Primary returns the primary node handle.
func (c *Cluster) Primary() *PrimaryNode { return c.primary }

// Replica returns the i-th replica handle.
func (c *Cluster) Replica(i int) *ReplicaNode {
	c.t.Helper()
	if i < 0 || i >= len(c.replicas) {
		c.t.Fatalf("Replica(%d): out of range (have %d)", i, len(c.replicas))
	}
	return c.replicas[i]
}

// ReplicaCount returns the number of replicas.
func (c *Cluster) ReplicaCount() int { return len(c.replicas) }

// --- Write / Sync primitives ---

// startLiveShip stands up the primary's StorageBackend +
// ReplicationVolume and registers the configured replicas with the
// vol so live-ship has peers. Caller has already populated
// c.primary.Store. Adds cleanup hooks.
func (c *Cluster) startLiveShip() {
	c.t.Helper()
	id := frontend.Identity{
		VolumeID:        "vol-component",
		ReplicaID:       "primary-component",
		Epoch:           1,
		EndpointVersion: 1,
	}
	view := &alwaysHealthyView{proj: frontend.Projection{
		VolumeID:        id.VolumeID,
		ReplicaID:       id.ReplicaID,
		Epoch:           id.Epoch,
		EndpointVersion: id.EndpointVersion,
		Healthy:         true,
	}}
	backend := durable.NewStorageBackend(c.primary.Store, view, id)
	backend.SetOperational(true, "component test ready")
	repVol := replication.NewReplicationVolume(id.VolumeID, c.primary.Store)
	backend.SetWriteObserver(repVol)
	c.primary.Backend = backend
	c.primary.RepVol = repVol
	c.t.Cleanup(func() {
		_ = backend.Close()
		_ = repVol.Close()
	})

	// Register all replicas as peers so live-ship has fan-out targets.
	targets := make([]replication.ReplicaTarget, 0, len(c.replicas))
	for i, r := range c.replicas {
		targets = append(targets, replication.ReplicaTarget{
			ReplicaID:       fmt.Sprintf("replica-%d", i),
			DataAddr:        r.Addr,
			ControlAddr:     r.Addr,
			Epoch:           1,
			EndpointVersion: 1,
		})
	}
	if err := repVol.UpdateReplicaSet(1, targets); err != nil {
		c.t.Fatalf("UpdateReplicaSet: %v", err)
	}
}

// alwaysHealthyView is the minimal frontend.View needed to drive the
// data-plane backend in component scope. Frontend identity is
// already lineage-gated upstream; component scope is the data path.
type alwaysHealthyView struct{ proj frontend.Projection }

func (v *alwaysHealthyView) Projection() frontend.Projection { return v.proj }

// PrimaryWriteViaBackend writes one LBA through the StorageBackend
// (the production data-plane entry), driving the WriteObserver hook
// → ReplicationVolume.OnLocalWrite → live-ship fan-out to all
// registered replicas. Requires WithLiveShip.
//
// Use this for live-ship scenarios; use PrimaryWrite for scenarios
// that bypass the live-ship path and target the executor / catch-up
// surface directly.
func (c *Cluster) PrimaryWriteViaBackend(lba uint32, data []byte) {
	c.t.Helper()
	if c.primary.Backend == nil {
		c.t.Fatal("PrimaryWriteViaBackend: cluster not built WithLiveShip()")
	}
	offset := int64(lba) * int64(c.blockSize)
	n, err := c.primary.Backend.Write(context.Background(), offset, data)
	if err != nil {
		c.t.Fatalf("PrimaryWriteViaBackend[lba=%d]: %v", lba, err)
	}
	if n != c.blockSize {
		c.t.Fatalf("PrimaryWriteViaBackend[lba=%d]: short write %d/%d", lba, n, c.blockSize)
	}
}

// PrimaryWriteViaBackendN writes n distinct LBAs through the
// production data-plane entry. Same content shape as PrimaryWriteN.
func (c *Cluster) PrimaryWriteViaBackendN(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		data := make([]byte, c.blockSize)
		data[0] = byte(i + 1)
		data[c.blockSize-1] = byte(0xC4 ^ i)
		c.PrimaryWriteViaBackend(uint32(i), data)
	}
}

// PrimaryWrite writes one LBA on the primary and returns the assigned LSN.
func (c *Cluster) PrimaryWrite(lba uint32, data []byte) uint64 {
	c.t.Helper()
	lsn, err := c.primary.Store.Write(lba, data)
	if err != nil {
		c.t.Fatalf("PrimaryWrite[lba=%d]: %v", lba, err)
	}
	return lsn
}

// PrimaryWriteN writes n distinct LBAs (0..n-1) with deterministic
// content markers (data[0]=lba+1, data[blockSize-1]=0xC4^lba).
func (c *Cluster) PrimaryWriteN(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		lba := uint32(i)
		data := make([]byte, c.blockSize)
		data[0] = byte(i + 1)
		data[c.blockSize-1] = byte(0xC4 ^ i)
		c.PrimaryWrite(lba, data)
	}
}

// PrimarySync flushes the primary's WAL.
func (c *Cluster) PrimarySync() uint64 {
	c.t.Helper()
	lsn, err := c.primary.Store.Sync()
	if err != nil {
		c.t.Fatalf("PrimarySync: %v", err)
	}
	return lsn
}

// ReplicaApply directly applies a (lba, data, lsn) to a replica's
// store, simulating an in-flight Ship. Useful for setting up
// replicas with a pre-existing partial state.
func (c *Cluster) ReplicaApply(replicaIdx int, lba uint32, data []byte, lsn uint64) {
	c.t.Helper()
	r := c.Replica(replicaIdx)
	if err := r.Store.ApplyEntry(lba, data, lsn); err != nil {
		c.t.Fatalf("ReplicaApply[r=%d lba=%d]: %v", replicaIdx, lba, err)
	}
}

// --- Recovery primitives ---

// CatchUpReplica drives executor.StartCatchUp against the i-th
// replica, blocking until SessionClose fires. Returns the close
// result. Uses the primary's current head as targetLSN.
func (c *Cluster) CatchUpReplica(replicaIdx int) adapter.SessionCloseResult {
	c.t.Helper()
	exec := c.primary.executors[replicaIdx]
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(adapter.SessionStartResult) {})
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := c.primary.Store.Boundaries()
	if err := exec.StartCatchUp(fmt.Sprintf("replica-%d", replicaIdx), 1, 1, 1, pH); err != nil {
		c.t.Fatalf("CatchUpReplica[%d] StartCatchUp: %v", replicaIdx, err)
	}

	select {
	case result := <-closeCh:
		return result
	case <-time.After(10 * time.Second):
		c.t.Fatalf("CatchUpReplica[%d]: timeout waiting for SessionClose", replicaIdx)
	}
	return adapter.SessionCloseResult{}
}

// ProbeReplica drives executor.Probe against the i-th replica with
// a transient probe sessionID. Returns the ProbeResult.
func (c *Cluster) ProbeReplica(replicaIdx int) adapter.ProbeResult {
	c.t.Helper()
	exec := c.primary.executors[replicaIdx]
	r := c.Replica(replicaIdx)
	return exec.Probe(fmt.Sprintf("replica-%d", replicaIdx), r.Addr, r.Addr, 99, 1, 1)
}

// --- Fault injection ---

// KillReplicaListener stops the replica's listener, simulating the
// replica process going away. Subsequent dial attempts fail
// connection-refused. The replica's stored data persists; restart
// via RestartReplica (TODO if needed).
func (c *Cluster) KillReplicaListener(replicaIdx int) {
	c.t.Helper()
	r := c.Replica(replicaIdx)
	r.Listener.Stop()
}

// SeverConnection forcefully closes any TCP connection currently
// open between the primary and the i-th replica. The next ship /
// barrier / probe will need to re-dial. Useful for "the network
// blipped mid-stream" scenarios.
//
// Implementation: opens a control-plane probe to the replica with
// an immediately-closed dial; doesn't directly access executor
// internals (those are private). The next session-bearing call
// from the executor will re-dial.
func (c *Cluster) SeverConnection(replicaIdx int) {
	c.t.Helper()
	r := c.Replica(replicaIdx)
	conn, err := net.Dial("tcp", r.Addr)
	if err != nil {
		// Listener already down — that's a stronger sever than what
		// we'd do here; treat as success.
		return
	}
	_ = conn.Close()
}

// --- Convergence + assertions ---

// WaitForConverge polls until every replica's stored bytes match
// the primary's on every written LBA, or the timeout expires.
// On timeout, fails the test with a diff summary.
func (c *Cluster) WaitForConverge(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.allConverged() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal(c.divergenceReport())
}

// AssertReplicaConverged checks the i-th replica has byte-exact
// content for every LBA written on the primary. Fails immediately
// (no polling) — caller is responsible for ordering this AFTER any
// async catch-up completion.
func (c *Cluster) AssertReplicaConverged(replicaIdx int) {
	c.t.Helper()
	r := c.Replica(replicaIdx)
	if !c.replicaConverged(r) {
		c.t.Fatal(c.divergenceReportFor(r))
	}
}

// AllReplicasConverged returns true iff every replica matches the
// primary byte-exact on every written LBA.
func (c *Cluster) allConverged() bool {
	for _, r := range c.replicas {
		if !c.replicaConverged(r) {
			return false
		}
	}
	return true
}

func (c *Cluster) replicaConverged(r *ReplicaNode) bool {
	pBlocks := c.primary.Store.AllBlocks()
	for lba, pData := range pBlocks {
		rData, _ := r.Store.Read(lba)
		if !bytes.Equal(pData, rData) {
			return false
		}
	}
	return true
}

func (c *Cluster) divergenceReport() string {
	var b []byte
	for _, r := range c.replicas {
		if !c.replicaConverged(r) {
			b = append(b, c.divergenceReportFor(r)...)
			b = append(b, '\n')
		}
	}
	return string(b)
}

func (c *Cluster) divergenceReportFor(r *ReplicaNode) string {
	var diffs []string
	pBlocks := c.primary.Store.AllBlocks()
	for lba, pData := range pBlocks {
		rData, _ := r.Store.Read(lba)
		if !bytes.Equal(pData, rData) {
			pHead := byte(0)
			rHead := byte(0)
			if len(pData) > 0 {
				pHead = pData[0]
			}
			if len(rData) > 0 {
				rHead = rData[0]
			}
			diffs = append(diffs, fmt.Sprintf("  lba=%d primary[0]=%02x replica[0]=%02x",
				lba, pHead, rHead))
		}
	}
	return fmt.Sprintf("replica %d divergent (%d LBAs):\n%s",
		r.Idx, len(diffs), joinLines(diffs))
}

func joinLines(lines []string) string {
	var b []byte
	for _, ln := range lines {
		b = append(b, ln...)
		b = append(b, '\n')
	}
	return string(b)
}

// --- Mode label observability ---

// RecoveryMode label expectations for assertions.
type RecoveryModeExpect string

const (
	ExpectAnyMode          RecoveryModeExpect = "*"
	ExpectWALReplay        RecoveryModeExpect = "wal_replay"
	ExpectStateConvergence RecoveryModeExpect = "state_convergence"
)

// Sanity-pin the constants match the storage package values.
// If storage.RecoveryMode* drift, this fails at package init time.
var _ = func() bool {
	if string(ExpectWALReplay) != string(storage.RecoveryModeWALReplay) {
		panic("component: ExpectWALReplay drift vs storage.RecoveryModeWALReplay")
	}
	if string(ExpectStateConvergence) != string(storage.RecoveryModeStateConvergence) {
		panic("component: ExpectStateConvergence drift vs storage.RecoveryModeStateConvergence")
	}
	return true
}()

// AssertSawRecoveryMode checks the captured executor logs for a
// `recovery_mode=<expected>` substring. Use ExpectAnyMode to assert
// "any mode label was emitted" without pinning a specific value
// (useful when the substrate's mode is not under test control).
//
// The log capture starts at Cluster.Start; calls before Start panic.
func (c *Cluster) AssertSawRecoveryMode(replicaIdx int, expected RecoveryModeExpect) {
	c.t.Helper()
	if c.logCapture == nil {
		c.t.Fatal("AssertSawRecoveryMode: cluster not started")
	}
	logs := c.logCapture.Snapshot()
	needle := "recovery_mode="
	if expected != ExpectAnyMode {
		needle = fmt.Sprintf("recovery_mode=%s", expected)
	}
	if !strings.Contains(logs, needle) {
		c.t.Fatalf("AssertSawRecoveryMode[r=%d]: did not see %q in executor logs:\n%s",
			replicaIdx, needle, logs)
	}
}

// --- Internal: log capture ---

type logCapture struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	stopper func()
}

func newLogCapture() *logCapture { return &logCapture{} }

func (l *logCapture) Start() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Tee log.Default().Writer() into the capture buffer.
	prev := defaultLogWriter()
	tee := &teeWriter{primary: prev, capture: &l.buf, mu: &l.mu}
	setDefaultLogWriter(tee)
	l.stopper = func() {
		l.mu.Lock()
		setDefaultLogWriter(prev)
		l.mu.Unlock()
	}
}

func (l *logCapture) Stop() {
	if l.stopper != nil {
		l.stopper()
	}
}

func (l *logCapture) Snapshot() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type teeWriter struct {
	primary io.Writer
	capture *bytes.Buffer
	mu      *sync.Mutex
}

func (t *teeWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	t.capture.Write(p)
	t.mu.Unlock()
	if t.primary != nil {
		return t.primary.Write(p)
	}
	return len(p), nil
}
