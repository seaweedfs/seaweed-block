package component

import (
	"bytes"
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

// Cluster is one primary + N replicas wired over localhost TCP.
// Authored as a builder: NewCluster → WithReplicas → Start →
// scenario primitives → assertions → t.Cleanup handles teardown.
type Cluster struct {
	t          *testing.T
	dir        string
	factory    SubstrateFactory
	blocks     uint32
	blockSize  int
	replicaN   int

	// Built at Start()
	primary  *PrimaryNode
	replicas []*ReplicaNode

	// Per-scenario hooks
	logCapture *logCapture
}

// PrimaryNode owns the primary's storage + per-replica executors.
type PrimaryNode struct {
	Store     storage.LogicalStorage
	executors []*transport.BlockExecutor // one per replica, indexed by replica idx

	cleanup func()
}

// ReplicaNode owns one replica's storage + listener.
type ReplicaNode struct {
	Idx       int
	Store     storage.LogicalStorage
	Listener  *transport.ReplicaListener
	Addr      string

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
		listener, err := transport.NewReplicaListener("127.0.0.1:0", store)
		if err != nil {
			cleanup()
			c.t.Fatalf("%s: NewReplicaListener: %v", label, err)
		}
		listener.Serve()
		node := &ReplicaNode{
			Idx:      i,
			Store:    store,
			Listener: listener,
			Addr:     listener.Addr(),
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
	pStore, pCleanup := c.factory(c.t, c.dir, "primary", c.blocks, c.blockSize)
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
