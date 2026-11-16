package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// The storage wrapper parks an actual wire-dispatched effect AFTER admission.
// It embeds the real storage implementation; no listener/gate is mocked.
type ingressParkedStore struct {
	storage.LogicalStorage
	operation                 string
	entered, resume, finished chan struct{}
	once, release             sync.Once
	active                    atomic.Int32
	closed                    atomic.Bool
}

func (s *ingressParkedStore) effect(op string) func() {
	if s.closed.Load() {
		panic("receiver used released store")
	}
	s.active.Add(1)
	parked := false
	if op == s.operation {
		s.once.Do(func() { parked = true; close(s.entered); <-s.resume })
	}
	return func() {
		s.active.Add(-1)
		if parked {
			close(s.finished)
		}
	}
}
func (s *ingressParkedStore) ApplyEntry(lba uint32, p []byte, lsn uint64) error {
	done := s.effect("apply")
	defer done()
	return s.LogicalStorage.ApplyEntry(lba, p, lsn)
}
func (s *ingressParkedStore) WriteExtentDirect(lba uint32, p []byte) error {
	done := s.effect("base")
	defer done()
	return s.LogicalStorage.WriteExtentDirect(lba, p)
}
func (s *ingressParkedStore) Sync() (uint64, error) {
	done := s.effect("sync")
	defer done()
	return s.LogicalStorage.Sync()
}
func (s *ingressParkedStore) unblock() { s.release.Do(func() { close(s.resume) }) }
func (s *ingressParkedStore) Close() error {
	if s.active.Load() != 0 {
		return fmt.Errorf("store still has %d active effects", s.active.Load())
	}
	s.closed.Store(true)
	return s.LogicalStorage.Close()
}

// The inherited dual bridge does not expose a join handle. Its caller logs
// AFTER Serve returns; observe that exact connection's exit before releasing
// our store. This is an observed test-owner join, not a production guarantee.
type ingressExitWitness struct {
	io.Writer
	peer string
	once sync.Once
	done chan struct{}
}

func (w *ingressExitWitness) Write(p []byte) (int, error) {
	n, e := w.Writer.Write(p)
	if strings.Contains(string(p), "replica Serve exit remote="+w.peer+" ") {
		w.once.Do(func() { close(w.done) })
	}
	return n, e
}
func ingressAwait(t *testing.T, what string, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(4 * time.Second):
		t.Fatal(what)
	}
}
func ingressPair(t *testing.T) (string, string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		a, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		x := a.Addr().String()
		y, e := deriveDualLaneAddr(x)
		if e != nil {
			a.Close()
			continue
		}
		b, e := net.Listen("tcp", y)
		a.Close()
		if e == nil {
			b.Close()
			return x, y
		}
	}
	t.Fatal("no available port pair")
	return "", ""
}
func ingressFrame(t *testing.T, c net.Conn, k byte, p []byte) {
	t.Helper()
	b := make([]byte, 5+len(p))
	b[0] = k
	binary.BigEndian.PutUint32(b[1:], uint32(len(p)))
	copy(b[5:], p)
	if _, e := c.Write(b); e != nil {
		t.Fatal(e)
	}
}
func ingressStart(t *testing.T, c net.Conn) {
	t.Helper()
	p := make([]byte, 36)
	binary.BigEndian.PutUint64(p, 1)
	binary.BigEndian.PutUint64(p[8:], 1)
	binary.BigEndian.PutUint64(p[16:], 1)
	binary.BigEndian.PutUint32(p[24:], 4)
	binary.BigEndian.PutUint64(p[28:], 1)
	ingressFrame(t, c, 1, p)
}
func ingressBase(t *testing.T, c net.Conn, p []byte) {
	t.Helper()
	b := make([]byte, 4+len(p))
	copy(b[4:], p)
	ingressFrame(t, c, 2, b)
}
func ingressWireClosed(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var b [1]byte
	_, err := c.Read(b[:])
	if err == nil {
		t.Fatal("unexpected successful wire outcome after withdrawal")
	}
	if n, ok := err.(net.Error); ok && n.Timeout() {
		t.Fatal("wire did not settle")
	}
	t.Logf("explicit wire failure after withdrawal: %v", err)
}
func ingressClosedPort(t *testing.T, addr string) {
	t.Helper()
	c, e := net.DialTimeout("tcp", addr, time.Second)
	if c != nil {
		c.Close()
	}
	if !errors.Is(e, syscall.ECONNREFUSED) {
		t.Fatalf("withdrawn endpoint %s accepted connection: %v", addr, e)
	}
}

// Each acquired resource is attached to this owner before the next fallible
// operation. Cleanup unblocks first, joins all started callers, then releases
// storage and restores the logger. Unknown settlement retains both resources.
type ingressTestOwner struct {
	t                *testing.T
	store            *ingressParkedStore
	ingress          *replicationIngress
	conn             net.Conn
	witness          *ingressExitWitness
	addr, dual, lane string
	withdrawalDone   chan struct{}
	withdrawalErr    error
	workerID         string
	cleanupOnce      sync.Once
	cleanupErr       error
}

func newIngressTestOwner(t *testing.T, operation, lane string) *ingressTestOwner {
	t.Helper()
	s := &ingressParkedStore{LogicalStorage: storage.NewBlockStore(4, 4096), operation: operation, entered: make(chan struct{}), resume: make(chan struct{}), finished: make(chan struct{})}
	o := &ingressTestOwner{t: t, store: s, lane: lane}
	t.Cleanup(func() {
		if err := o.cleanup(); err != nil {
			t.Error(err)
		}
	})
	o.addr, o.dual = ingressPair(t)
	o.ingress = &replicationIngress{addr: o.addr, store: s, dualLane: true}
	if err := o.ingress.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	return o
}
func (o *ingressTestOwner) dial() {
	target := o.addr
	if o.lane == "dual" {
		target = o.dual
	}
	c, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		o.t.Fatal(err)
	}
	o.conn = c // ownership recorded immediately, before deadlines or assertions
	_ = c.SetDeadline(time.Now().Add(8 * time.Second))
	if o.lane == "dual" {
		o.witness = &ingressExitWitness{Writer: log.Writer(), peer: c.LocalAddr().String(), done: make(chan struct{})}
		log.SetOutput(o.witness)
	}
}
func (o *ingressTestOwner) withdraw() {
	o.withdrawalDone = make(chan struct{})
	id := make(chan string, 1)
	go func() {
		var b [128]byte
		n := runtime.Stack(b[:], false)
		id <- strings.Fields(string(b[:n]))[1]
		o.withdrawalErr = o.ingress.FenceAsPrimaryWithPeers(2, false)
		close(o.withdrawalDone)
	}()
	o.workerID = <-id
}
func (o *ingressTestOwner) waitWithdrawal() {
	ingressAwait(o.t, "withdrawal did not settle", o.withdrawalDone)
	if o.withdrawalErr != nil {
		o.t.Fatal(o.withdrawalErr)
	}
}
func (o *ingressTestOwner) cleanup() error {
	o.cleanupOnce.Do(func() {
		o.store.unblock()
		if o.conn != nil {
			_ = o.conn.Close()
		}
		await := func(c <-chan struct{}, what string) bool {
			select {
			case <-c:
				return true
			case <-time.After(4 * time.Second):
				o.cleanupErr = fmt.Errorf("UNKNOWN cleanup: %s; store/logger retained", what)
				return false
			}
		}
		if o.withdrawalDone != nil && !await(o.withdrawalDone, "withdrawal worker") {
			return
		}
		if o.ingress != nil {
			stopped := make(chan struct{})
			go func() { o.ingress.Stop(); close(stopped) }()
			if !await(stopped, "ingress Stop") {
				return
			}
		}
		if o.witness != nil && !await(o.witness.done, "dual Serve exit") {
			return
		}
		if err := o.store.Close(); err != nil {
			o.cleanupErr = err
			return
		}
		if o.witness != nil {
			log.SetOutput(o.witness.Writer)
		}
		if os.Getenv("INGRESS_FAILURE_CASE") != "" {
			if o.store.active.Load() != 0 || !o.store.closed.Load() {
				o.cleanupErr = fmt.Errorf("cleanup did not release settled store")
				return
			}
			for _, addr := range []string{o.addr, o.dual} {
				c, e := net.DialTimeout("tcp", addr, time.Second)
				if c != nil {
					c.Close()
				}
				if !errors.Is(e, syscall.ECONNREFUSED) {
					o.cleanupErr = fmt.Errorf("cleanup port remains open: %s %v", addr, e)
					return
				}
			}
			if o.witness != nil && log.Writer() != o.witness.Writer {
				o.cleanupErr = fmt.Errorf("logger not restored")
				return
			}
			fmt.Println("CLEANUP_SETTLED store_closed=true active=0 logger_restored=true ports_closed=true")
		}
	})
	return o.cleanupErr
}
func (o *ingressTestOwner) parked(payload []byte) {
	if o.lane == "legacy" {
		l := transport.RecoveryLineage{Epoch: 1, SessionID: 1, EndpointVersion: 1, FrontierHint: 1}
		if err := transport.WriteMsg(o.conn, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: l, LBA: 0, LSN: 1, Data: payload})); err != nil {
			o.t.Fatal(err)
		}
	} else {
		ingressStart(o.t, o.conn)
		ingressBase(o.t, o.conn, payload)
		if o.store.operation == "sync" {
			ingressFrame(o.t, o.conn, 3, nil)
		}
	}
	ingressAwait(o.t, "storage operation was not admitted", o.store.entered)
}

// No public wait-arrival seam exists. Observe ONLY this caller's actual
// sync.Cond wait in the production fence, using runtime's diagnostic stack.
// A skip-drain mutant must return (and fail) before we ever release the effect;
// merely closing admission is no longer accepted as a wait witness.
func ingressRequireDrainWait(t *testing.T, o *ingressTestOwner) {
	t.Helper()
	end := time.Now().Add(4 * time.Second)
	for {
		select {
		case <-o.withdrawalDone:
			t.Fatalf("drain skipped: withdrawal returned while admitted effect is held (active=%d)", o.store.active.Load())
		default:
		}
		b := make([]byte, 1<<20)
		n := runtime.Stack(b, true)
		for _, stack := range strings.Split(string(b[:n]), "\n\n") {
			if strings.HasPrefix(stack, "goroutine "+o.workerID+" ") &&
				((strings.Contains(stack, "(*ReplicaListener).fenceAndDrain") && strings.Contains(stack, "sync.(*Cond).Wait")) ||
					(o.lane == "legacy" && strings.Contains(stack, "(*ReplicaListener).StopHard") && strings.Contains(stack, "sync.(*WaitGroup).Wait"))) {
				t.Log("withdrawal caller reached production effect drain/join wait")
				return
			}
		}
		if time.Now().After(end) {
			t.Fatal("no production drain-wait witness")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestReplicationIngressWithdrawalDrainsAdmittedEffects(t *testing.T) {
	for _, lane := range []string{"legacy", "dual"} {
		t.Run(lane, func(t *testing.T) {
			op := "apply"
			if lane == "dual" {
				op = "base"
			}
			o := newIngressTestOwner(t, op, lane)
			o.dial()
			s, r, c := o.store, o.ingress, o.conn
			payload := bytes.Repeat([]byte{0xD3}, 4096)
			o.parked(payload)
			listener := r.listener
			later := bytes.Repeat([]byte{0xF5}, 4096)
			if lane == "legacy" {
				l := transport.RecoveryLineage{Epoch: 1, SessionID: 1, EndpointVersion: 1, FrontierHint: 1}
				if err := transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: l, LBA: 1, LSN: 2, Data: later})); err != nil {
					t.Fatal(err)
				}
			} else {
				p := make([]byte, 4+len(later))
				binary.BigEndian.PutUint32(p, 1)
				copy(p[4:], later)
				ingressFrame(t, c, 2, p)
			}
			o.withdraw()
			ingressRequireDrainWait(t, o)
			if release, err := listener.EnterDualLane(1); err == nil {
				release()
				t.Fatal("drain wait did not close admission")
			}
			if s.active.Load() != 1 {
				t.Fatal("effect no longer parked")
			}
			s.unblock()
			ingressAwait(t, "effect did not return", s.finished)
			o.waitWithdrawal()
			ingressWireClosed(t, c)
			if o.witness != nil {
				ingressAwait(t, "receiver did not exit", o.witness.done)
			}
			ingressClosedPort(t, o.addr)
			ingressClosedPort(t, o.dual)
			if release, e := listener.EnterDualLane(1); e == nil {
				release()
				t.Fatal("old gate accepted another frame")
			}
			got, err := s.Read(0)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatal("admitted bytes differ", err)
			}
			got, err = s.Read(1)
			if err != nil || !bytes.Equal(got, make([]byte, 4096)) {
				t.Fatal("queued mutation reached storage", err)
			}
			if err = o.cleanup(); err != nil {
				t.Fatal(err)
			}
			t.Log("actual wait witnessed; exact effect drained; wire failed; callers settled before store/logger release")
		})
	}
}

// Inherited sendAck Sync remains outside counted effects. The owner still
// joins the observed caller before releasing store/logger, on every exit.
func TestReplicationIngressDualAckSyncNeedsOwnerSettlement(t *testing.T) {
	o := newIngressTestOwner(t, "sync", "dual")
	o.dial()
	payload := bytes.Repeat([]byte{0xE4}, 4096)
	o.parked(payload)
	o.withdraw()
	o.waitWithdrawal()
	if o.store.active.Load() != 1 {
		t.Fatal("Sync not held at withdrawal return")
	}
	select {
	case <-o.witness.done:
		t.Fatal("receiver exited with Sync held")
	default:
	}
	ingressWireClosed(t, o.conn)
	o.store.unblock()
	ingressAwait(t, "Sync did not return", o.store.finished)
	ingressAwait(t, "receiver did not settle", o.witness.done)
	got, err := o.store.Read(0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("base bytes differ", err)
	}
	if err = o.cleanup(); err != nil {
		t.Fatal(err)
	}
	t.Log("INHERITED LIMIT: withdrawal returned with uncounted Sync live; owner joined before release")
}
func TestReplicationIngressCleanupFailureHelper(t *testing.T) {
	stage := os.Getenv("INGRESS_FAILURE_CASE")
	if stage == "" {
		return
	}
	lane, op := "dual", "base"
	if stage == "held-legacy" {
		lane, op = "legacy", "apply"
	}
	if stage == "held-sync" {
		op = "sync"
	}
	o := newIngressTestOwner(t, op, lane)
	if stage == "pre-dial" {
		t.Fatal("FORCED_FAILURE pre-dial")
	}
	o.dial()
	o.parked(bytes.Repeat([]byte{0x67}, 4096))
	o.withdraw()
	if op == "sync" {
		o.waitWithdrawal()
	} else {
		ingressRequireDrainWait(t, o)
	}
	t.Fatal("FORCED_FAILURE " + stage)
}
func TestReplicationIngressCleanupOnForcedFailure(t *testing.T) {
	for _, stage := range []string{"pre-dial", "held-legacy", "held-dual", "held-sync"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplicationIngressCleanupFailureHelper$", "-test.v")
			cmd.Env = append(os.Environ(), "INGRESS_FAILURE_CASE="+stage)
			b, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || ctx.Err() != nil {
				t.Fatalf("forced failure status: %v %s", err, b)
			}
			out := string(b)
			if !strings.Contains(out, "FORCED_FAILURE "+stage) || !strings.Contains(out, "CLEANUP_SETTLED store_closed=true active=0 logger_restored=true ports_closed=true") || strings.Contains(out, "UNKNOWN cleanup") {
				t.Fatalf("unsafe failure cleanup: %s", b)
			}
			t.Logf("%s expected Fatal; owned cleanup settled before process exit:\n%s", stage, b)
		})
	}
}
