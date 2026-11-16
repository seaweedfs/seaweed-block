package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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

func TestReplicationIngressWithdrawalDrainsAdmittedEffects(t *testing.T) {
	for _, lane := range []string{"legacy", "dual"} {
		t.Run(lane, func(t *testing.T) {
			op := "apply"
			if lane == "dual" {
				op = "base"
			}
			s := &ingressParkedStore{LogicalStorage: storage.NewBlockStore(4, 4096), operation: op, entered: make(chan struct{}), resume: make(chan struct{}), finished: make(chan struct{})}
			addr, dual := ingressPair(t)
			r := &replicationIngress{addr: addr, store: s, dualLane: true}
			if err := r.AdmitAsReplica(1); err != nil {
				t.Fatal(err)
			}
			target := addr
			if lane == "dual" {
				target = dual
			}
			c, err := net.DialTimeout("tcp", target, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_ = c.SetDeadline(time.Now().Add(8 * time.Second))
			witness := &ingressExitWitness{Writer: log.Writer(), peer: c.LocalAddr().String(), done: make(chan struct{})}
			if lane == "dual" {
				log.SetOutput(witness)
				defer log.SetOutput(witness.Writer)
			}
			defer func() {
				s.unblock()
				c.Close()
				r.Stop()
				if !s.closed.Load() {
					_ = s.Close()
				}
			}()
			payload := bytes.Repeat([]byte{0xD3}, 4096)
			listener := r.listener
			if lane == "legacy" {
				l := transport.RecoveryLineage{Epoch: 1, SessionID: 1, EndpointVersion: 1, FrontierHint: 1}
				if err = transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: l, LBA: 0, LSN: 1, Data: payload})); err != nil {
					t.Fatal(err)
				}
			} else {
				ingressStart(t, c)
				ingressBase(t, c, payload)
			}
			ingressAwait(t, "storage operation was not admitted", s.entered)
			// Queue a second mutation on the already-open connection. It must
			// never reach storage after the first admitted effect drains.
			later := bytes.Repeat([]byte{0xF5}, 4096)
			if lane == "legacy" {
				l := transport.RecoveryLineage{Epoch: 1, SessionID: 1, EndpointVersion: 1, FrontierHint: 1}
				if err = transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: l, LBA: 1, LSN: 2, Data: later})); err != nil {
					t.Fatal(err)
				}
			} else {
				p := make([]byte, 4+len(later))
				binary.BigEndian.PutUint32(p, 1)
				copy(p[4:], later)
				ingressFrame(t, c, 2, p)
			}

			withdrawn := make(chan error, 1)
			go func() { withdrawn <- r.FenceAsPrimaryWithPeers(2, false) }()
			// Public gate refusal is the fence-arrival witness. This cannot be a
			// scheduler-only negative: the new role is installed while our effect lives.
			end := time.Now().Add(3 * time.Second)
			for {
				release, err := listener.EnterDualLane(1)
				if err != nil {
					break
				}
				release()
				if time.Now().After(end) {
					t.Fatal("withdrawal never closed admission")
				}
				time.Sleep(time.Millisecond)
			}
			select {
			case e := <-withdrawn:
				t.Fatalf("withdrawal completed before effect release: %v", e)
			default:
			}
			if s.active.Load() != 1 {
				t.Fatal("effect no longer parked")
			}
			s.unblock()
			ingressAwait(t, "effect did not return", s.finished)
			select {
			case err = <-withdrawn:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("withdrawal did not settle")
			}
			ingressWireClosed(t, c)
			if lane == "dual" {
				ingressAwait(t, "dual receiver did not exit after cancellation", witness.done)
			}
			ingressClosedPort(t, addr)
			ingressClosedPort(t, dual)
			if release, e := listener.EnterDualLane(1); e == nil {
				release()
				t.Fatal("old gate accepted another frame")
			}
			got, err := s.Read(0)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatal("admitted bytes differ", err)
			}
			untouched, e := s.Read(1)
			if e != nil || !bytes.Equal(untouched, make([]byte, 4096)) {
				t.Fatal("queued post-withdrawal mutation reached storage", e)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			t.Log("admitted exact effect drained; wire failed explicitly; worker settled before store release")
		})
	}
}

// This bounded reproducer records the inherited distinction: sendAck's Sync
// is outside the counted frame. The test owner must await the receiver exit;
// ingress withdrawal alone is NOT proof that this Sync has stopped.
func TestReplicationIngressDualAckSyncNeedsOwnerSettlement(t *testing.T) {
	s := &ingressParkedStore{LogicalStorage: storage.NewBlockStore(4, 4096), operation: "sync", entered: make(chan struct{}), resume: make(chan struct{}), finished: make(chan struct{})}
	addr, dual := ingressPair(t)
	r := &replicationIngress{addr: addr, store: s, dualLane: true}
	if err := r.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	c, err := net.DialTimeout("tcp", dual, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(8 * time.Second))
	witness := &ingressExitWitness{Writer: log.Writer(), peer: c.LocalAddr().String(), done: make(chan struct{})}
	log.SetOutput(witness)
	defer log.SetOutput(witness.Writer)
	defer func() {
		s.unblock()
		c.Close()
		r.Stop()
		if !s.closed.Load() {
			_ = s.Close()
		}
	}()
	payload := bytes.Repeat([]byte{0xE4}, 4096)
	ingressStart(t, c)
	ingressBase(t, c, payload)
	ingressFrame(t, c, 3, nil)
	ingressAwait(t, "uncounted acknowledgement Sync did not start", s.entered)
	withdrawn := make(chan error, 1)
	go func() { withdrawn <- r.FenceAsPrimaryWithPeers(2, false) }()
	select {
	case err = <-withdrawn:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("inherited uncounted Sync became a counted drain")
	}
	if s.active.Load() != 1 {
		t.Fatal("no outstanding Sync at withdrawal return")
	}
	select {
	case <-witness.done:
		t.Fatal("receiver exited while Sync held")
	default:
	}
	ingressWireClosed(t, c)
	// Explicit owner disposition: release the parked Sync and wait for the
	// caller's actual Serve-exit witness before releasing the store.
	s.unblock()
	ingressAwait(t, "Sync did not return", s.finished)
	ingressAwait(t, "receiver did not settle", witness.done)
	got, err := s.Read(0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("base bytes differ", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("INHERITED LIMIT reproduced: withdrawal returned with sendAck Sync live; test owner waited for receiver exit before store release")
}
