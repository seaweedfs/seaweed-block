package replication

import (
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// nextLSNStore records the LSNs ApplyEntry is called with (attempted) and
// the ones it applied (recorded after the store returned without error).
type nextLSNStore struct {
	storage.LogicalStorage
	mu                 sync.Mutex
	attempted, applied []uint64
	changed            chan struct{}
}

func (s *nextLSNStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	s.mu.Lock()
	s.attempted = append(s.attempted, lsn)
	s.mu.Unlock()
	err := s.LogicalStorage.ApplyEntry(lba, data, lsn)
	if err == nil {
		s.mu.Lock()
		s.applied = append(s.applied, lsn)
		s.mu.Unlock()
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
	return err
}

func (s *nextLSNStore) snapshot() (attempted, applied []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.attempted...), append([]uint64(nil), s.applied...)
}

// waitApplied waits until n entries are applied.
func (s *nextLSNStore) waitApplied(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if _, applied := s.snapshot(); len(applied) >= n {
			return
		}
		select {
		case <-s.changed:
		case <-deadline:
			_, applied := s.snapshot()
			t.Fatalf("applied %v; want %d entries", applied, n)
		}
	}
}

type nextLSNReplica struct {
	listener *transport.ReplicaListener
	store    *nextLSNStore
	epochs   *transport.EpochTable
}

// newNextLSNReplica builds the replica listener the way the volume does
// (cmd/blockvolume replicationIngress.roleLocked): the floor, the apply gate
// over the store, the epoch table, then the replica role. gate false builds
// the listener without a hook (the demo and harness path).
func newNextLSNReplica(t *testing.T, kind string, gate bool) *nextLSNReplica {
	t.Helper()
	dir := t.TempDir()
	var base storage.LogicalStorage
	switch kind {
	case "walstore":
		s, err := storage.CreateWALStore(filepath.Join(dir, "r.wal"), 64, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		base = s
	case "smartwal":
		s, err := smartwal.CreateStore(filepath.Join(dir, "r.smartwal"), 64, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		base = s
	default:
		t.Fatalf("store kind %q", kind)
	}
	store := &nextLSNStore{LogicalStorage: base, changed: make(chan struct{}, 1)}
	var hook transport.ApplyHook
	if gate {
		hook = NewReplicaApplyGate(store)
	}
	floor := filepath.Join(dir, "v1.floor")
	if err := transport.InitReplicaFloor(floor, "v1"); err != nil { // written at the volume's creation
		t.Fatal(err)
	}
	listener, err := transport.NewReplicaListenerWithFloorAndApplyHook("127.0.0.1:0", store, "v1", floor, hook)
	if err != nil {
		t.Fatal(err)
	}
	epochs, err := transport.LoadEpochTable(filepath.Join(dir, "v1.epoch-table"), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := epochs.TrimAbove(store.NextLSN()); err != nil {
		t.Fatal(err)
	}
	listener.SetEpochTable(epochs)
	if err := listener.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	return &nextLSNReplica{listener: listener, store: store, epochs: epochs}
}

var nextLSNLive = transport.RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

func (r *nextLSNReplica) dial(t *testing.T) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", r.listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	return c
}

func ship(t *testing.T, c net.Conn, l transport.RecoveryLineage, lsns ...uint64) {
	t.Helper()
	for _, lsn := range lsns {
		e := transport.ShipEntry{Lineage: l, LBA: uint32(lsn), LSN: lsn, Data: make([]byte, blockSize)}
		if err := transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(e)); err != nil {
			t.Fatalf("ship LSN %d: %v", lsn, err)
		}
	}
}

// expectClosed sends a barrier on c and requires the replica to have
// closed the connection: end of stream or a reset, never a reply and never
// a deadline.
func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	_ = transport.WriteMsg(c, transport.MsgBarrierReq, transport.EncodeLineage(nextLSNLive))
	msgType, _, err := transport.ReadMsg(c)
	var ne net.Error
	switch {
	case err == nil:
		t.Fatalf("the refused connection answered (type 0x%02x); want it closed", msgType)
	case errors.As(err, &ne) && ne.Timeout():
		t.Fatalf("the refused connection stayed open (read timed out); want it closed")
	case !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !isReset(err):
		t.Fatalf("the refused connection: %v; want end of stream", err)
	}
}

func isReset(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && !op.Timeout()
}

func (r *nextLSNReplica) barrier(t *testing.T) uint64 {
	t.Helper()
	c := r.dial(t)
	if err := transport.WriteMsg(c, transport.MsgBarrierReq, transport.EncodeLineage(nextLSNLive)); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := transport.ReadMsg(c)
	if err != nil || msgType != transport.MsgBarrierResp {
		t.Fatalf("barrier: type=0x%02x err=%v", msgType, err)
	}
	resp, err := transport.DecodeBarrierResp(payload)
	if err != nil {
		t.Fatal(err)
	}
	return resp.AchievedLSN
}

func equalLSNs(a []uint64, b ...uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var nextLSNRows = []struct {
	kind string
	gate bool
}{
	{"walstore", true},
	{"smartwal", true},
	{"walstore", false},
}

func rowName(kind string, gate bool) string {
	if gate {
		return kind + "/apply gate (the volume's listener)"
	}
	return kind + "/no hook"
}

// Shipped 1, 2, 4 on the live lane, the replica applies 1 and 2 and refuses
// 4 before the store sees it: the connection closes, the barrier reports 2.
// The primary's catch-up from that end (here replayed by hand on a new
// connection: 3, 4) applies every LSN once, in order, and the barrier
// reports 4.
func TestReplicaAppliesALiveEntryOnlyAtItsNextLSN(t *testing.T) {
	for _, row := range nextLSNRows {
		t.Run(rowName(row.kind, row.gate), func(t *testing.T) {
			r := newNextLSNReplica(t, row.kind, row.gate)
			c := r.dial(t)
			ship(t, c, nextLSNLive, 1, 2, 4) // 3 is skipped
			expectClosed(t, c)
			r.store.waitApplied(t, 2)
			if attempted, _ := r.store.snapshot(); !equalLSNs(attempted, 1, 2) {
				t.Fatalf("the store was asked to apply %v; want 1, 2 (4 refused before the store)", attempted)
			}
			if end := r.barrier(t); end != 2 {
				t.Fatalf("barrier after 1, 2, 4: %d; want 2 (nothing past the gap)", end)
			}

			again := r.dial(t)
			ship(t, again, nextLSNLive, 3, 4)
			r.store.waitApplied(t, 4)
			if end := r.barrier(t); end != 4 {
				t.Fatalf("barrier after the catch-up: %d; want 4", end)
			}
			if attempted, applied := r.store.snapshot(); !equalLSNs(attempted, 1, 2, 3, 4) || !equalLSNs(applied, 1, 2, 3, 4) {
				t.Fatalf("attempted %v applied %v; want 1, 2, 3, 4 once each, in order", attempted, applied)
			}
		})
	}
}

// While a recovery owns the store (its lane has started, the epoch table
// is invalid), no live entry lands, also at the store's next LSN: the live
// lane never runs ahead of a recovery, so the end the replica reports is
// the one the recovery lane built.
func TestReplicaAppliesNoLiveEntryWhileARecoveryOwnsTheStore(t *testing.T) {
	for _, row := range nextLSNRows {
		if !row.gate {
			continue
		}
		t.Run(rowName(row.kind, row.gate), func(t *testing.T) {
			r := newNextLSNReplica(t, row.kind, row.gate)
			c := r.dial(t)
			ship(t, c, nextLSNLive, 1, 2)
			r.store.waitApplied(t, 2)

			recovery := transport.RecoveryLineage{SessionID: 2, Epoch: 1, EndpointVersion: 1, TargetLSN: 4}
			lane := r.dial(t)
			if err := transport.WriteMsg(lane, transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(recovery, 2)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for !r.epochs.Invalid() {
				if time.Now().After(deadline) {
					t.Fatal("the recovery lane never took the store")
				}
				time.Sleep(5 * time.Millisecond)
			}

			live := r.dial(t)
			ship(t, live, recovery, 3)
			expectClosed(t, live)
			if attempted, _ := r.store.snapshot(); !equalLSNs(attempted, 1, 2) {
				t.Fatalf("the store was asked to apply %v while a recovery owned it; want 1, 2", attempted)
			}
		})
	}
}
