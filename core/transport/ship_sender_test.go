package transport

import (
	"bytes"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// shipTestLineage is the canonical lineage used by Ship tests.
func shipTestLineage() RecoveryLineage {
	return RecoveryLineage{
		SessionID:       42,
		Epoch:           7,
		EndpointVersion: 3,
		TargetLSN:       1000,
	}
}

// dialAttachedShipSession stands up a primary executor + replica listener,
// registers a session, dials the replica, and attaches the conn to the
// session. Returns the live (exec, replicaStore) ready for Ship calls.
func dialAttachedShipSession(t *testing.T, lineage RecoveryLineage) (*BlockExecutor, *storage.BlockStore) {
	t.Helper()
	_, replica, listener := setupPrimaryReplica(t)
	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, listener.Addr())

	conn, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatalf("dial replica: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := exec.attachShipSession(lineage, conn); err != nil {
		t.Fatalf("attachShipSession: %v", err)
	}
	return exec, replica
}

// TestExecutor_Ship_SingleEntry — happy path: one entry, one replica,
// verify byte-exact arrival.
func TestExecutor_Ship_SingleEntry(t *testing.T) {
	lineage := shipTestLineage()
	exec, replica := dialAttachedShipSession(t, lineage)

	data := make([]byte, 4096)
	data[0], data[1] = 0xAB, 0xCD
	lsn := uint64(1)

	if err := exec.Ship("r1", lineage, 3, lsn, data); err != nil {
		t.Fatalf("Ship: %v", err)
	}

	// Replica apply is async — wait for the LBA to land.
	deadline := time.Now().Add(2 * time.Second)
	var got []byte
	for time.Now().Before(deadline) {
		got, _ = replica.Read(3)
		if got != nil && got[0] == 0xAB && got[1] == 0xCD {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("replica LBA 3 data mismatch: got [%02x %02x ...], want [ab cd ...]",
			got[0], got[1])
	}
}

// TestExecutor_Ship_MultipleEntriesOrdered — ship 10 entries sequentially;
// replica apply order must preserve LBA/LSN ordering byte-exact.
func TestExecutor_Ship_MultipleEntriesOrdered(t *testing.T) {
	lineage := shipTestLineage()
	exec, replica := dialAttachedShipSession(t, lineage)

	const n = 10
	for i := uint32(0); i < n; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		data[1] = byte(0xA0 + i)
		if err := exec.Ship("r1", lineage, i, uint64(i+1), data); err != nil {
			t.Fatalf("Ship %d: %v", i, err)
		}
	}

	// Wait for all LBAs to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		last, _ := replica.Read(n - 1)
		if last != nil && last[0] == byte(n) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := uint32(0); i < n; i++ {
		got, _ := replica.Read(i)
		if got == nil || got[0] != byte(i+1) || got[1] != byte(0xA0+i) {
			t.Fatalf("LBA %d mismatch: got [%02x %02x], want [%02x %02x]",
				i, got[0], got[1], byte(i+1), byte(0xA0+i))
		}
	}
}

// TestExecutor_Ship_ConnFailure — kill conn before Ship; Ship returns
// error. This is the error-return behavior the forward-carry condition
// relies on (T4a-3 ReplicaPeer translates error → Invalidate).
func TestExecutor_Ship_ConnFailure(t *testing.T) {
	lineage := shipTestLineage()
	exec, _ := dialAttachedShipSession(t, lineage)

	// Yank the conn out from under the session to force a write error.
	exec.mu.Lock()
	sess := exec.sessions[lineage.SessionID]
	conn := sess.conn
	exec.mu.Unlock()
	_ = conn.Close()

	data := make([]byte, 4096)
	err := exec.Ship("r1", lineage, 0, 1, data)
	if err == nil {
		t.Fatal("expected Ship error after conn close, got nil")
	}
	if !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("expected 'write failed' error, got: %v", err)
	}
}

// TestExecutor_Ship_NoSession — Ship against an unknown sessionID returns
// error without panicking. Exercises the pre-registration contract:
// Ship does NOT auto-register from incoming lineage (architect-revised
// G-1, Change 1) because the accepted-lineage model is what grounds
// the epoch fence.
func TestExecutor_Ship_NoSession(t *testing.T) {
	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, "127.0.0.1:0")

	data := make([]byte, 4096)
	err := exec.Ship("r1", shipTestLineage(), 0, 1, data)
	if err == nil {
		t.Fatal("expected error with no registered session, got nil")
	}
	if !strings.Contains(err.Error(), "no session") {
		t.Fatalf("expected 'no session' error, got: %v", err)
	}
}

// TestExecutor_Ship_LazyDial_OnRegisteredSessionWithoutConn — architect-
// revised Option B regression: when the session is registered but has
// no attached conn, Ship lazy-dials replicaAddr, attaches, and sends.
// Preserves V2 ensureDataConn semantic (wal_shipper.go:632) at the
// V2-faithful V3 location.
func TestExecutor_Ship_LazyDial_OnRegisteredSessionWithoutConn(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)
	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, listener.Addr())

	lineage := shipTestLineage()
	if err := exec.registerShipSessionForTest(lineage); err != nil {
		t.Fatalf("registerShipSessionForTest: %v", err)
	}
	// Close whatever conn Ship lazy-dials so the replica listener's
	// handleConn goroutine can exit — listener.Stop() waits on it.
	t.Cleanup(func() {
		exec.mu.Lock()
		if s := exec.sessions[lineage.SessionID]; s != nil && s.conn != nil {
			_ = s.conn.Close()
		}
		exec.mu.Unlock()
	})

	// Verify no conn attached before first Ship.
	exec.mu.Lock()
	if exec.sessions[lineage.SessionID].conn != nil {
		exec.mu.Unlock()
		t.Fatal("precondition violated: conn already attached")
	}
	exec.mu.Unlock()

	data := make([]byte, 4096)
	data[0], data[1] = 0xBA, 0xBE
	if err := exec.Ship("r1", lineage, 2, 1, data); err != nil {
		t.Fatalf("Ship lazy-dial: %v", err)
	}

	// Post-Ship: conn must be attached to the session.
	exec.mu.Lock()
	attached := exec.sessions[lineage.SessionID].conn != nil
	exec.mu.Unlock()
	if !attached {
		t.Fatal("lazy-dial did not attach conn to session")
	}

	// Data must arrive on replica.
	deadline := time.Now().Add(2 * time.Second)
	var got []byte
	for time.Now().Before(deadline) {
		got, _ = replica.Read(2)
		if got != nil && got[0] == 0xBA {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got == nil || got[0] != 0xBA || got[1] != 0xBE {
		t.Fatalf("replica LBA 2 did not receive lazy-dialed ship: got [%02x %02x]",
			got[0], got[1])
	}

	// Second Ship must reuse the already-attached conn (no re-dial).
	exec.mu.Lock()
	connAfterFirst := exec.sessions[lineage.SessionID].conn
	exec.mu.Unlock()

	data2 := make([]byte, 4096)
	data2[0] = 0xEF
	if err := exec.Ship("r1", lineage, 3, 2, data2); err != nil {
		t.Fatalf("Ship 2: %v", err)
	}
	exec.mu.Lock()
	connAfterSecond := exec.sessions[lineage.SessionID].conn
	exec.mu.Unlock()
	if connAfterFirst != connAfterSecond {
		t.Fatal("second Ship re-dialed instead of reusing attached conn")
	}
}

// TestExecutor_Ship_LazyDial_DialFailure_ReturnsError — architect-revised
// Change 2: dial failure returns error (not V2's silent return-nil),
// because V3 BlockExecutor does not own peer state. Peer layer
// (ReplicaPeer.ShipEntry, T4a-3) will translate the error to Degraded
// + Invalidate.
func TestExecutor_Ship_LazyDial_DialFailure_ReturnsError(t *testing.T) {
	// Reserve a port then release it → guaranteed unreachable.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, deadAddr)

	lineage := shipTestLineage()
	if err := exec.registerShipSessionForTest(lineage); err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 4096)
	err = exec.Ship("r1", lineage, 0, 1, data)
	if err == nil {
		t.Fatal("expected dial-failure error, got nil")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Fatalf("expected 'dial' error, got: %v", err)
	}

	// Session must remain registered but without a conn so a future
	// dial can succeed once replicaAddr becomes reachable again.
	exec.mu.Lock()
	sess, ok := exec.sessions[lineage.SessionID]
	stillNoConn := ok && sess != nil && sess.conn == nil
	exec.mu.Unlock()
	if !stillNoConn {
		t.Fatal("dial failure should leave session registered with no conn")
	}
}

// TestExecutor_Ship_StaleEpoch_SilentDrop is the explicit regression test
// for L1 §2.1 invariant #2 (epoch-== silent drop). QA added this as a
// Gate G-1 closing condition.
//
// Primary session is registered at epoch E. A Ship arrives with
// lineage.Epoch = E-1 (stale). Ship must:
//  1. Return nil (not error — caller keeps shipping subsequent entries)
//  2. Not write any bytes to the wire (nothing reaches replica)
func TestExecutor_Ship_StaleEpoch_SilentDrop(t *testing.T) {
	lineage := shipTestLineage() // Epoch = 7
	_, replica, listener := setupPrimaryReplica(t)
	primary := storage.NewBlockStore(64, 4096)
	exec := NewBlockExecutor(primary, listener.Addr())

	// Intercept the replica side with a counting wrapper so we can assert
	// "nothing crossed the wire." Use a TCP proxy that forwards bytes to
	// the real replica listener and increments a counter on any byte seen.
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxyLn.Close() })

	var bytesForwarded atomic.Int64
	go func() {
		for {
			client, err := proxyLn.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", listener.Addr())
			if err != nil {
				_ = client.Close()
				continue
			}
			go func(c, u net.Conn) {
				defer c.Close()
				defer u.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						bytesForwarded.Add(int64(n))
						_, wErr := u.Write(buf[:n])
						if wErr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(client, upstream)
		}
	}()

	conn, err := net.Dial("tcp", proxyLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := exec.attachShipSession(lineage, conn); err != nil {
		t.Fatal(err)
	}

	// Ship with stale epoch (E-1).
	stale := lineage
	stale.Epoch = lineage.Epoch - 1

	data := make([]byte, 4096)
	data[0], data[1] = 0x11, 0x22
	if err := exec.Ship("r1", stale, 5, 1, data); err != nil {
		t.Fatalf("stale-epoch Ship should return nil, got: %v", err)
	}

	// Let any stray bytes settle.
	time.Sleep(100 * time.Millisecond)

	if got := bytesForwarded.Load(); got != 0 {
		t.Fatalf("stale-epoch Ship must not cross the wire, %d bytes forwarded", got)
	}

	// Replica must not have applied LBA 5.
	rd, _ := replica.Read(5)
	if rd != nil && (rd[0] != 0 || rd[1] != 0) {
		t.Fatalf("replica LBA 5 should be untouched by stale-epoch Ship, got [%02x %02x]",
			rd[0], rd[1])
	}

	// Sanity: a matching-epoch Ship on the same session still works.
	fresh := make([]byte, 4096)
	fresh[0], fresh[1] = 0xAA, 0xBB
	if err := exec.Ship("r1", lineage, 6, 2, fresh); err != nil {
		t.Fatalf("fresh-epoch Ship: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rd, _ = replica.Read(6)
		if rd != nil && rd[0] == 0xAA {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rd == nil || rd[0] != 0xAA || rd[1] != 0xBB {
		t.Fatalf("fresh-epoch Ship should arrive: got [%02x %02x]", rd[0], rd[1])
	}
}
