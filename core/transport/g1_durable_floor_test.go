package transport

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// G1 (fencing-gaps-todo.md): the replica's epoch floor is durable. It is
// created with the volume (epoch 0, a bootstrap floor, never an assignment),
// raised and persisted before any mutation or ACK at a higher epoch, loaded at
// restart, and a production listener refuses every mutation until its role is
// known.

const g1Vol = "vol1"

func g1Fresh(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), g1Vol+".replica-floor")
	if err := InitReplicaFloor(path, g1Vol); err != nil {
		t.Fatalf("init floor: %v", err)
	}
	return path
}

func g1Listener(t *testing.T, store storage.LogicalStorage, floorPath string) *ReplicaListener {
	t.Helper()
	ln, err := NewReplicaListenerWithFloor("127.0.0.1:0", store, g1Vol, floorPath)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	ln.Serve()
	t.Cleanup(ln.Stop)
	return ln
}

func g1Block(t *testing.T, store storage.LogicalStorage) []byte {
	t.Helper()
	b, err := store.Read(0)
	if err != nil {
		t.Fatalf("read LBA 0: %v", err)
	}
	return b
}

func g1Lineage(sess, epoch uint64) RecoveryLineage {
	return RecoveryLineage{SessionID: sess, Epoch: epoch, EndpointVersion: 1, TargetLSN: 1}
}

// g1Ship sends one ship on a fresh connection and reports whether it was
// applied (bytes visible within d) or refused (connection closed).
func g1Ship(t *testing.T, ln *ReplicaListener, store storage.LogicalStorage, l RecoveryLineage, lsn uint64, fill byte) bool {
	t.Helper()
	want := bytes.Repeat([]byte{fill}, 4096)
	if g2Refused(t, ln.Addr(), MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: l, LBA: 0, LSN: lsn, Data: want})) {
		if bytes.Equal(g1Block(t, store), want) {
			t.Fatalf("epoch-%d ship refused yet its bytes were applied", l.Epoch)
		}
		return false
	}
	deadline := time.Now().Add(2 * time.Second)
	for !bytes.Equal(g1Block(t, store), want) {
		if time.Now().After(deadline) {
			t.Fatalf("epoch-%d ship neither refused nor applied", l.Epoch)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// g1Barrier sends a barrier and reports whether it was ACKed (a barrier
// response echoing the lineage) or refused (connection closed, no ACK).
func g1Barrier(t *testing.T, ln *ReplicaListener, l RecoveryLineage) bool {
	t.Helper()
	conn := g2Dial(t, ln.Addr())
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := WriteMsg(conn, MsgBarrierReq, EncodeLineage(l)); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil {
		return false
	}
	if msgType != MsgBarrierResp {
		t.Fatalf("barrier answered with 0x%02x", msgType)
	}
	resp, err := DecodeBarrierResp(payload)
	if err != nil || resp.Lineage != l {
		t.Fatalf("barrier ack %+v err=%v, want lineage %+v", resp, err, l)
	}
	return true
}

func g1Rebuild(t *testing.T, ln *ReplicaListener, store storage.LogicalStorage, l RecoveryLineage, fill byte) bool {
	t.Helper()
	data := bytes.Repeat([]byte{fill}, 4096)
	refused := g2Refused(t, ln.Addr(), MsgRebuildBlock, EncodeRebuildBlock(l, 0, data))
	if refused && bytes.Equal(g1Block(t, store), data) {
		t.Fatalf("epoch-%d rebuild refused yet its bytes were written", l.Epoch)
	}
	return !refused
}

func g1Persisted(t *testing.T, path string) uint64 {
	t.Helper()
	e, err := loadFloor(path, g1Vol)
	if err != nil {
		t.Fatalf("load floor: %v", err)
	}
	return e
}

// TestG1_RestartedReplicaRefusesShipBelowItsAcknowledgedEpoch — the red test,
// revised per the accepted contract (codex01 R3):
//
//	(a) admitted at epoch 1, a ship raises it to 2 and a real epoch-2 barrier
//	    is ACKed; restart on the same floor record;
//	(b) before any assignment, ship, barrier and rebuild are all refused;
//	(c) admitted at the STALE epoch 1, epoch 1 is still refused (from the
//	    persisted floor), and the current epoch 2 ship and barrier succeed.
func TestG1_RestartedReplicaRefusesShipBelowItsAcknowledgedEpoch(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)

	first := g1Listener(t, store, floor)
	if err := first.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	if !g1Ship(t, first, store, g1Lineage(1, 2), 1, '2') {
		t.Fatal("fixture: the epoch-2 ship was refused before the restart")
	}
	if !g1Barrier(t, first, g1Lineage(1, 2)) {
		t.Fatal("fixture: no epoch-2 barrier ACK before the restart")
	}
	first.StopHard() // restart: the process dies with connections open

	second := g1Listener(t, store, floor) // restart, no assignment yet
	if g1Ship(t, second, store, g1Lineage(2, 2), 2, 'u') || g1Barrier(t, second, g1Lineage(2, 2)) || g1Rebuild(t, second, store, g1Lineage(2, 2), 'r') {
		t.Fatal("G1: a restarted replica accepted a mutation before its assignment was known")
	}

	if err := second.AdmitAsReplica(1); err != nil { // a stale assignment
		t.Fatal(err)
	}
	if g1Ship(t, second, store, g1Lineage(3, 1), 3, '1') {
		t.Fatal("G1: after a restart the replica applied an epoch-1 ship below the epoch 2 it had acknowledged")
	}
	if g1Barrier(t, second, g1Lineage(3, 1)) {
		t.Fatal("G1: after a restart the replica ACKed an epoch-1 barrier below its acknowledged epoch 2")
	}
	if !g1Ship(t, second, store, g1Lineage(4, 2), 4, 'c') || !g1Barrier(t, second, g1Lineage(4, 2)) {
		t.Fatal("the current epoch-2 ship/barrier was refused after the role was known")
	}
}

// An epoch first reached by a barrier alone (no ship) is persisted before the
// barrier ACK and survives a restart.
func TestG1_BarrierOnlyEpochSurvivesRestart(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	first := g1Listener(t, store, floor)
	if err := first.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	if !g1Barrier(t, first, g1Lineage(1, 2)) {
		t.Fatal("fixture: the epoch-2 barrier was refused")
	}
	if got := g1Persisted(t, floor); got != 2 {
		t.Fatalf("persisted floor after the epoch-2 barrier ACK = %d, want 2", got)
	}
	first.StopHard() // restart: the process dies with connections open
	second := g1Listener(t, store, floor)
	if err := second.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	if g1Ship(t, second, store, g1Lineage(2, 1), 1, '1') {
		t.Fatal("an epoch-1 ship was applied below a barrier-acknowledged epoch 2 after restart")
	}
}

// A freshly created volume loads its bootstrap floor (epoch 0) at restart,
// stays unassigned and refuses every mutation, and a successful current
// assignment then admits valid traffic (codex01 round 3 positive).
func TestG1_FreshVolumeRestartBeforeAssignment(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	if got := g1Persisted(t, floor); got != 0 {
		t.Fatalf("bootstrap floor = %d, want 0", got)
	}
	ln := g1Listener(t, store, floor) // restart right after creation
	if g1Ship(t, ln, store, g1Lineage(1, 1), 1, 'x') || g1Barrier(t, ln, g1Lineage(1, 1)) {
		t.Fatal("an unassigned listener on a fresh volume accepted a mutation")
	}
	if err := ln.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	if !g1Ship(t, ln, store, g1Lineage(1, 1), 2, 'a') || !g1Barrier(t, ln, g1Lineage(1, 1)) {
		t.Fatal("the current assignment did not admit valid traffic")
	}
}

// Creation order: the record is made before the volume, a crash in between
// keeps it, and a second InitReplicaFloor never replaces an existing record.
func TestG1_InitKeepsAnExistingRecord(t *testing.T) {
	floor := g1Fresh(t)
	if err := persistFloor(floor, g1Vol, 5); err != nil {
		t.Fatal(err)
	}
	if err := InitReplicaFloor(floor, g1Vol); err != nil {
		t.Fatalf("re-init over an existing record: %v", err)
	}
	if got := g1Persisted(t, floor); got != 5 {
		t.Fatalf("re-init replaced the record: floor %d, want 5", got)
	}
}

// Every invalid or missing record refuses construction; nothing is read as 0.
func TestG1_InvalidOrMissingRecordRefusesTheListener(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	write := func(t *testing.T, body string) string {
		p := filepath.Join(t.TempDir(), "f")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ok := `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":3}`
	cases := map[string]string{
		"truncated":     ok[:30],
		"malformed":     `not json`,
		"unknown field": `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":3,"x":1}`,
		"duplicate key": `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":3,"epoch":9}`,
		"trailing data": ok + `{}`,
		"wrong volume":  `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol2","epoch":3}`,
		"wrong schema":  `{"schema":"other/v1","volume_id":"vol1","epoch":3}`,
		"missing field": `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1"}`,
		"bad epoch":     `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":-1}`,
		"oversized":     ok + string(bytes.Repeat([]byte(" "), floorRecordMaxBytes)),
		// codex01 R2 on #83: case variants are not the same key.
		"uppercase keys":            `{"SCHEMA":"seaweed-block.replica-floor/v1","VOLUME_ID":"vol1","EPOCH":3}`,
		"case-folded duplicate":     `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":7,"EPOCH":0}`,
		"case-folded duplicate rev": `{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","Epoch":0,"epoch":7}`,
		"case-folded schema":        `{"Schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":3}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewReplicaListenerWithFloor("127.0.0.1:0", store, g1Vol, write(t, body)); err == nil {
				t.Fatal("constructed a listener from an invalid floor record")
			}
		})
	}
	if _, err := NewReplicaListenerWithFloor("127.0.0.1:0", store, g1Vol, write(t, ok)); err != nil {
		t.Fatalf("a valid record was refused: %v", err)
	}
	if e, err := decodeFloor([]byte(`{"schema":"seaweed-block.replica-floor/v1","volume_id":"vol1","epoch":0}`), g1Vol); err != nil || e != 0 {
		t.Fatalf("the canonical bootstrap record (epoch 0) was refused: %d %v", e, err)
	}

	// An existing volume whose record is missing, even an empty one whose only
	// history is a barrier ACK, refuses: missing is never fresh.
	floor := g1Fresh(t)
	empty := storage.NewBlockStore(16, 4096)
	ln := g1Listener(t, empty, floor)
	if err := ln.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	if !g1Barrier(t, ln, g1Lineage(1, 2)) {
		t.Fatal("fixture: barrier refused")
	}
	ln.StopHard() // restart: the process dies with connections open
	if err := os.Remove(floor); err != nil {
		t.Fatal(err)
	}
	if _, err := NewReplicaListenerWithFloor("127.0.0.1:0", empty, g1Vol, floor); !errors.Is(err, ErrFloorRecordMissing) {
		t.Fatalf("existing empty volume with its record removed: err=%v, want ErrFloorRecordMissing", err)
	}
}

// failAt makes one persistence step fail (the rest behave as on disk).
func failAt(t *testing.T, step string) {
	t.Helper()
	ops := osFloorOps
	boom := errors.New("injected " + step + " failure")
	switch step {
	case "write":
		ops.writeSynced = func(string, []byte) error { return boom }
	case "rename":
		ops.rename = func(string, string) error { return boom }
	case "dirsync":
		ops.syncDir = func(string) error { return boom }
	default:
		t.Fatalf("unknown step %q", step)
	}
	floorFS = ops
	t.Cleanup(func() { floorFS = osFloorOps })
}

// A mutation that would raise the floor is refused, with nothing applied and
// no ACK, when any persistence step fails (including after the rename,
// before the directory sync).
func TestG1_PersistFailureRefusesTheRaisingMutation(t *testing.T) {
	for _, step := range []string{"write", "rename", "dirsync"} {
		t.Run(step, func(t *testing.T) {
			store := storage.NewBlockStore(16, 4096)
			floor := g1Fresh(t)
			ln := g1Listener(t, store, floor)
			if err := ln.AdmitAsReplica(1); err != nil {
				t.Fatal(err)
			}
			failAt(t, step)
			if g1Ship(t, ln, store, g1Lineage(1, 2), 1, 's') {
				t.Fatal("a ship raising the floor was applied though the floor write failed")
			}
			if g1Barrier(t, ln, g1Lineage(2, 2)) {
				t.Fatal("a barrier raising the floor was ACKed though the floor write failed")
			}
			if g1Rebuild(t, ln, store, g1Lineage(3, 2), 'r') {
				t.Fatal("a rebuild raising the floor was written though the floor write failed")
			}
			floorFS = osFloorOps
			if !g1Ship(t, ln, store, g1Lineage(4, 1), 2, 'o') {
				t.Fatal("a ship at the current floor was refused")
			}
		})
	}
}

// The role calls persist before they return; a persist failure is returned
// and nothing changes (an unassigned listener stays unassigned).
func TestG1_RoleCallsPersistBeforeReturning(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	ln := g1Listener(t, store, floor)

	failAt(t, "rename")
	if err := ln.AdmitAsReplica(3); err == nil {
		t.Fatal("AdmitAsReplica returned nil though its floor write failed")
	}
	if err := ln.FenceAsPrimary(3); err == nil {
		t.Fatal("FenceAsPrimary returned nil though its floor write failed (after a successful drain)")
	}
	floorFS = osFloorOps
	if g1Persisted(t, floor) != 0 {
		t.Fatal("a failed role call changed the record")
	}

	// Fence at 3, then "crash" before the host adopts the role: the floor
	// is already durable and survives the restart.
	if err := ln.FenceAsPrimary(3); err != nil {
		t.Fatal(err)
	}
	ln.StopHard() // restart: the process dies with connections open
	if got := g1Persisted(t, floor); got != 3 {
		t.Fatalf("floor after fence-then-crash = %d, want 3", got)
	}
	again := g1Listener(t, store, floor)
	if err := again.AdmitAsReplica(2); err != nil {
		t.Fatal(err)
	}
	if g1Ship(t, again, store, g1Lineage(1, 2), 1, 'z') {
		t.Fatal("an epoch-2 ship was applied below the fenced epoch 3 after restart")
	}
}

// Concurrent raises are serialized and monotonic: a lower write that is
// delayed past a higher one never lowers the record.
func TestG1_ConcurrentRaisesNeverLowerTheRecord(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	ln := g1Listener(t, store, floor)

	// The epoch-2 write blocks inside its rename until epoch 3 has tried.
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	ops := osFloorOps
	ops.rename = func(from, to string) error {
		if e, _ := decodeFloorFile(from); e == 2 {
			entered <- struct{}{}
			<-release
		}
		return os.Rename(from, to)
	}
	floorFS = ops
	t.Cleanup(func() { floorFS = osFloorOps })

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = ln.AdmitAsReplica(2) }()
	<-entered
	go func() { defer wg.Done(); _ = ln.AdmitAsReplica(3) }()
	time.Sleep(50 * time.Millisecond) // epoch 3 is now queued behind floorMu
	close(release)
	wg.Wait()
	if got := g1Persisted(t, floor); got != 3 {
		t.Fatalf("record after concurrent raises 2 then 3 = %d, want 3", got)
	}
	if err := ln.AdmitAsReplica(2); err != nil || g1Persisted(t, floor) != 3 {
		t.Fatalf("a later lower admission lowered the record (err=%v)", err)
	}
}

func decodeFloorFile(path string) (uint64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return decodeFloor(raw, g1Vol)
}

// Probes are non-mutating: they never raise or persist the floor.
func TestG1_ProbesNeverPersistTheFloor(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floor := g1Fresh(t)
	ln := g1Listener(t, store, floor)
	if err := ln.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	conn := g2Dial(t, ln.Addr())
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := WriteMsg(conn, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: g1Lineage(9, 7)})); err != nil {
		t.Fatal(err)
	}
	_, _, _ = ReadMsg(conn)
	if got := g1Persisted(t, floor); got != 1 {
		t.Fatalf("a probe at epoch 7 changed the persisted floor to %d", got)
	}
}
