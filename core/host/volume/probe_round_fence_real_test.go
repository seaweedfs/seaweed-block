package volume

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// The fencing round on the real path: the host's round answer drives a real
// transport.ReplicaListener whose floor lives in a real floor record, and the
// old primary's frames reach that listener over TCP.

const realFenceVol = "v1"

// realProbeLineage is the transient lineage of the handshake probe that
// follows a ship (a probe is not a mutation and raises no floor).
var realProbeLineage = transport.RecoveryLineage{SessionID: 1, Epoch: 1 << 62, EndpointVersion: 1, TargetLSN: 1}

// realEvidence is the replication slot over a real listener: its store, and
// the listener's durable floor at each evidence read.
type realEvidence struct {
	id      storage.StoreID
	ln      *transport.ReplicaListener
	end     uint64
	readsAt []uint64
}

func (e *realEvidence) UpdateReplicaSet(uint64, []replication.ReplicaTarget) error { return nil }
func (e *realEvidence) LocalStoreID() storage.StoreID                              { return e.id }
func (e *realEvidence) LocalPrimaryEvidence() (replication.EpochTableStatus, uint64, error) {
	e.readsAt = append(e.readsAt, e.ln.DurableFloor())
	return replication.EpochTableStatus{}, e.end, nil
}

func realFloorRecord(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "floors", realFenceVol+".replica-floor")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := transport.InitReplicaFloor(path, realFenceVol); err != nil {
		t.Fatal(err)
	}
	return path
}

// realFenceHost is a replica host at epoch admitted on a real listener over
// store and the floor record at path.
func realFenceHost(t *testing.T, store storage.LogicalStorage, path string, epoch uint64) (*Host, *transport.ReplicaListener, *realEvidence) {
	t.Helper()
	ln, err := transport.NewReplicaListenerWithFloor("127.0.0.1:0", store, realFenceVol, path)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()
	t.Cleanup(ln.StopHard)
	if err := ln.AdmitAsReplica(epoch); err != nil {
		t.Fatal(err)
	}
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "s2", VolumeID: realFenceVol, ReplicaID: "r2", DataAddr: "127.0.0.1:19313", CtrlAddr: "127.0.0.1:19314", EnableT1Readiness: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.SetSelfFence(ln); err != nil {
		t.Fatal(err)
	}
	ev := &realEvidence{id: storage.StoreID{7, 7, 7}, ln: ln, end: 42}
	h.replicationMu.Lock()
	h.replication = ev
	h.replicationMu.Unlock()
	return h, ln, ev
}

func realFencingRound(id, fence uint64, ev *realEvidence) *control.ProbeRound {
	return &control.ProbeRound{VolumeId: realFenceVol, ReplicaId: "r2", RoundId: id, FenceEpoch: fence, ExpectedStoreId: ev.id[:]}
}

// realShip sends one epoch ship from an old primary over TCP and reports
// whether the replica applied it (the probe handshake after the ship: an
// accepted ship is applied before the probe is answered; a refused one
// closes the connection).
// It ships at the store's next LSN, the only one a replica applies inside
// an epoch.
func realShip(t *testing.T, ln *transport.ReplicaListener, store storage.LogicalStorage, session, epoch uint64, fill byte) bool {
	t.Helper()
	lsn := store.NextLSN()
	l := transport.RecoveryLineage{SessionID: session, Epoch: epoch, EndpointVersion: 1, TargetLSN: lsn}
	data := bytes.Repeat([]byte{fill}, 4096)
	conn, err := net.DialTimeout("tcp", ln.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := transport.WriteMsg(conn, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: l, LBA: 0, LSN: lsn, Data: data})); err != nil {
		t.Fatal(err)
	}
	accepted := transport.WriteMsg(conn, transport.MsgProbeReq, transport.EncodeProbeReq(transport.ProbeRequest{Lineage: realProbeLineage})) == nil
	if accepted {
		if _, _, err := transport.ReadMsg(conn); err != nil {
			accepted = false
		}
	}
	got, err := store.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	applied := bytes.Equal(got, data)
	if accepted != applied {
		t.Fatalf("epoch-%d ship: handshake accepted=%v but bytes applied=%v", epoch, accepted, applied)
	}
	return applied
}

// refuse <= e: once the host has answered a fencing round at e+1, the old
// primary's frames at e (and below) are refused, here and after a restart;
// the line re-issued at e+1 is admitted. The control shows the same frame
// was admitted before the round.
func TestFencingRoundOnTheRealListenerRefusesTheOldEpochAfterTheAnswer(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	path := realFloorRecord(t)
	h, ln, ev := realFenceHost(t, store, path, 3)
	if !realShip(t, ln, store, 1, 3, 'a') {
		t.Fatal("control: the epoch-3 primary's ship was refused before the fencing round")
	}
	a, ok := h.roundAnswer(realFencingRound(10, 4, ev))
	if !ok || a.GetFloor() != 4 {
		t.Fatalf("fencing round at 4: answered=%v floor=%d; want an answer at floor 4", ok, a.GetFloor())
	}
	if len(ev.readsAt) != 1 || ev.readsAt[0] != 4 {
		t.Fatalf("evidence read at durable floors %v; want [4] (read after the durable raise)", ev.readsAt)
	}
	if got, err := transport.LoadFloorRecord(path, realFenceVol); err != nil || got != 4 {
		t.Fatalf("floor record after the answer: %d err=%v; want 4", got, err)
	}
	if realShip(t, ln, store, 2, 3, 'b') || realShip(t, ln, store, 3, 2, 'c') {
		t.Fatal("after the answer at floor 4 the replica applied a ship at epoch 3 or 2")
	}
	ln.StopHard()

	_, restarted, _ := realFenceHost(t, store, path, 3) // the stale line, again
	if realShip(t, restarted, store, 4, 3, 'd') {
		t.Fatal("after a restart the replica applied an epoch-3 ship below its answered floor 4")
	}
	if err := restarted.AdmitAsReplica(4); err != nil {
		t.Fatal(err)
	}
	if !realShip(t, restarted, store, 5, 4, 'e') {
		t.Fatal("the line re-issued at the fenced epoch 4 was refused")
	}
}

// A superseded round: the replica's durable floor is already above the
// round's fence epoch. The answer carries that floor (the master does not
// count it), the floor is not lowered, and the round's own epoch stays
// refused.
func TestFencingRoundOnTheRealListenerAnswersASupersededFloor(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	path := realFloorRecord(t)
	h, ln, ev := realFenceHost(t, store, path, 5)
	a, ok := h.roundAnswer(realFencingRound(11, 4, ev))
	if !ok {
		t.Fatal("a superseded fencing round was not answered")
	}
	if a.GetFloor() != 5 {
		t.Fatalf("superseded round at 4: answer floor %d; want 5 (above the fence: not counted)", a.GetFloor())
	}
	if got, err := transport.LoadFloorRecord(path, realFenceVol); err != nil || got != 5 {
		t.Fatalf("floor record after a superseded round: %d err=%v; want 5 (never lowered)", got, err)
	}
	if realShip(t, ln, store, 1, 4, 'a') {
		t.Fatal("after a superseded round at 4 the floor-5 replica applied an epoch-4 ship")
	}
}

// Crash before the persist: the floor record cannot be written (its
// directory is read-only), so the fence makes no promise and the round is
// not answered. After the crash the record still holds the old floor (no
// restart is bound by a promise that was never made), and the round asked
// again completes and answers at the fence epoch.
func TestFencingRoundCrashBeforePersistIsNotAnsweredAndBindsNoRestart(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	path := realFloorRecord(t)
	dir := filepath.Dir(path)
	h, ln, ev := realFenceHost(t, store, path, 3)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if f, err := os.CreateTemp(dir, "probe"); err == nil {
		f.Close()
		os.Remove(f.Name())
		t.Skip("the floor directory is still writable (running as root?): the persist failure cannot be produced")
	}
	if a, ok := h.roundAnswer(realFencingRound(12, 4, ev)); ok {
		t.Fatalf("a fencing round whose floor could not be persisted was answered: %+v", a)
	}
	if len(ev.readsAt) != 0 {
		t.Fatalf("the evidence was read (at floors %v) though no promise was made", ev.readsAt)
	}
	ln.StopHard() // the crash
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if got, err := transport.LoadFloorRecord(path, realFenceVol); err != nil || got != 3 {
		t.Fatalf("record after the crash: %d err=%v; want 3 (nothing was promised)", got, err)
	}
	h2, restarted, ev2 := realFenceHost(t, store, path, 3)
	if !realShip(t, restarted, store, 1, 3, 'a') {
		t.Fatal("control: with no promise made, the restarted replica refused the epoch-3 ship")
	}
	a, ok := h2.roundAnswer(realFencingRound(13, 4, ev2))
	if !ok || a.GetFloor() != 4 {
		t.Fatalf("the round asked again after the crash: answered=%v floor=%d; want floor 4", ok, a.GetFloor())
	}
	if realShip(t, restarted, store, 2, 3, 'b') {
		t.Fatal("after the re-asked round's answer an epoch-3 ship was applied")
	}
}

// Crash after the persist but before the answer: the floor is durable but no
// answer left. A restart is bound by it (the old epoch is refused), and the
// round asked again answers at the same floor without raising anything.
func TestFencingRoundCrashAfterPersistBeforeTheAnswer(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	path := realFloorRecord(t)
	_, ln, _ := realFenceHost(t, store, path, 3)
	if err := ln.FenceFloor(4); err != nil { // roundAnswer's fence step; the crash comes before the evidence and the answer
		t.Fatal(err)
	}
	ln.StopHard() // the crash
	if got, err := transport.LoadFloorRecord(path, realFenceVol); err != nil || got != 4 {
		t.Fatalf("record after a crash after the persist: %d err=%v; want 4", got, err)
	}
	h, restarted, ev := realFenceHost(t, store, path, 3)
	if realShip(t, restarted, store, 1, 3, 'a') {
		t.Fatal("restarted on a durable floor 4, the replica applied an epoch-3 ship")
	}
	a, ok := h.roundAnswer(realFencingRound(14, 4, ev))
	if !ok || a.GetFloor() != 4 {
		t.Fatalf("the round asked again: answered=%v floor=%d; want floor 4", ok, a.GetFloor())
	}
	if len(ev.readsAt) != 1 || ev.readsAt[0] != 4 {
		t.Fatalf("evidence read at %v; want [4]", ev.readsAt)
	}
}
