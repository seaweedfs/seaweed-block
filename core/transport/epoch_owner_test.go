package transport

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// The invalid mark belongs to the last recovery that started, by a random
// token: an older recovery's completion cannot make the table valid over a
// newer one's data, and after a reload a dead recovery's token stays refused.
func TestEpochTable_OnlyTheMarksOwnerCompletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.epoch-table")
	tb, _ := LoadEpochTable(path, "v1")
	_ = tb.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	a, err := tb.Invalidate() // recovery A
	if err != nil {
		t.Fatal(err)
	}
	b, err := tb.Invalidate() // recovery B takes the mark over
	if err != nil || b == a || b == 0 {
		t.Fatalf("tokens a=%d b=%d err=%v; want two distinct random tokens", a, b, err)
	}
	if err := tb.CompleteRecovery([]EpochRow{{Epoch: 1, FirstLSN: 1}}, a); !errors.Is(err, ErrNotRecoveryOwner) {
		t.Fatalf("A's late completion over B's mark: %v; want ErrNotRecoveryOwner", err)
	}
	if !tb.Invalid() {
		t.Fatal("A's refused completion made the table valid")
	}
	reloaded, _ := LoadEpochTable(path, "v1")
	if !reloaded.Invalid() {
		t.Fatal("after a reload the table is valid")
	}
	if err := reloaded.CompleteRecovery([]EpochRow{{Epoch: 1, FirstLSN: 1}}, a); !errors.Is(err, ErrNotRecoveryOwner) {
		t.Fatal("after a reload A's token was accepted")
	}
	if err := reloaded.CompleteRecovery([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 6}}, b); err != nil {
		t.Fatalf("B's own completion: %v", err)
	}
	if reloaded.Invalid() || len(reloaded.Rows()) != 2 {
		t.Fatalf("after B's completion: invalid %t rows %v", reloaded.Invalid(), reloaded.Rows())
	}
	if err := reloaded.CompleteRecovery(nil, b); !errors.Is(err, ErrNotRecoveryOwner) {
		t.Fatal("a second completion of a valid table was accepted")
	}
	c, err := reloaded.Invalidate()
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []uint64{0, b} {
		if err := reloaded.CompleteRecovery(nil, tok); !errors.Is(err, ErrNotRecoveryOwner) {
			t.Fatalf("completion with token %d cleared recovery %d's mark", tok, c)
		}
	}
	if err := reloaded.Note(3, 20); err == nil {
		t.Fatal("a note on an invalid table was accepted")
	}
}

// The schedule on the real listener: recovery A starts on one connection,
// recovery B starts on another and takes the mark; A's final barrier carries
// the primary's rows and must not make the table valid.
func TestOverlappingRecoveriesAStaleCompletionLeavesTheTableInvalid(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	theirs, _ := LoadEpochTable(filepath.Join(t.TempDir(), "v1.epoch-table"), "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(theirs)
	owner := func() uint64 { theirs.mu.Lock(); defer theirs.mu.Unlock(); return theirs.owner }

	a := RecoveryLineage{SessionID: 5, Epoch: 2, EndpointVersion: 1, TargetLSN: 10}
	b := RecoveryLineage{SessionID: 6, Epoch: 2, EndpointVersion: 1, TargetLSN: 10}
	connA, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	if err := WriteMsg(connA, MsgRecoveryLaneStart, EncodeLineage(a)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return theirs.Invalid() })
	tokenA := owner()
	connB, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Close()
	if err := WriteMsg(connB, MsgRecoveryLaneStart, EncodeLineage(b)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return owner() != tokenA })

	req := append(EncodeLineage(a), EncodeEpochRows([]EpochRow{{Epoch: 1, FirstLSN: 1}})...)
	if err := WriteMsg(connA, MsgBarrierReq, req); err != nil {
		t.Fatal(err)
	}
	_ = connA.SetReadDeadline(time.Now().Add(2 * time.Second))
	if typ, _, err := ReadMsg(connA); err == nil && typ == MsgBarrierResp {
		t.Fatal("recovery A's stale completion was acknowledged")
	}
	if !theirs.Invalid() {
		t.Fatalf("recovery A's stale completion made the table valid: rows %v", theirs.Rows())
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
