package lifecycle

import (
	"sync"
	"testing"
	"time"
)

// INV-WS-RENEW-NONBLOCKING-001 + INV-AUTH-HOLDER-IDENTITY-001: the holder host check reads the node registry from a
// committed snapshot, so a lease renewal never waits behind a registration write (which holds the store mutex across
// file I/O). The snapshot is a copy, survives a reopen, and a removed node is unpublished.
func TestNodeSnapshotDoesNotWaitForARegistrationWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenNodeInventoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterNode(NodeRegistration{ServerID: "node-a", DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102"}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock() // the mutex RegisterNode holds across file I/O: a held registration write
	var once sync.Once
	release := func() { once.Do(s.mu.Unlock) }
	result := make(chan NodeRegistration, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("snapshot reader did not join")
		}
	})
	go func() { defer close(joined); r, _ := s.GetNodeSnapshot("node-a"); result <- r }()
	var got NodeRegistration
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("node snapshot waited for a held registration write")
	}
	// control: the mutex-taking read does wait while the write is held
	blocked := make(chan struct{})
	go func() { _, _ = s.GetNode("node-a"); close(blocked) }()
	t.Cleanup(func() {
		release()
		select {
		case <-blocked:
		case <-time.After(5 * time.Second):
			t.Error("control reader did not join")
		}
	})
	select {
	case <-blocked:
		t.Fatal("control: GetNode did not wait for the held registration write")
	case <-time.After(200 * time.Millisecond):
	}
	release()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("control reader did not finish after the release")
	}
	if got.DataAddr != "127.0.0.1:19101" {
		t.Fatalf("wrong committed registration: %+v", got)
	}
	got.DataAddr = "changed-by-reader"
	if again, _ := s.GetNodeSnapshot("node-a"); again.DataAddr != "127.0.0.1:19101" {
		t.Fatal("caller mutated the published registration")
	}
	reopened, err := OpenNodeInventoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := reopened.GetNodeSnapshot("node-a"); !ok || r.DataAddr != "127.0.0.1:19101" {
		t.Fatal("reopened snapshot lost the registration")
	}
	if err := s.RemoveNode("node-a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetNodeSnapshot("node-a"); ok {
		t.Fatal("removed node remained published")
	}
}
