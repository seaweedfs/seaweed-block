package authority

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type deleteGrantStore struct{ failingStore }

func (*deleteGrantStore) LoadWorkspaceLostRanges() (map[string][]WorkspaceLostRange, []error, error) {
	return map[string][]WorkspaceLostRange{}, nil, nil
}

type skippedDeleteStore struct{ deleteGrantStore }

func (*skippedDeleteStore) LoadWithSkips() ([]DurableRecord, []error, error) {
	return nil, []error{errors.New("corrupt authority record")}, nil
}

func TestDeleteCannotProveNoLeaseFromASkippedAuthorityRecord(t *testing.T) {
	p := NewPublisher(nil, WithStore(&skippedDeleteStore{}))
	if err := p.ReserveWorkspaceDeletion(context.Background(), "v", nil); err != nil {
		t.Fatal(err)
	}
	if err := p.NoLiveWorkspaceLease(context.Background(), "v"); err == nil {
		t.Fatal("a partial authority load proved lease absence")
	}
}

func TestCommittedDeleteStopsGrantsAndWaitsForTheWholeLease(t *testing.T) {
	ctx := context.Background()
	c := newTestClock()
	p := newLeasePublisher(t, &deleteGrantStore{}, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	if err := p.ReserveWorkspaceDeletion(ctx, "v", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err == nil {
		t.Fatal("committed delete extended a holder lease")
	}
	// First horizon is 45 s, plus the default skew of 20 s. Host validity
	// expiring earlier is insufficient evidence for deleting remote data.
	c.at(64)
	if err := p.NoLiveWorkspaceLease(ctx, "v"); err == nil || !strings.Contains(err.Error(), "live until") {
		t.Fatalf("before durable horizon + skew: %v", err)
	}
	c.at(65)
	if err := p.NoLiveWorkspaceLease(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err == nil {
		t.Fatal("expired regrant reopened a committed delete")
	}
}

func TestDeleteReservationMustBeDurableBeforeItCanProveNoLease(t *testing.T) {
	p := NewPublisher(nil)
	errDisk := errors.New("reservation sync failed")
	if err := p.ReserveWorkspaceDeletion(context.Background(), "v", func() error { return errDisk }); !errors.Is(err, errDisk) {
		t.Fatalf("persist error: %v", err)
	}
	if err := p.NoLiveWorkspaceLease(context.Background(), "v"); err == nil {
		t.Fatal("a failed reservation proved no lease")
	}
}

func TestDeleteReservationPersistenceDoesNotBlockLiveRenewal(t *testing.T) {
	c := newTestClock()
	p := newLeasePublisher(t, &deleteGrantStore{}, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	entered, release, reserved := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		reserved <- p.ReserveWorkspaceDeletion(context.Background(), "v", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	renewed := make(chan error, 1)
	go func() { _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); renewed <- err }()
	select {
	case err := <-renewed:
		close(release)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("live renewal waited on reservation disk I/O")
	}
	if err := <-reserved; err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); !errors.Is(err, ErrWorkspaceDeleteReserved) {
		t.Fatal(err)
	}
}

func TestDeleteReservationClosesPausedGrantWriters(t *testing.T) {
	ctx := context.Background()
	for _, site := range []string{"publication", "first grant", "regrant", "renewal", "extension", "move"} {
		t.Run(site, func(t *testing.T) {
			c := newTestClock()
			store := &deleteGrantStore{}
			p := newLeasePublisher(t, store, 30*time.Second, c)
			if site != "publication" {
				if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
					t.Fatal(err)
				}
				if site != "first grant" {
					if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
						t.Fatal(err)
					}
				}
			}
			if site == "regrant" || site == "move" {
				c.at(100)
			}
			paused, resume := make(chan struct{}), make(chan struct{})
			p.testBeforeGrantLock = func(at string) {
				if at == site || (site == "move" && at == "publication") {
					close(paused)
					<-resume
				}
			}
			done := make(chan error, 1)
			go func() {
				switch site {
				case "publication":
					done <- p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind})
				case "extension":
					p.extWG.Add(1)
					p.extendWorkspaceLease("v", "r", 1, c.sec(100))
					done <- nil
				case "move":
					_, err := p.MoveWorkspace(ctx, WorkspaceMoveRequest{RequestID: "m", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "a", TargetHost: "b", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}, WorkspaceHead{Gen: 1, Epoch: 1, Holder: "a/r"})
					done <- err
				default:
					_, err := p.RenewWorkspaceLease(ctx, "v", "r", 1)
					done <- err
				}
			}()
			select {
			case <-paused:
			case err := <-done:
				t.Fatalf("request did not reach %s boundary: %v", site, err)
			}
			if err := p.ReserveWorkspaceDeletion(ctx, "v", func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			c.at(100)
			if err := p.NoLiveWorkspaceLease(ctx, "v"); err != nil {
				t.Fatal(err)
			}
			writes := len(store.records)
			close(resume)
			err := <-done
			if site != "extension" && !errors.Is(err, ErrWorkspaceDeleteReserved) {
				t.Fatalf("paused %s: %v", site, err)
			}
			if len(store.records) != writes {
				t.Fatalf("paused %s wrote after the no-live-lease proof", site)
			}
			if err := p.NoLiveWorkspaceLease(ctx, "v"); err != nil {
				t.Fatalf("paused %s invalidated the proof: %v", site, err)
			}
		})
	}
}

func TestDeleteReservationReplayAndRestart(t *testing.T) {
	ctx := context.Background()
	c := newTestClock()
	store := &deleteGrantStore{}
	p := newLeasePublisher(t, store, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	writes := 0
	persist := func() error { writes++; return nil }
	if err := p.ReserveWorkspaceDeletion(ctx, "v", persist); err != nil {
		t.Fatal(err)
	}
	if err := p.ReserveWorkspaceDeletion(ctx, "v", persist); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("reservation persisted %d times", writes)
	}
	// The fake preserves the latest complete authority record; the lifecycle
	// reservation is independently reloaded by master.New before serving.
	last := store.records[len(store.records)-1]
	c.at(100)
	loaded := newLeasePublisher(t, &deleteGrantStore{failingStore: failingStore{records: []DurableRecord{last}}}, 30*time.Second, c)
	if err := loaded.ReserveWorkspaceDeletion(ctx, "v", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.RenewWorkspaceLease(ctx, "v", "r", 1); !errors.Is(err, ErrWorkspaceDeleteReserved) {
		t.Fatal(err)
	}
	// A restart conservatively starts a fresh TTL deadline, even though the
	// old persisted horizon has passed. It too must expire with the skew.
	c.at(149)
	if err := loaded.NoLiveWorkspaceLease(ctx, "v"); err == nil {
		t.Fatal("restart deadline was ignored")
	}
	c.at(150)
	if err := loaded.NoLiveWorkspaceLease(ctx, "v"); err != nil {
		t.Fatal(err)
	}
}
