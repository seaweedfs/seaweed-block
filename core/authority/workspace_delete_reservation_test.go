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

// A delete of a volume still served is refused before anything is
// persisted, and the holder keeps renewing: a mistaken delete is not an
// outage. Once the lease has lapsed the reservation commits.
func TestDeleteOfALiveVolumeIsRefusedBeforePersisting(t *testing.T) {
	ctx := context.Background()
	c := newTestClock()
	p := newLeasePublisher(t, &deleteGrantStore{}, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	persisted := false
	if err := p.ReserveWorkspaceDeletion(ctx, "v", func() error { persisted = true; return nil }); !errors.Is(err, ErrWorkspaceDeleteRefused) || !strings.Contains(err.Error(), "live until") || persisted {
		t.Fatalf("reserve of a live volume: %v (persisted %t); want refused, nothing persisted", err, persisted)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatalf("the holder's renewal after a refused delete: %v", err)
	}
	c.at(1000)
	if err := p.ReserveWorkspaceDeletion(ctx, "v", func() error { persisted = true; return nil }); err != nil || !persisted {
		t.Fatalf("reserve after the lease lapsed: %v (persisted %t)", err, persisted)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err == nil {
		t.Fatal("a committed delete extended a holder lease")
	}
}

// A committed reservation reloaded at startup (persist nil) while a lease
// record exists stops every grant and waits for the whole lease.
func TestCommittedDeleteStopsGrantsAndWaitsForTheWholeLease(t *testing.T) {
	ctx := context.Background()
	c := newTestClock()
	p := newLeasePublisher(t, &deleteGrantStore{}, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	if err := p.ReserveWorkspaceDeletion(ctx, "v", nil); err != nil {
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
	// Another volume's holder renews while v's reservation is on disk.
	bindAndGrant(t, p, "w", "r")
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
	go func() { _, err := p.RenewWorkspaceLease(context.Background(), "w", "r", 1); renewed <- err }()
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
			// The delete is admitted once the old lease has lapsed (a live one
			// refuses it early); the paused writer must still be closed.
			c.at(100)
			if err := p.ReserveWorkspaceDeletion(ctx, "v", func() error { return nil }); err != nil {
				close(resume)
				t.Fatal(err)
			}
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
	c.at(70) // the lease has lapsed: a live one refuses the delete early
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

func TestDeleteReservationContextAndPersistenceFailuresStayUnknown(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()
	disk := errors.New("disk response lost")
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		failure error
	}{
		{"cancel", canceled, context.Canceled},
		{"deadline", expired, context.DeadlineExceeded},
		{"persist", context.Background(), disk},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newLeasePublisher(t, &deleteGrantStore{}, 30*time.Second, newTestClock())
			err := p.ReserveWorkspaceDeletion(tc.ctx, "v", func() error { return disk })
			if !errors.Is(err, tc.failure) || errors.Is(err, ErrWorkspaceDeleteRefused) {
				t.Fatalf("failure classification=%v", err)
			}
			if err := p.NoLiveWorkspaceLease(context.Background(), "v"); err == nil {
				t.Fatal("failed reservation proved lease absence")
			}
		})
	}
}
