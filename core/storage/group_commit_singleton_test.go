package storage

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupCommitSingletonStartsWithoutCollectionDelay(t *testing.T) {
	started := make(chan struct{}, 1)
	gc := NewGroupCommitter(GroupCommitterConfig{MaxDelay: 5 * time.Second, SyncFunc: func() error { started <- struct{}{}; return nil }})
	go gc.Run()
	defer gc.Stop()
	result := make(chan error, 1)
	go func() { result <- gc.SyncCache() }()
	select {
	case <-started:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("singleton waited for collection timer before physical sync")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func waitCommitPending(t *testing.T, gc *GroupCommitter, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		gc.mu.Lock()
		n := len(gc.pending)
		gc.mu.Unlock()
		if n == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("pending queue did not reach %d", want)
}

func TestGroupCommitSingletonFollowersRequireNextPhysicalSync(t *testing.T) {
	firstStarted, secondStarted := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	failed := errors.New("second physical sync failed")
	var calls atomic.Int32
	gc := NewGroupCommitter(GroupCommitterConfig{MaxDelay: 10 * time.Millisecond, SyncFunc: func() error {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-releaseFirst
			return nil
		case 2:
			close(secondStarted)
			<-releaseSecond
			return failed
		default:
			return errors.New("unexpected physical sync")
		}
	}})
	go gc.Run()
	defer gc.Stop()
	defer func() {
		select {
		case <-releaseSecond:
		default:
			close(releaseSecond)
		}
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	}()
	leader := make(chan error, 1)
	go func() { leader <- gc.SyncCache() }()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("leader did not start")
	}
	followers := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { followers <- gc.SyncCache() }()
	}
	waitCommitPending(t, gc, 8)
	select {
	case err := <-followers:
		t.Fatal("ack before physical sync", err)
	default:
	}
	close(releaseFirst)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("follower batch did not start")
	}
	select {
	case err := <-followers:
		t.Fatal("followers used preceding sync", err)
	default:
	}
	close(releaseSecond)
	for i := 0; i < 8; i++ {
		if err := <-followers; !errors.Is(err, failed) {
			t.Fatal("physical error not propagated", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("physical syncs=%d", calls.Load())
	}
}
