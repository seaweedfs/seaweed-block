package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
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
	if err := receiveCommitResult(t, result, time.After(2*time.Second)); err != nil {
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
	gc := NewGroupCommitter(GroupCommitterConfig{MaxDelay: 5 * time.Second, SyncFunc: func() error {
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
	if err := receiveCommitResult(t, leader, time.After(2*time.Second)); err != nil {
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
	completionDeadline := time.NewTimer(2 * time.Second)
	defer completionDeadline.Stop()
	for i := 0; i < 8; i++ {
		if err := receiveCommitResult(t, followers, completionDeadline.C); !errors.Is(err, failed) {
			t.Fatal("physical error not propagated", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("physical syncs=%d", calls.Load())
	}
}

func TestGroupCommitQD1EveryAckFollowsPhysicalSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var syncs atomic.Int32
	gc := NewGroupCommitter(GroupCommitterConfig{SyncFunc: func() error {
		if err := f.Sync(); err != nil {
			return err
		}
		syncs.Add(1)
		return nil
	}})
	go gc.Run()
	defer gc.Stop()
	want := make([]byte, 32*4096)
	for i := 0; i < 32; i++ {
		block := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		copy(want[i*4096:], block)
		if _, err := f.WriteAt(block, int64(i*4096)); err != nil {
			t.Fatal(err)
		}
		if err := gc.SyncCache(); err != nil {
			t.Fatal(err)
		}
		if n := syncs.Load(); n != int32(i+1) {
			t.Fatalf("ack without physical fsync: request=%d syncs=%d", i+1, n)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("fsynced bytes differ on reopen", err)
	}
}

func receiveCommitResult(t *testing.T, result <-chan error, deadline <-chan time.Time) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-deadline:
		t.Fatal("physical sync started but completion did not settle within deadline")
		return nil
	}
}
