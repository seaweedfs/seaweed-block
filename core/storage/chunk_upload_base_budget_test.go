package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestChunkUploadOutageSettlesBeforeQueuedBASE(t *testing.T) {
	s, f, l := mappedBASELayerV3(t)
	if _, err := s.Write(5, blk(0x31)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	entered, release := holdPuts(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	cutDone := make(chan error, 1)
	cutJoined := make(chan struct{})
	var baseJoined chan struct{}
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-cutJoined:
		case <-time.After(5 * time.Second):
			t.Error("cut not joined")
		}
		if baseJoined != nil {
			select {
			case <-baseJoined:
			case <-time.After(5 * time.Second):
				t.Error("BASE worker not joined")
			}
		}
	})
	start := time.Now()
	go func() { defer close(cutJoined); _, err := l.Cut(ctx); cutDone <- err }()
	waitKey(t, entered, "cooperative cut upload")
	attempted := make(chan struct{})
	l.testBaseTransitionBeforeLock = func() { close(attempted) }
	baseJoined = make(chan struct{})
	type result struct {
		h   InstallHandle
		err error
	}
	baseDone := make(chan result, 1)
	go func() { defer close(baseJoined); h, err := s.BeginBaseInstall(7, idA); baseDone <- result{h, err} }()
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("BASE did not reach cut drain")
	}
	select {
	case err := <-cutDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cut deadline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cooperative cut not settled")
	}
	var got result
	select {
	case got = <-baseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("BASE did not proceed after cut settlement")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if time.Since(start) >= 5*time.Second {
		t.Fatal("single queued BASE exceeded sub-5s target")
	}
	if len(f.installedHeads()) != 0 {
		t.Fatal("failed upload published HEAD")
	}
	select {
	case <-baseJoined:
	case <-time.After(5 * time.Second):
		t.Fatal("BASE begin worker not joined")
	}
	l.testBaseTransitionBeforeLock = nil
	if err := got.h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
}
