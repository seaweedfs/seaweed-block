package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type delayedLayoutDetach struct {
	layout.HeadStore
	held           chan struct{}
	release        chan struct{}
	loseAbortReply bool
}

func (s *delayedLayoutDetach) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	if next.State == layout.StateDetached {
		close(s.held)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := s.HeadStore.UpdateHead(ctx, gen, next)
	if err == nil && s.loseAbortReply && next.State == layout.StateLive && next.Activity.Kind == layout.ActIdle {
		return errors.New("fixture: abort reply lost")
	}
	return err
}

func TestLayoutStopAbortRefusesALateDetachCommit(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint("abort reply lost=", lost), func(t *testing.T) { layoutStopLateCommit(t, lost) })
	}
}
func layoutStopLateCommit(t *testing.T, lost bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewBCatalog(f, &layoutCrashBlobs{b: map[string][]byte{}}, "/stop-race")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: "v", ID: id}
	if _, err = cat.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	st, err := cat.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	if _, err = layout.Start(ctx, st, grantedLayoutLine(me), me, layout.LocalRoot{ClaimID: "k", Fresh: true}); err != nil {
		t.Fatal(err)
	}
	const session = "delayed-stop"
	if _, err = layout.RunBegin(ctx, st, me, session); err != nil {
		t.Fatal(err)
	}
	root, _, err := layout.Build(nil)
	if err != nil {
		t.Fatal(err)
	}
	name, raw, err := layout.NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = layout.EnsureObject(ctx, st, name.Path(), raw, session); err != nil {
		t.Fatal(err)
	}
	delay := &delayedLayoutDetach{HeadStore: st, held: make(chan struct{}), release: make(chan struct{}), loseAbortReply: lost}
	var once sync.Once
	release := func() { once.Do(func() { close(delay.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := layout.RunCommit(ctx, delay, st, "v", me, layout.CommitRequest{Session: session, Position: 7, State: layout.StateDetached})
		done <- err
	}()
	select {
	case <-delay.held:
	case <-ctx.Done():
		t.Fatal("commit did not reach conditional write")
	}
	how, _, err := settleLayoutStop(ctx, delay, me, session)
	if err != nil || how != storage.DetachNotPublished {
		t.Fatalf("settlement: %v %v", how, err)
	}
	release()
	if err = <-done; !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("late detach commit accepted after resume: %v", err)
	}
	h, found, err := st.ReadHead(ctx)
	if err != nil || !found || h.State != layout.StateLive || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("after late commit: %+v %v", h, err)
	}
}

type stopAbortFaultHead struct {
	layoutStopHead
	mode   string
	writes int
}

func (s *stopAbortFaultHead) UpdateHead(_ context.Context, gen uint64, next layout.Head) error {
	s.writes++
	switch s.mode {
	case "commit wins":
		s.h.State = layout.StateDetached
		s.h.Activity = layout.Activity{Kind: layout.ActIdle}
		s.h.Gen++
		return layout.ErrPrecondition
	case "lost abort reply":
		s.h = next
		return errors.New("abort reply unknown")
	default:
		return errors.New("abort not applied")
	}
}
func TestLayoutStopAbortMustConfirmItsOutcome(t *testing.T) {
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	for _, tc := range []struct {
		mode, session string
		want          storage.DetachSettlement
		bad           bool
		writes        int
	}{
		{"commit wins", "mine", storage.DetachApplied, false, 1},
		{"lost abort reply", "mine", storage.DetachNotPublished, false, 1},
		{"abort not applied", "mine", 0, true, 1},
		{"another session", "other", 0, true, 0},
		{"missing session", "", 0, true, 0},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			st := &stopAbortFaultHead{layoutStopHead: layoutStopHead{h: layout.Head{VolumeID: id, Gen: 7, Epoch: 1, Holder: "s/r", ClaimID: "k", State: layout.StateLive, Activity: layout.Activity{Kind: layout.ActSnap, Session: "mine"}}, found: true}, mode: tc.mode}
			got, _, err := settleLayoutStop(context.Background(), st, me, tc.session)
			if (err != nil) != tc.bad || (!tc.bad && got != tc.want) || st.writes != tc.writes {
				t.Fatalf("got=%v err=%v writes=%d", got, err, st.writes)
			}
		})
	}
}
