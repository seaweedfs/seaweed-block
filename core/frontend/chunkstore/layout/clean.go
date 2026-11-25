package layout

import (
	"context"
	"errors"
	"fmt"
)

// BeginClean is transition 6: activity idle -> clean(session), by the
// holder at HEAD's epoch. While it is open no snapshot session can begin,
// so no name is removed while a snapshot is open. BeginClean of the
// cleaning already open is a replay; any other open activity is ErrBusy.
func (h Head) BeginClean(me HolderID, session string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: BeginClean needs a session id")
	}
	if err := h.holderErr(me); err != nil {
		return Head{}, false, err
	}
	switch {
	case h.Activity == Activity{Kind: ActClean, Session: session}:
		return h, false, nil
	case h.Activity.Kind != ActIdle:
		return Head{}, false, fmt.Errorf("%w: %s(%s)", ErrBusy, h.Activity.Kind, h.Activity.Session)
	}
	next = h.clone()
	next.Activity = Activity{Kind: ActClean, Session: session}
	return next, true, nil
}

// EndClean is transition 8: activity clean(session) -> idle, by the holder
// at HEAD's epoch. EndClean over idle succeeds and writes nothing: it means
// only that no activity remains (the cleaning ended, or never began).
// Another open session or cleaning is ErrNoSession.
func (h Head) EndClean(me HolderID, session string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: EndClean needs a session id")
	}
	if err := h.holderErr(me); err != nil {
		return Head{}, false, err
	}
	switch {
	case h.Activity.Kind == ActIdle:
		return h, false, nil
	case h.Activity != Activity{Kind: ActClean, Session: session}:
		return Head{}, false, fmt.Errorf("%w: HEAD has %s(%s), not clean(%s)", ErrNoSession, h.Activity.Kind, h.Activity.Session, session)
	}
	next = h.clone()
	next.Activity = Activity{Kind: ActIdle}
	return next, true, nil
}

// RunBeginClean applies BeginClean by one conditional update.
func RunBeginClean(ctx context.Context, st HeadStore, me HolderID, session string) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.BeginClean(me, session) })
}

// RunEndClean applies EndClean by one conditional update.
func RunEndClean(ctx context.Context, st HeadStore, me HolderID, session string) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.EndClean(me, session) })
}
