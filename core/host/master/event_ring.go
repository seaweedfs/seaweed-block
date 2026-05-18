package master

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
)

type eventRing struct {
	mu     sync.Mutex
	max    int
	nextID uint64
	events []ops.ClusterEvent
	notify chan struct{}
}

func newEventRing(max int) *eventRing {
	if max <= 0 {
		max = 512
	}
	return &eventRing{max: max, notify: make(chan struct{})}
}

func (r *eventRing) append(event ops.ClusterEvent) ops.ClusterEvent {
	if r == nil {
		return event
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	if event.EventID == "" {
		event.EventID = fmt.Sprintf("master-%d", r.nextID)
	}
	if event.EventTime.IsZero() {
		event.EventTime = time.Now().UTC()
	} else {
		event.EventTime = event.EventTime.UTC()
	}
	r.events = append(r.events, event)
	if len(r.events) > r.max {
		r.events = append([]ops.ClusterEvent(nil), r.events[len(r.events)-r.max:]...)
	}
	close(r.notify)
	r.notify = make(chan struct{})
	return event
}

func (r *eventRing) list(volumeID string) []ops.ClusterEvent {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ops.ClusterEvent, 0, len(r.events))
	for _, event := range r.events {
		if volumeID == "" || event.VolumeID == "" || event.VolumeID == volumeID {
			out = append(out, event)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EventTime.Equal(out[j].EventTime) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].EventTime.Before(out[j].EventTime)
	})
	return out
}

func (r *eventRing) listAfter(volumeID, sinceEventID string) []ops.ClusterEvent {
	events := r.list(volumeID)
	if sinceEventID == "" {
		return events
	}
	for i, event := range events {
		if event.EventID == sinceEventID {
			return append([]ops.ClusterEvent(nil), events[i+1:]...)
		}
	}
	// Unknown cursor: return all retained events rather than silently dropping
	// evidence. Clients may de-duplicate by event_id.
	return events
}

func (r *eventRing) waitAfter(ctx context.Context, volumeID, sinceEventID string) ([]ops.ClusterEvent, error) {
	if r == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for {
		events, notify := r.listAfterWithNotify(volumeID, sinceEventID)
		if len(events) > 0 {
			return events, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (r *eventRing) listAfterWithNotify(volumeID, sinceEventID string) ([]ops.ClusterEvent, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ops.ClusterEvent, 0, len(r.events))
	seenCursor := sinceEventID == ""
	for _, event := range r.events {
		if volumeID != "" && event.VolumeID != "" && event.VolumeID != volumeID {
			continue
		}
		if !seenCursor {
			if event.EventID == sinceEventID {
				seenCursor = true
			}
			continue
		}
		out = append(out, event)
	}
	if sinceEventID != "" && !seenCursor {
		out = out[:0]
		for _, event := range r.events {
			if volumeID == "" || event.VolumeID == "" || event.VolumeID == volumeID {
				out = append(out, event)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EventTime.Equal(out[j].EventTime) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].EventTime.Before(out[j].EventTime)
	})
	return out, r.notify
}
