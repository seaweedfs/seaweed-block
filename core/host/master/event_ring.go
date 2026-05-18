package master

import (
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
}

func newEventRing(max int) *eventRing {
	if max <= 0 {
		max = 512
	}
	return &eventRing{max: max}
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
