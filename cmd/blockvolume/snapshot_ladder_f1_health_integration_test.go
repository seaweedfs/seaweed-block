//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// Rung F1's heartbeat-ordering watch: with N processes under
// one server id, a volume's slot at the master can expire when its
// heartbeats are dropped. The watch polls the master's view of every source
// volume's replica each second and records each span where the master does
// not observe it (a slot expiry), and what the volume shows meanwhile: an
// epoch change or its frontend leaving ready. At the end it counts the
// master's per-slot superseded lines for the volume ('blockmaster:
// heartbeat superseded ... volume=<v> ...'; 0 where the master does not log
// them).

// f1Health is one source volume's record over the whole run.
type f1Health struct {
	Volume             string   `json:"volume"`
	Replica            string   `json:"replica"`
	Polls              int      `json:"polls"`
	PollErrors         int      `json:"poll_errors"`
	SlotExpiries       int      `json:"slot_expiries"`
	SlotUnobservedMs   float64  `json:"slot_unobserved_ms"`
	EpochChanges       int      `json:"epoch_changes"`
	FrontendNotReady   int      `json:"frontend_not_ready_polls"`
	DuringExpiry       []string `json:"during_expiry,omitempty"`
	SupersededLogLines int      `json:"master_superseded_lines"`
}

type f1Watch struct {
	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	vols map[string]*f1Health
}

// f1StartWatch starts polling the master for every source volume.
func f1StartWatch(env *handoffEnv, vols []f1Volume) *f1Watch {
	w := &f1Watch{stop: make(chan struct{}), done: make(chan struct{}), vols: map[string]*f1Health{}}
	for _, v := range vols {
		w.vols[v.id] = &f1Health{Volume: v.id, Replica: env.hosts[v.node].replica}
	}
	go func() {
		defer close(w.done)
		conn, err := link.Dial(env.h.masterAddr)
		if err != nil {
			return
		}
		defer conn.Close()
		cl := control.NewClusterEvidenceServiceClient(conn)
		type state struct {
			unobservedSince time.Time
			epoch           uint64
		}
		st := map[string]*state{}
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
			}
			for _, v := range vols {
				h, s := w.vols[v.id], st[v.id]
				if s == nil {
					s = &state{}
					st[v.id] = s
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				ev, err := cl.GetVolumeStatus(ctx, &control.GetVolumeStatusRequest{VolumeId: v.id})
				cancel()
				w.mu.Lock()
				h.Polls++
				if err != nil {
					h.PollErrors++
					w.mu.Unlock()
					continue
				}
				observed := false
				for _, r := range ev.GetReplicas() {
					if r.GetReplicaId() == h.Replica && r.GetObserved() {
						observed = true
					}
				}
				now := time.Now()
				switch {
				case !observed && s.unobservedSince.IsZero():
					s.unobservedSince = now
					h.SlotExpiries++
				case observed && !s.unobservedSince.IsZero():
					h.SlotUnobservedMs += ms(now.Sub(s.unobservedSince))
					s.unobservedSince = time.Time{}
				}
				if e := ev.GetEpoch(); s.epoch != 0 && e != s.epoch {
					h.EpochChanges++
					if !s.unobservedSince.IsZero() {
						h.DuringExpiry = append(h.DuringExpiry, fmt.Sprintf("epoch %d -> %d", s.epoch, e))
					}
				}
				s.epoch = ev.GetEpoch()
				w.mu.Unlock()
				ready := volumeFrontendReady(env.hosts[v.node].status, v.id)
				w.mu.Lock()
				if !ready {
					h.FrontendNotReady++
					if !s.unobservedSince.IsZero() {
						h.DuringExpiry = append(h.DuringExpiry, "frontend not ready at "+now.UTC().Format(time.RFC3339))
					}
				}
				w.mu.Unlock()
			}
		}
	}()
	return w
}

// finish stops the watch and counts the master's superseded lines per
// volume from its log.
func (w *f1Watch) finish(masterLog string) []f1Health {
	close(w.stop)
	<-w.done
	b, _ := os.ReadFile(masterLog)
	lines := strings.Split(string(b), "\n")
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []f1Health
	for _, h := range w.vols {
		for _, l := range lines {
			if strings.Contains(l, "heartbeat superseded") && strings.Contains(l, " volume="+h.Volume+" ") {
				h.SupersededLogLines++
			}
		}
		out = append(out, *h)
	}
	return out
}
