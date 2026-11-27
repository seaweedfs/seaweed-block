package volume

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The replica's side of the promotion probe round: it subscribes to the
// master's rounds for its node and answers each one for this replica from
// the store as it is now (durable end, epoch-table row there, validity) and
// its role. It keeps no state and reports nothing unasked.
//
// A fencing round (fence_epoch > 0) is answered only after its promise: when
// the round names this replica's store, the listener's floor is raised to
// fence_epoch, every mutation admitted before has drained and the floor is
// durable; only then is the evidence read, so the end it reports is final
// for every epoch below. A round naming another store raises nothing; a
// fence that fails is not answered.

// probeRoleSource is this replica's role as the promotion probe reads it
// (StatusServer.ProbeRoles): ready to be promoted, or a primary that serves.
type probeRoleSource func() (replicaReady, primaryReady bool)

// SetProbeRoleSource wires the replica's role for its probe answers.
func (h *Host) SetProbeRoleSource(src func() (replicaReady, primaryReady bool)) {
	h.probeRoles.Store(probeRoleSource(src))
}

type probeRoles struct{ atomic.Value }

func (h *Host) runProbeRounds(ctx context.Context) {
	defer h.wg.Done()
	var backoff time.Duration
	for ctx.Err() == nil {
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		if err := h.probeStreamOnce(ctx); err != nil && err != io.EOF && ctx.Err() == nil {
			h.log.Printf("blockvolume: probe round stream: %v", err)
			if backoff < 5*time.Second {
				backoff = backoff*2 + 200*time.Millisecond
			}
			continue
		}
		backoff = 0
	}
}

func (h *Host) probeStreamOnce(ctx context.Context) error {
	stream, err := h.asnCli.SubscribeProbeRounds(ctx, &control.ProbeRoundSubscription{ServerId: h.cfg.ServerID})
	if err != nil {
		return err
	}
	for {
		round, err := stream.Recv()
		if err != nil {
			return err
		}
		if round.GetVolumeId() != h.cfg.VolumeID || round.GetReplicaId() != h.cfg.ReplicaID {
			continue // another replica on this node answers its own rounds
		}
		answer, ok := h.roundAnswer(round)
		if !ok {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = h.obsCli.AnswerProbe(rpcCtx, answer)
		cancel()
		if err != nil {
			h.log.Printf("blockvolume: probe answer round=%d: %v", round.GetRoundId(), err)
		}
	}
}

// floorFencer is the replication listener's fencing-round side
// (transport.ReplicaListener).
type floorFencer interface {
	FenceFloor(epoch uint64) error
	DurableFloor() uint64
}

func (h *Host) floorFencer() (floorFencer, bool) {
	h.selfFenceMu.Lock()
	defer h.selfFenceMu.Unlock()
	f, ok := h.selfFence.(floorFencer)
	return f, ok && f != nil
}

// roundAnswer is the answer to round, or false when none may be sent: a
// fencing round for this store whose fence did not complete.
func (h *Host) roundAnswer(round *control.ProbeRound) (*control.ProbeAnswer, bool) {
	fencer, hasFloor := h.floorFencer()
	if fe := round.GetFenceEpoch(); fe > 0 && hasFloor && h.roundNamesThisStore(round) {
		if err := fencer.FenceFloor(fe); err != nil {
			h.log.Printf("blockvolume: fencing round=%d at epoch %d not answered: %v", round.GetRoundId(), fe, err)
			return nil, false
		}
	} else if fe > 0 {
		h.log.Printf("blockvolume: fencing round=%d at epoch %d names another store; floor unchanged", round.GetRoundId(), fe)
	}
	a := h.probeAnswer(round.GetRoundId())
	if hasFloor {
		a.Floor = fencer.DurableFloor()
	}
	return a, true
}

// roundNamesThisStore reports whether the round's expected store is this
// replica's store (a store without an id never matches).
func (h *Host) roundNamesThisStore(round *control.ProbeRound) bool {
	sid, ok := h.getReplication().(interface{ LocalStoreID() storage.StoreID })
	if !ok || sid == nil {
		return false
	}
	id := sid.LocalStoreID()
	return !id.IsZero() && bytes.Equal(round.GetExpectedStoreId(), id[:])
}

// probeAnswer is this replica's evidence now, for round.
func (h *Host) probeAnswer(round uint64) *control.ProbeAnswer {
	a := &control.ProbeAnswer{ServerId: h.cfg.ServerID, VolumeId: h.cfg.VolumeID, ReplicaId: h.cfg.ReplicaID, RoundId: round, AckProfile: h.cfg.AckProfile}
	if src, ok := h.getReplication().(primaryEvidenceSource); ok && src != nil {
		if st, end, err := src.LocalPrimaryEvidence(); err == nil {
			a.DurableEnd = end
			a.TablePresent = st.Present
			a.TableValid = st.Present && !st.Invalid
			if a.TableValid {
				for _, r := range st.Rows {
					if r.FirstLSN <= end {
						a.LastDataEpoch, a.LastDataFirstLsn = r.Epoch, r.FirstLSN
					}
				}
			}
		}
	}
	if sid, ok := h.getReplication().(interface{ LocalStoreID() storage.StoreID }); ok && sid != nil {
		if id := sid.LocalStoreID(); !id.IsZero() {
			a.StoreId = id[:]
		}
	}
	// The wait is read BEFORE readiness: if it held, a not-ready answer read
	// after is the wait (the frontend gate reads the same value); if it no
	// longer held, a not-ready answer has another cause and says nothing.
	awaiting := h.awaitingStoresEpoch()
	if roles, ok := h.probeRoles.Load().(probeRoleSource); ok && roles != nil {
		a.ReplicaReady, a.PrimaryReady = roles()
	}
	// A replica refused a rebuild for a retained BASE holds an image the
	// primary could not bring up to date: never ready for promotion, so no
	// fencing round or planned move can make it primary (it reports
	// rebuild_retained_base until a rebuild completes).
	if h.rebuildRetainedBase() {
		a.ReplicaReady = false
	}
	// Not serving only because the creation wait of this line still holds:
	// said so, with the line's epoch, so the master does not take the
	// not-ready answer for a failure (and no other line can borrow it).
	if !a.PrimaryReady {
		a.AwaitingReplicaStoresEpoch = awaiting
	}
	return a
}
