package volume

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// The replica's side of the promotion probe round: it subscribes to the
// master's rounds for its node and answers each one for this replica from
// the store as it is now (durable end, epoch-table row there, validity) and
// its role. It keeps no state and reports nothing unasked.

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
		answer := h.probeAnswer(round.GetRoundId())
		rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = h.obsCli.AnswerProbe(rpcCtx, answer)
		cancel()
		if err != nil {
			h.log.Printf("blockvolume: probe answer round=%d: %v", round.GetRoundId(), err)
		}
	}
}

// probeAnswer is this replica's evidence now, for round.
func (h *Host) probeAnswer(round uint64) *control.ProbeAnswer {
	a := &control.ProbeAnswer{ServerId: h.cfg.ServerID, VolumeId: h.cfg.VolumeID, ReplicaId: h.cfg.ReplicaID, RoundId: round}
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
	if roles, ok := h.probeRoles.Load().(probeRoleSource); ok && roles != nil {
		a.ReplicaReady, a.PrimaryReady = roles()
	}
	return a
}
