package volume

import (
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/proto"
)

// applyFixtureFact supplies the explicit holder identity for pre-r2.34 tests
// whose subject is peer ordering, readiness or recovery. Identity-refusal
// regressions call applyFact directly and never use this fixture constructor.
func (h *Host) applyFixtureFact(f *control.AssignmentFact) {
	f = proto.Clone(f).(*control.AssignmentFact)
	if h.cfg.ServerID == "" {
		h.cfg.ServerID = "fixture-server"
	}
	if f.ServerId == "" {
		f.ServerId = h.cfg.ServerID
	}
	if f.ReplicaId == h.cfg.ReplicaID {
		if h.cfg.DataAddr == "" {
			h.cfg.DataAddr = f.DataAddr
		}
		if h.cfg.CtrlAddr == "" {
			h.cfg.CtrlAddr = f.CtrlAddr
		}
	}
	h.applyFact(f)
}
