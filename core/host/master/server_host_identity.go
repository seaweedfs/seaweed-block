package master

import (
	"fmt"
	"net"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// addrHost is the host part of a host:port (or bare host) address.
func addrHost(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return strings.TrimSpace(addr)
}

// refuseServerIDFromOtherHost implements the host half of INV-ASSIGNMENT-HOLDER-IDENTITY-001. OSS has no host
// authentication, so a server id names a host only if every address a heartbeat reports under that id is on the host
// registered for it. A second process started with the same --server-id elsewhere (a copied config) is refused before
// its slots reach the observations. Only the host part is compared (each volume process listens on its own ports);
// several server ids on one host stay allowed; an id with no registry record has nothing to contradict.
func (h *Host) refuseServerIDFromOtherHost(r *control.HeartbeatReport) error {
	if h.lifecycle == nil || h.lifecycle.Nodes == nil {
		return nil
	}
	reg, ok := h.lifecycle.Nodes.GetNode(r.ServerId)
	if !ok {
		return nil
	}
	hosts := map[string]bool{}
	for _, a := range []string{reg.DataAddr, reg.CtrlAddr, reg.Addr} {
		if a != "" {
			hosts[addrHost(a)] = true
		}
	}
	if len(hosts) == 0 {
		return nil
	}
	registered := addrHost(reg.DataAddr)
	if registered == "" {
		registered = addrHost(reg.Addr)
	}
	for _, s := range r.Slots {
		for _, a := range []string{s.DataAddr, s.CtrlAddr} {
			if a != "" && !hosts[addrHost(a)] {
				return fmt.Errorf("heartbeat: server id %s is registered on host %s; refused from %s (volume %s replica %s): one server id names one host",
					r.ServerId, registered, addrHost(a), s.VolumeId, s.ReplicaId)
			}
		}
	}
	return nil
}

// holderSlotFact is the slot fact of replica on the server the authority line names (workspaceSourceHost); a
// same-replica claim from any other server, however fresh, is never used (INV-ASSIGNMENT-HOLDER-IDENTITY-001).
// No resolvable holder: no fact (fail closed).
func (h *Host) holderSlotFact(volumeID, replicaID string, epoch uint64) (string, authority.SlotFact, bool) {
	holder := h.workspaceSourceHost(volumeID, replicaID, epoch)
	if holder == "" || h.obs == nil {
		return holder, authority.SlotFact{}, false
	}
	fact, ok := h.obs.Store().SlotFactOnServer(holder, volumeID, replicaID)
	return holder, fact, ok
}
