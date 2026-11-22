package master

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/peer"
)

// addrHost is the host part of a host:port (or bare host) address.
func addrHost(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return strings.TrimSpace(addr)
}

// registryHosts is the host set the node registry records for serverID (data, ctrl and plain addresses) and the host
// named in refusals; ok is false when serverID has no registry record.
func (h *Host) registryHosts(serverID string) (hosts map[string]bool, named string, ok bool) {
	if h.lifecycle == nil || h.lifecycle.Nodes == nil || serverID == "" {
		return nil, "", false
	}
	reg, ok := h.lifecycle.Nodes.GetNode(serverID)
	if !ok {
		return nil, "", false
	}
	hosts = map[string]bool{}
	for _, a := range []string{reg.DataAddr, reg.CtrlAddr, reg.Addr} {
		if a != "" {
			hosts[addrHost(a)] = true
		}
	}
	named = addrHost(reg.DataAddr)
	if named == "" {
		named = addrHost(reg.Addr)
	}
	return hosts, named, true
}

// refuseServerIDFromOtherHost is the observation half of INV-AUTH-HOLDER-IDENTITY-001: a heartbeat whose slot addresses
// are on a host other than the one registered for its server id is refused before its slots reach the observations.
// Only the host part is compared (each volume process listens on its own ports); several server ids on one host stay
// allowed; an id with no registry record is not checked here (no first-seen binding in v1.0: host addresses change).
func (h *Host) refuseServerIDFromOtherHost(r *control.HeartbeatReport) error {
	hosts, named, ok := h.registryHosts(r.ServerId)
	if !ok || len(hosts) == 0 {
		return nil
	}
	for _, s := range r.Slots {
		for _, a := range []string{s.DataAddr, s.CtrlAddr} {
			if a != "" && !hosts[addrHost(a)] {
				return fmt.Errorf("heartbeat: server id %s is registered on host %s; refused from %s (volume %s replica %s): one server id names one host",
					r.ServerId, named, addrHost(a), s.VolumeId, s.ReplicaId)
			}
		}
	}
	return nil
}

// peerHost is the host part of a gRPC call's network peer; "" when the call carries none.
func peerHost(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	return addrHost(p.Addr.String())
}

// refuseWorkspaceHolderHost is the holder half of INV-AUTH-HOLDER-IDENTITY-001 for workspace volumes (lease renew, ACK,
// assignment stream): the server id must be registered (no first-seen binding), and the call's network peer must be on
// the host the registry records for it. Limits: it trusts the network peer address (no NAT or proxy between volume and
// master in OSS), and it is not authentication. Every refusal is logged.
func (h *Host) refuseWorkspaceHolderHost(ctx context.Context, rpc, server string) error {
	hosts, named, ok := h.registryHosts(server)
	var err error
	switch ph := peerHost(ctx); {
	case !ok:
		err = fmt.Errorf("%s: server id %q is not registered; a workspace holder's server id must be", rpc, server)
	case ph == "" || !hosts[ph]:
		if ph == "" {
			ph = "an unknown peer"
		}
		err = fmt.Errorf("%s: server id %s is registered on host %s; refused from %s: one server id names one host", rpc, server, named, ph)
	}
	if err != nil {
		h.log.Print(err)
	}
	return err
}

// holderSlotFact is the slot fact of replica on the server the authority line names (workspaceSourceHost); a
// same-replica claim from any other server, however fresh, is never used (INV-AUTH-HOLDER-IDENTITY-001).
// No resolvable holder: no fact (fail closed).
func (h *Host) holderSlotFact(volumeID, replicaID string, epoch uint64) (string, authority.SlotFact, bool) {
	holder := h.workspaceSourceHost(volumeID, replicaID, epoch)
	if holder == "" || h.obs == nil {
		return holder, authority.SlotFact{}, false
	}
	fact, ok := h.obs.Store().SlotFactOnServer(holder, volumeID, replicaID)
	return holder, fact, ok
}
