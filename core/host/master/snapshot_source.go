package master

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
)

// ResolveSnapshotSource returns a fresh, positively ready runtime endpoint for
// exactly the current published primary. Heartbeat facts provide reachability
// and the endpoint; they never mint or replace the publisher's authority.
func (h *Host) ResolveSnapshotSource(ctx context.Context, volumeID string) (snapshot.SourceAuthority, error) {
	if err := ctx.Err(); err != nil {
		return snapshot.SourceAuthority{}, err
	}
	if h == nil || h.Publisher() == nil || h.obs == nil {
		return snapshot.SourceAuthority{}, snapshot.ErrSourceNotReady
	}
	line, hasLine := h.Publisher().VolumeAuthorityLine(volumeID)
	if !hasLine || !line.Assigned {
		return snapshot.SourceAuthority{}, snapshot.ErrSourceNotReady
	}
	expectedServerID, hasServer := h.snapshotReplicaServerID(volumeID, line.ReplicaID)
	if !hasServer || h.lifecycle == nil || h.lifecycle.Volumes == nil {
		return snapshot.SourceAuthority{}, snapshot.ErrSourceNotReady
	}
	volume, hasVolume := h.lifecycle.Volumes.GetVolume(volumeID)
	if !hasVolume || volume.Spec.SizeBytes == 0 {
		return snapshot.SourceAuthority{}, snapshot.ErrSourceNotReady
	}
	slot, hasSlot := h.obs.Store().SlotFact(volumeID, line.ReplicaID)
	resolved, ok := snapshotSourceFromFacts(volumeID, expectedServerID, volume.Spec.SizeBytes, line, hasSlot, slot)
	if !ok {
		return snapshot.SourceAuthority{}, fmt.Errorf("%w: current primary has no matching fresh snapshot runtime", snapshot.ErrSourceNotReady)
	}
	return resolved, nil
}

func snapshotSourceFromFacts(volumeID, expectedServerID string, sizeBytes uint64, line authority.AuthorityBasis, hasSlot bool, slot authority.SlotFact) (snapshot.SourceAuthority, bool) {
	if volumeID == "" || expectedServerID == "" || sizeBytes == 0 || !line.Assigned || line.ReplicaID == "" || line.Epoch == 0 || line.EndpointVersion == 0 || !hasSlot {
		return snapshot.SourceAuthority{}, false
	}
	if slot.VolumeID != volumeID || slot.ReplicaID != line.ReplicaID ||
		slot.ReportingServerID != expectedServerID ||
		slot.DataAddr != line.DataAddr || slot.CtrlAddr != line.CtrlAddr ||
		!slot.Reachable || !slot.ReadyForPrimary || !slot.Eligible || slot.Withdrawn ||
		slot.SnapshotRuntimeEndpoint == "" {
		return snapshot.SourceAuthority{}, false
	}
	if err := snapshot.ValidateRuntimeEndpoint(slot.SnapshotRuntimeEndpoint); err != nil {
		return snapshot.SourceAuthority{}, false
	}
	if !snapshotEndpointMatchesDataHost(slot.SnapshotRuntimeEndpoint, line.DataAddr) {
		return snapshot.SourceAuthority{}, false
	}
	return snapshot.SourceAuthority{
		VolumeID:        volumeID,
		ReplicaID:       line.ReplicaID,
		Epoch:           line.Epoch,
		EndpointVersion: line.EndpointVersion,
		RuntimeEndpoint: slot.SnapshotRuntimeEndpoint,
		SizeBytes:       sizeBytes,
	}, true
}

func (h *Host) snapshotReplicaServerID(volumeID, replicaID string) (string, bool) {
	for _, volume := range h.topo.Volumes {
		if volume.VolumeID != volumeID {
			continue
		}
		for _, slot := range volume.Slots {
			if slot.ReplicaID == replicaID && slot.ServerID != "" {
				return slot.ServerID, true
			}
		}
	}
	if h.lifecycle != nil && h.lifecycle.Placements != nil {
		if placement, ok := h.lifecycle.Placements.GetPlacement(volumeID); ok {
			for _, slot := range placement.Slots {
				if slot.ReplicaID == replicaID && slot.ServerID != "" {
					return slot.ServerID, true
				}
			}
		}
	}
	return "", false
}

func snapshotEndpointMatchesDataHost(endpoint, dataAddr string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	dataHost, _, err := net.SplitHostPort(dataAddr)
	if err != nil || dataHost == "" {
		return false
	}
	endpointHost := u.Hostname()
	endpointIP, dataIP := net.ParseIP(endpointHost), net.ParseIP(dataHost)
	if endpointIP != nil || dataIP != nil {
		return endpointIP != nil && dataIP != nil && endpointIP.Equal(dataIP)
	}
	return strings.EqualFold(endpointHost, dataHost)
}

var _ snapshot.SourceResolver = (*Host)(nil)
