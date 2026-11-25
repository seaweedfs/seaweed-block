package chunkstore

import "github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"

// SnapshotInfoFromHead is a snapshot request's answer from HEAD's record
// of the name: its root, dependents, retention facts, and the HEAD that
// holds it.
func SnapshotInfoFromHead(volumeID, name string, h layout.Head) SnapshotInfo {
	s := h.Snapshots[name]
	return SnapshotInfo{
		VolumeID: volumeID, Name: name, Root: s.Root.String(), Dependents: s.Dependents,
		HeadGen: h.Gen, HeadEpoch: h.Epoch, CutLSN: h.Position, State: SnapActive,
		Class: s.Meta.Class, CreatedNs: s.Meta.CreatedNs, ExpiresAtNs: s.Meta.ExpiresNs, Labels: s.Meta.Labels,
	}
}
