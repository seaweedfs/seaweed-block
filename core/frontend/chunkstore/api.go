package chunkstore

// The JSON bodies of a snapshot listing and a snapshot delete (B9 W2 §3.3,
// §3.4), shared by blockvolume's /chunk/* routes and the sw-block CLI, which
// reads the filer directly; and the body of `sw-block ops chunk show`.

import (
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// SnapshotListItem is one listed snapshot, exactly §3.3's fields.
type SnapshotListItem struct {
	Class       string             `json:"class,omitempty"`
	ExpiresAtNs int64              `json:"expiresAtNs,omitempty"`
	Root        string             `json:"root,omitempty"`
	Dependents  []layout.Dependent `json:"dependents,omitempty"`
	Name        string             `json:"name"`
	HeadGen     uint64             `json:"headGen"`
	HeadEpoch   uint64             `json:"headEpoch"`
	CutLSN      uint64             `json:"cutLSN"`
	CreatedNs   int64              `json:"createdNs"`
	State       string             `json:"state"`
	ChunkCount  int                `json:"chunkCount"`
	Labels      map[string]string  `json:"labels"`
	Consistency string             `json:"consistency"`
	Parent      *Parent            `json:"parent"`
}

// SnapshotListBody is a listing page: the snapshots after the request's
// start, and next (the last name returned when more may follow, else "").
type SnapshotListBody struct {
	VolumeID  string             `json:"volumeID"`
	Snapshots []SnapshotListItem `json:"snapshots"`
	Next      string             `json:"next"`
}

// NewSnapshotListBody is the listing body of vol's page list.
func NewSnapshotListBody(vol string, list []SnapshotInfo, next string) SnapshotListBody {
	body := SnapshotListBody{VolumeID: vol, Snapshots: make([]SnapshotListItem, 0, len(list)), Next: next}
	for _, s := range list {
		body.Snapshots = append(body.Snapshots, SnapshotListItem{Name: s.Name, Root: s.Root, Dependents: s.Dependents, Class: s.Class, ExpiresAtNs: s.ExpiresAtNs, HeadGen: s.HeadGen, HeadEpoch: s.HeadEpoch,
			CutLSN: s.CutLSN, CreatedNs: s.CreatedNs, State: s.State, ChunkCount: s.ChunkCount, Labels: s.Labels,
			Consistency: s.Consistency, Parent: s.Parent})
	}
	return body
}

// SnapshotDeleteBody is a delete's result: state is SnapDeleting or SnapAbsent.
type SnapshotDeleteBody struct {
	VolumeID string `json:"volumeID"`
	Name     string `json:"name"`
	State    string `json:"state"`
}

// VolumeShowBody is one volume's VOLUME entry and HEAD (sw-block ops chunk
// show); each is null when it does not exist.
type VolumeShowBody struct {
	VolumeID string      `json:"volumeID"`
	Volume   *VolumeShow `json:"volume"`
	Head     *HeadShow   `json:"head"`
}

// VolumeShow is the VOLUME entry (§4.2).
type VolumeShow struct {
	Origin      string  `json:"origin"`
	NumBlocks   uint64  `json:"numBlocks"`
	BlockSize   uint32  `json:"blockSize"`
	ChunkBlocks uint32  `json:"chunkBlocks"`
	CreatedNs   int64   `json:"createdNs"`
	Parent      *Parent `json:"parent"`
}

// HeadShow is the HEAD; a HEAD without a state is active.
type HeadShow struct {
	Gen        uint64          `json:"gen"`
	Epoch      uint64          `json:"epoch"`
	Holder     string          `json:"holder"`
	RemoteLSN  uint64          `json:"remoteLSN"`
	State      string          `json:"state"`
	DetachedBy *DetachedByShow `json:"detachedBy"`
	ChunkCount int             `json:"chunkCount"`
}

// DetachedByShow is the detach proof in a detached HEAD.
type DetachedByShow struct {
	Holder string `json:"holder"`
	Epoch  uint64 `json:"epoch"`
	Gen    uint64 `json:"gen"`
}

// NewVolumeShowBody is vol's show body from what exists.
func NewVolumeShowBody(vol string, v *VolumeInfo, h *storage.ChunkHead) VolumeShowBody {
	body := VolumeShowBody{VolumeID: vol}
	if v != nil {
		body.Volume = &VolumeShow{Origin: v.Origin, NumBlocks: v.NumBlocks, BlockSize: v.BlockSize, ChunkBlocks: v.ChunkBlocks,
			CreatedNs: v.CreatedNs, Parent: v.Parent}
	}
	if h != nil {
		state := h.State
		if state == "" {
			state = storage.ChunkHeadActive
		}
		body.Head = &HeadShow{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, State: state, ChunkCount: manifestCount(*h)}
		if d := h.DetachedBy; d != nil {
			body.Head.DetachedBy = &DetachedByShow{Holder: d.Holder, Epoch: d.Epoch, Gen: d.Gen}
		}
	}
	return body
}
