package chunkstore

// The JSON bodies of a snapshot listing and a snapshot delete (B9 W2 §3.3,
// §3.4), shared by blockvolume's /chunk/* routes and the sw-block CLI, which
// reads the filer directly.

// SnapshotListItem is one listed snapshot, exactly §3.3's fields.
type SnapshotListItem struct {
	Name        string            `json:"name"`
	HeadGen     uint64            `json:"headGen"`
	HeadEpoch   uint64            `json:"headEpoch"`
	CutLSN      uint64            `json:"cutLSN"`
	CreatedNs   int64             `json:"createdNs"`
	State       string            `json:"state"`
	ChunkCount  int               `json:"chunkCount"`
	Labels      map[string]string `json:"labels"`
	Consistency string            `json:"consistency"`
	Parent      *Parent           `json:"parent"`
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
		body.Snapshots = append(body.Snapshots, SnapshotListItem{Name: s.Name, HeadGen: s.HeadGen, HeadEpoch: s.HeadEpoch,
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
