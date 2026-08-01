// Package snapshot owns immutable snapshot archives and their durable catalog.
// It does not own CSI requests, authority selection, or Kubernetes objects.
package snapshot

import (
	"errors"
	"time"
)

var (
	ErrNotFound         = errors.New("snapshot: not found")
	ErrNameConflict     = errors.New("snapshot: name already belongs to another source")
	ErrInUse            = errors.New("snapshot: in use")
	ErrArchiveCorrupt   = errors.New("snapshot: archive corrupt")
	ErrSourceNotReady   = errors.New("snapshot: source not ready")
	ErrAuthorityChanged = errors.New("snapshot: source authority changed")
)

const StateReady = "ready"

// Record is the durable catalog entry for one immutable snapshot.
type Record struct {
	SnapshotID     string    `json:"snapshot_id"`
	Name           string    `json:"name"`
	SourceVolumeID string    `json:"source_volume_id"`
	CreatedAt      time.Time `json:"created_at"`
	State          string    `json:"state"`
	Frontier       uint64    `json:"frontier"`
	SizeBytes      uint64    `json:"size_bytes"`
	NumBlocks      uint32    `json:"num_blocks"`
	BlockSize      int       `json:"block_size"`
	RecordCount    uint64    `json:"record_count"`
	DataBytes      uint64    `json:"data_bytes"`
	ArchiveBytes   int64     `json:"archive_bytes"`
	ArchiveSHA256  string    `json:"archive_sha256"`
}

type CreateRequest struct {
	Name           string
	SourceVolumeID string
}
