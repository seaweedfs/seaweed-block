package chunkstore

// Control response types and error codes are shared by the layout catalog,
// the status HTTP routes and the operator CLI.
import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	// SnapActive and SnapDeleting are a snapshot's states.
	SnapActive   = "active"
	SnapDeleting = "deleting"
	// SnapAbsent is DeleteSnapshot's answer for a snapshot that is not there.
	SnapAbsent             = "absent"
	SnapshotClassSemantic  = "semantic"
	SnapshotClassPinned    = "pinned"
	SnapshotClassScheduled = "scheduled"

	// LabelScheduleKeep, on a scheduled snapshot, is how many scheduled
	// snapshots its schedule keeps (a decimal count).
	LabelScheduleKeep = "swb.schedule.keep"
	// DefaultScheduleKeep is the count kept when the newest scheduled
	// snapshot carries no valid LabelScheduleKeep.
	DefaultScheduleKeep = 7

	CodeInvalidClass  = "invalid_class"
	CodeInvalidPolicy = "invalid_policy"
)

// Error codes of the control plane (spec §2.2), as the HTTP API and the CLI
// report them.
const (
	CodeNotChunkVolume    = "not_chunk_volume"
	CodeInvalidName       = "invalid_name"
	CodeSnapshotExists    = "snapshot_exists"
	CodeSnapshotNotFound  = "snapshot_not_found"
	CodeSnapshotDeleting  = "snapshot_deleting"
	CodeTargetExists      = "target_exists"
	CodeGeometryMismatch  = "geometry_mismatch"
	CodeForkIncomplete    = "fork_incomplete"
	CodeDeadlineExceeded  = "deadline_exceeded"
	CodeRemoteUnavailable = "remote_unavailable"
	CodeOutcomeUnknown    = "outcome_unknown"
	CodeInvalidLabel      = "invalid_label"
)

// CatalogError is a typed control-plane failure.
type CatalogError struct {
	Code      string
	Message   string
	Retryable bool
	Err       error
}

func (e *CatalogError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("chunkstore: %s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("chunkstore: %s: %s", e.Code, e.Message)
}

func (e *CatalogError) Unwrap() error { return e.Err }

// Detail is the error without its code: the message, and the cause if any
// (an error body carries the code in its own field).
func (e *CatalogError) Detail() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// CodeOf returns the control-plane code of err, or "" when it has none.
func CodeOf(err error) string {
	var ce *CatalogError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return ""
}

func catErr(code string, retryable bool, err error, format string, a ...any) error {
	return &CatalogError{Code: code, Message: fmt.Sprintf(format, a...), Retryable: retryable, Err: err}
}

// remoteErr wraps a filer failure as remote_unavailable, keeping the cause.
func remoteErr(err error, format string, a ...any) error {
	return catErr(CodeRemoteUnavailable, true, err, format, a...)
}

// Geometry is a volume's shape; a chunk index means nothing without it.
type Geometry struct {
	NumBlocks   uint64 `json:"num_blocks"`
	BlockSize   uint32 `json:"block_size"`
	ChunkBlocks uint32 `json:"chunk_blocks"`
}

// Parent is a forked volume's source.
type Parent struct {
	VolumeID string `json:"volume_id"`
	Snapshot string `json:"snapshot"`
	Gen      uint64 `json:"gen"`
}

// SnapshotInfo describes one snapshot, with the field names of the HTTP API
// (spec §3.2-§3.3).
type SnapshotInfo struct {
	Root          string             `json:"root,omitempty"`
	Dependents    []layout.Dependent `json:"dependents,omitempty"`
	VolumeID      string             `json:"volumeID"`
	Name          string             `json:"name"`
	HeadGen       uint64             `json:"headGen"`
	HeadEpoch     uint64             `json:"headEpoch"`
	CutLSN        uint64             `json:"cutLSN"`
	CreatedNs     int64              `json:"createdNs"`
	State         string             `json:"state"`
	ChunkCount    int                `json:"chunkCount"`
	ManifestBytes int                `json:"manifestBytes"`
	Consistency   string             `json:"consistency"`
	Labels        map[string]string  `json:"labels"`
	Parent        *Parent            `json:"parent"`
	ExpiresAtNs   int64              `json:"expiresAtNs"`
	Replayed      bool               `json:"replayed"`
	Class         string             `json:"class"`

	attempt string // the attempt the name resolved to, when read by name

	head   storage.ChunkHead
	geo    Geometry
	digest string // identity uses the embedded writer version, not today's serializer
}

// Head returns the HEAD the snapshot copied.
func (s SnapshotInfo) Head() storage.ChunkHead { return s.head }

type ListOptions struct {
	Prefix          string
	StartAfter      string
	Limit           int
	IncludeDeleting bool
}

// ValidateSnapshotClass refuses a class other than "", "semantic",
// "pinned" and "scheduled" (invalid_class).
func ValidateSnapshotClass(class string) error {
	switch class {
	case "", SnapshotClassSemantic, SnapshotClassPinned, SnapshotClassScheduled:
		return nil
	}
	return catErr(CodeInvalidClass, false, nil, "snapshot class %q: want %q, %q or %q", class, SnapshotClassSemantic, SnapshotClassPinned, SnapshotClassScheduled)
}
