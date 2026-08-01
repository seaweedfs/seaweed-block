package snapshot

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// SourceAuthority is the exact current-primary lineage and the separately
// published snapshot runtime endpoint. RuntimeEndpoint must never be inferred
// from a replication control or frontend address.
type SourceAuthority struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	RuntimeEndpoint string
	SizeBytes       uint64
}

func (a SourceAuthority) valid() bool {
	return a.VolumeID != "" && a.ReplicaID != "" && a.Epoch > 0 && a.EndpointVersion > 0 && a.RuntimeEndpoint != ""
}

// SourceResolver returns only a positively ready current primary. Unknown,
// stale, non-primary, or locally blocked sources return ErrSourceNotReady.
type SourceResolver interface {
	ResolveSnapshotSource(ctx context.Context, volumeID string) (SourceAuthority, error)
}

type RuntimeCaptureRequest struct {
	SnapshotName string
	Source       SourceAuthority
}

// CaptureRuntime transports one authority-guarded cut from the selected
// blockvolume into the central archive writer. Authentication belongs to the
// concrete transport; this interface does not make an unauthenticated status
// endpoint a product API.
type CaptureRuntime interface {
	CaptureSnapshot(ctx context.Context, req RuntimeCaptureRequest, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error)
}

type Coordinator struct {
	manager  *Manager
	resolver SourceResolver
	runtime  CaptureRuntime
}

func NewCoordinator(manager *Manager, resolver SourceResolver, runtime CaptureRuntime) (*Coordinator, error) {
	if manager == nil || resolver == nil || runtime == nil {
		return nil, fmt.Errorf("snapshot: coordinator requires manager, resolver, and runtime")
	}
	return &Coordinator{manager: manager, resolver: resolver, runtime: runtime}, nil
}

func (c *Coordinator) Create(ctx context.Context, req CreateRequest) (Record, error) {
	if err := validateCreateRequest(req); err != nil {
		return Record{}, err
	}
	if existing, ok := c.manager.GetByName(req.Name); ok {
		if existing.SourceVolumeID != req.SourceVolumeID {
			return Record{}, ErrNameConflict
		}
		return existing, nil
	}
	expected, err := c.resolver.ResolveSnapshotSource(ctx, req.SourceVolumeID)
	if err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrSourceNotReady, err)
	}
	if !expected.valid() || expected.VolumeID != req.SourceVolumeID {
		return Record{}, ErrSourceNotReady
	}
	source := coordinatorSource{
		name:     req.Name,
		expected: expected,
		resolver: c.resolver,
		runtime:  c.runtime,
	}
	return c.manager.Create(ctx, req, source)
}

func (c *Coordinator) Get(snapshotID string) (Record, bool) {
	return c.manager.Get(snapshotID)
}

func (c *Coordinator) List(sourceVolumeID string) []Record {
	return c.manager.List(sourceVolumeID)
}

func (c *Coordinator) Delete(snapshotID string) error {
	return c.manager.Delete(snapshotID)
}

type coordinatorSource struct {
	name     string
	expected SourceAuthority
	resolver SourceResolver
	runtime  CaptureRuntime
}

func (s coordinatorSource) CaptureSnapshot(ctx context.Context, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	before, err := s.resolver.ResolveSnapshotSource(ctx, s.expected.VolumeID)
	if err != nil || before != s.expected {
		return storage.SnapshotCut{}, ErrAuthorityChanged
	}
	cut, err := s.runtime.CaptureSnapshot(ctx, RuntimeCaptureRequest{
		SnapshotName: s.name,
		Source:       s.expected,
	}, sink)
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	after, err := s.resolver.ResolveSnapshotSource(ctx, s.expected.VolumeID)
	if err != nil || after != s.expected {
		return storage.SnapshotCut{}, ErrAuthorityChanged
	}
	return cut, nil
}

var _ storage.SnapshotSource = coordinatorSource{}
