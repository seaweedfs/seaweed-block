package ops

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// VolumeStatusReportCollector is the read-only seam between product status
// sources and the stable operations report schema. Source functions should only
// read already-existing facts; authority, lifecycle, storage, frontend, or
// Kubernetes mutation belongs outside this package.
type VolumeStatusReportCollector struct {
	Now             func() time.Time
	Source          ReportSource
	ProductRevision string
	RunnerRevision  string

	MasterStatus func(context.Context) (*control.StatusResponse, error)
	LocalStatus  func(context.Context) (*hostvolume.StatusProjection, error)
	Peers        func(context.Context) ([]replication.ReplicaPeerStatus, error)
	Durable      func(context.Context) ([]durable.VolumeStatus, error)
	Residue      func(context.Context) (ResidueReport, error)
}

// Collect reads every configured source and returns the assembled report. If a
// source fails, Collect still returns a partial report from the sources that did
// succeed and returns a joined error naming the failed source.
func (c VolumeStatusReportCollector) Collect(ctx context.Context) (VolumeStatusReport, error) {
	var (
		errs         []error
		masterStatus *control.StatusResponse
		localStatus  *hostvolume.StatusProjection
		peers        []replication.ReplicaPeerStatus
		durableFacts []durable.VolumeStatus
		residue      ResidueReport
	)

	if c.MasterStatus != nil {
		v, err := c.MasterStatus(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("collect master status: %w", err))
		} else {
			masterStatus = v
		}
	}
	if c.LocalStatus != nil {
		v, err := c.LocalStatus(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("collect local status: %w", err))
		} else {
			localStatus = v
		}
	}
	if c.Peers != nil {
		v, err := c.Peers(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("collect peer status: %w", err))
		} else {
			peers = v
		}
	}
	if c.Durable != nil {
		v, err := c.Durable(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("collect durable status: %w", err))
		} else {
			durableFacts = v
		}
	}
	if c.Residue != nil {
		v, err := c.Residue(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("collect residue status: %w", err))
		} else {
			residue = v
		}
	}

	capturedAt := time.Time{}
	if c.Now != nil {
		capturedAt = c.Now()
	}

	return BuildVolumeStatusReport(VolumeStatusReportInput{
		CapturedAt:      capturedAt,
		Source:          c.Source,
		ProductRevision: c.ProductRevision,
		RunnerRevision:  c.RunnerRevision,
		MasterStatus:    masterStatus,
		LocalStatus:     localStatus,
		Peers:           peers,
		Durable:         durableFacts,
		Residue:         residue,
	}), errors.Join(errs...)
}
