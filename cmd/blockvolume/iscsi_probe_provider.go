package main

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

type durableProbeProvider struct {
	provider *durable.DurableProvider
}

var _ interface {
	ProbeBackend(context.Context, string) (frontend.Backend, error)
} = (*durableProbeProvider)(nil)

func (p *durableProbeProvider) ProbeBackend(_ context.Context, volumeID string) (frontend.Backend, error) {
	if p == nil || p.provider == nil {
		return nil, frontend.ErrNotReady
	}
	if b := p.provider.Backend(volumeID); b != nil {
		return b, nil
	}
	if _, err := p.provider.EnsureStorage(volumeID); err != nil {
		return nil, err
	}
	if b := p.provider.Backend(volumeID); b != nil {
		return b, nil
	}
	return nil, fmt.Errorf("%w: no probe backend for %s", frontend.ErrNotReady, volumeID)
}
