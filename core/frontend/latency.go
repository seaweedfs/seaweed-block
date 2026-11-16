package frontend

import (
	"context"

	"github.com/seaweedfs/seaweed-block/core/iolatency"
)

// BeginDurabilityRequest instruments only backends that expose the optional
// diagnostic seam. Other backends retain their existing behavior.
func BeginDurabilityRequest(ctx context.Context, backend any, kind string) (context.Context, *iolatency.Request) {
	if b, ok := backend.(interface {
		BeginDurabilityRequest(context.Context, string) (context.Context, *iolatency.Request)
	}); ok {
		return b.BeginDurabilityRequest(ctx, kind)
	}
	return ctx, nil
}
