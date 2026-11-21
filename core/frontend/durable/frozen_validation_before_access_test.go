package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestFrozenValidationPrecedesAnyProviderAccess(t *testing.T) {
	// Deliberately poisoned receiver: validation must return before touching
	// even the provider mutex/volume map, much less StopIO, capture or HEAD.
	var p *DurableProvider
	for _, at := range []time.Time{{}, time.Unix(0, -1), time.Now().Add(time.Hour)} {
		result, last, err := p.DetachVolumeFrozen(context.Background(), "v1", storage.FrozenDetachOptions{StartedAt: at})
		if !errors.Is(err, storage.ErrInvalidFreezeBudget) || result.Head.Gen != 0 || last != 0 {
			t.Fatalf("invalid input accessed provider or returned an applied point: %+v %d %v", result, last, err)
		}
	}
}
