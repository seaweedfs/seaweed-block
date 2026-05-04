package testops

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

func NewRegistry() *Registry {
	return &Registry{drivers: make(map[string]Driver)}
}

func (r *Registry) Register(scenario string, driver Driver) error {
	if scenario == "" {
		return fmt.Errorf("testops: scenario is required")
	}
	if driver == nil {
		return fmt.Errorf("testops: driver for %q is nil", scenario)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.drivers[scenario]; exists {
		return fmt.Errorf("testops: scenario %q already registered", scenario)
	}
	r.drivers[scenario] = driver
	return nil
}

func (r *Registry) Run(ctx context.Context, req RunRequest) (Result, error) {
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	r.mu.RLock()
	driver := r.drivers[req.Scenario]
	r.mu.RUnlock()
	if driver == nil {
		return Result{}, fmt.Errorf("testops: scenario %q is not registered", req.Scenario)
	}

	if timeout := req.Timeout(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	start := time.Now()
	res, err := driver.Run(ctx, req)
	if res.SchemaVersion == "" {
		res.SchemaVersion = SchemaVersion
	}
	if res.RunID == "" {
		res.RunID = req.RunID
	}
	if res.Scenario == "" {
		res.Scenario = req.Scenario
	}
	if res.SourceCommit == "" {
		res.SourceCommit = req.Source.Commit
	}
	if res.ArtifactDir == "" {
		res.ArtifactDir = req.ArtifactDir
	}
	if res.WallClock == 0 {
		res.WallClock = time.Since(start).Seconds()
	}
	if err != nil && res.Status == "" {
		res.Status = StatusError
	}
	return res, err
}
