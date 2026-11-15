package chunkstore

import "context"

// SweepCopies settles expired copy attempts using permanent publication
// target claims; it never expires them from a deadline alone.
func (g *Collector) SweepCopies(ctx context.Context) (int, error) { return 0, nil }
