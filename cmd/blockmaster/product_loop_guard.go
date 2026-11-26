package main

import (
	"fmt"
	"time"
)

// requireProductLoop refuses a start whose cluster spec holds a replicated
// volume (replication factor above 1) that the master could never decide:
//   - one whose replication factor differs from --expected-slots-per-volume:
//     the controller validates every replicated volume against that one
//     count and refuses the whole decision when one differs, so it would
//     stop every replicated volume's assignment and failover;
//   - any, while the lifecycle product loop is off: on the cluster-spec path
//     that loop alone binds a replicated volume's placement, mints its line
//     and runs its promotion gate, so with the loop off it never gets a line.
//
// A static --topology (whose observation controller mints) and
// single-replica volumes are untouched.
func requireProductLoop(interval time.Duration, expectedSlots int, spec clusterSpecImport) error {
	// Every cluster-spec volume has a placement intent (a placement-only
	// entry, without size_bytes, has no volume spec), so the placements are
	// what is read.
	for _, p := range spec.Placements {
		if p.DesiredRF > 1 && p.DesiredRF != expectedSlots {
			return fmt.Errorf("cluster spec volume %s has replication factor %d but --expected-slots-per-volume is %d: "+
				"the master decides replicated volumes of one slot count only (one replicated factor per cluster)",
				p.VolumeID, p.DesiredRF, expectedSlots)
		}
	}
	if interval > 0 {
		return nil
	}
	for _, p := range spec.Placements {
		if p.DesiredRF > 1 {
			return fmt.Errorf("cluster spec volume %s has replication factor %d but --lifecycle-product-loop-interval is 0 (off): "+
				"with --cluster-spec a replicated volume is bound and promoted only by that loop; set --lifecycle-product-loop-interval (for example 100ms)",
				p.VolumeID, p.DesiredRF)
		}
	}
	return nil
}
