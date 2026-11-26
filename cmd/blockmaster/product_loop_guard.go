package main

import (
	"fmt"
	"time"
)

// requireProductLoop refuses a start whose cluster spec holds a replicated
// volume (replication factor above 1) while the lifecycle product loop is
// off. On the cluster-spec path that loop alone binds a replicated volume's
// placement, mints its line and runs its promotion gate, so with the loop
// off the volume never gets a line. A static --topology (whose observation
// controller mints) and single-replica volumes are untouched.
func requireProductLoop(interval time.Duration, spec clusterSpecImport) error {
	if interval > 0 {
		return nil
	}
	for _, v := range spec.Volumes {
		if v.ReplicationFactor > 1 {
			return fmt.Errorf("cluster spec volume %s has replication factor %d but --lifecycle-product-loop-interval is 0 (off): "+
				"with --cluster-spec a replicated volume is bound and promoted only by that loop; set --lifecycle-product-loop-interval (for example 100ms)",
				v.VolumeID, v.ReplicationFactor)
		}
	}
	return nil
}
