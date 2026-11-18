package component_test

import (
	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
)

// These scenarios prime an otherwise zero replica at a chosen historical
// floor. V3 requires a complete BASE anchor before any later sparse LSN.
func primeReplicaZeroBase(t *testing.T, c *component.Cluster, floor uint64) {
	t.Helper()
	s, ok := c.Replica(0).Store.(*storage.WALStore)
	if !ok || !s.FormatV3() {
		return
	}
	h, err := s.BeginBaseInstall(floor, [16]byte{0x61})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
}
