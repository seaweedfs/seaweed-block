//go:build chunkstore_integration

package master

import "testing"

// Only replace MemChunkRemote with the actual local filer/blob implementation.
// The lease gate, 1GiB image, random writes/FUA/Flush and timer0 stay unchanged.
func TestChunkPressureLocalFilerDiagnostic(t *testing.T) {
	cluster := startWorkspaceWeed(t)
	remote := cluster.remote(t, "/swblock/diagnostic/v1")
	runChunkPressureRandomDiagnostic(t, []bool{true}, remote)
}
