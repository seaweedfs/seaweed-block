//go:build swblock_format_v3_test

package storage

// CreateMappedWALStoreV3Test is available only to tagged cross-package caller
// controls. Production Create/Open remain unchanged until the activation gate.
func CreateMappedWALStoreV3Test(path string, blocks, blockSize, chunkBlocks uint32) (*WALStore, error) {
	return createMappedWALStoreV3ForTest(path, blocks, blockSize, 1<<20, WModeLagged, uint64(chunkBlocks))
}

func OpenMappedWALStoreV3Test(path string) (*WALStore, error) {
	return openWALStoreV3ForTest(path)
}

func FlushMappedWALStoreV3Test(s *WALStore) error {
	return s.flusher.flushOnce()
}

// CreateWALStoreV3Test creates an unmapped dark v3 store (mode A) for the
// tagged cross-package install controls (plan row 9b).
func CreateWALStoreV3Test(path string, blocks, blockSize uint32) (*WALStore, error) {
	s, err := createWALStoreV3ForTest(path, blocks, blockSize, 1<<20, WModeStrict)
	if err == nil {
		s.DisableAutoFlushForRecoveryTest()
	}
	return s, err
}

// SetInstallHooksV3Test sets the install admission hooks (DESIGN 8.7):
// beforeAdmit runs in a handle mutation before it acquires installMu,
// admitted after its ID check and before the write. nil clears a hook.
func SetInstallHooksV3Test(s *WALStore, beforeAdmit, admitted func()) {
	s.v3.testInstallBeforeAdmit = beforeAdmit
	s.v3.testInstallAdmitted = admitted
}

// BaseStateV3Test reports the selected descriptor's BASE state and ID.
func BaseStateV3Test(s *WALStore) (BaseState, [16]byte) {
	d := s.v3.pub.current()
	return d.BaseState, d.BaseInstallID
}
