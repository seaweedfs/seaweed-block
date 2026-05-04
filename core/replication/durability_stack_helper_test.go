package replication

import "runtime"

// runtimeStackFn is a package-test helper used by
// TestDurabilityCoordinator_ZeroPeers_StandaloneFastPath to extract
// the current goroutine ID. Isolated in its own file so the runtime
// import lives apart from the main test file.
var runtimeStackFn = runtime.Stack
