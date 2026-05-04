// Ownership: sw test-support (per test-spec §9: non-test helper
// files *_test_support.go are sw-owned). Holds shared fakes
// used by QA-owned test files in this package.
package frontend_test

import (
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// fakeProjectionView is a concurrency-safe, settable
// ProjectionView used by all L0 tests. set() swaps the current
// projection; Projection() returns the latest.
type fakeProjectionView struct {
	mu   sync.Mutex
	proj frontend.Projection
}

func newFakeProjectionView(p frontend.Projection) *fakeProjectionView {
	return &fakeProjectionView{proj: p}
}

func (f *fakeProjectionView) Projection() frontend.Projection {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.proj
}

func (f *fakeProjectionView) set(p frontend.Projection) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proj = p
}
