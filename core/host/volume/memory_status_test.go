package volume

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func TestStatusProjectionReportsProcessMemory(t *testing.T) {
	old := debug.SetMemoryLimit(192 << 20)
	defer debug.SetMemoryLimit(old)
	b := make([]byte, 1<<20)
	for i := range b {
		b[i] = 1
	}
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	m := s.statusProjection().Memory
	runtime.KeepAlive(b)
	if m.HeapAllocBytes < 1<<20 || m.HeapInuseBytes < m.HeapAllocBytes || m.SoftLimitBytes != 192<<20 {
		t.Fatalf("memory metrics: %+v", m)
	}
	if runtime.GOOS == "linux" && (m.RSSBytes == 0 || m.AnonymousRSSBytes == 0 || m.AnonymousRSSBytes > m.RSSBytes) {
		t.Fatalf("RSS metrics: %+v", m)
	}
}
