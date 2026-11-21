package volume

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

// ProcessMemoryStatus separates live heap from resident pages awaiting return
// to the OS. RSS fields are zero on systems without /proc/self/status.
type ProcessMemoryStatus struct {
	HeapAllocBytes    uint64 // Go heap bytes in live objects (runtime.MemStats.HeapAlloc)
	HeapInuseBytes    uint64 // Go heap bytes in in-use spans (HeapInuse)
	HeapSysBytes      uint64 // Go heap bytes obtained from the OS (HeapSys)
	HeapReleasedBytes uint64 // Go heap bytes returned to the OS (HeapReleased)
	SoftLimitBytes    int64  // the Go soft memory limit in force (--memory-limit-mib or GOMEMLIMIT)
	RSSBytes          uint64 // resident set size from /proc/self/status (VmRSS); 0 off Linux
	AnonymousRSSBytes uint64 // anonymous resident memory (RssAnon); 0 off Linux
}

func processMemoryStatus() ProcessMemoryStatus {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	out := ProcessMemoryStatus{HeapAllocBytes: m.HeapAlloc, HeapInuseBytes: m.HeapInuse, HeapSysBytes: m.HeapSys, HeapReleasedBytes: m.HeapReleased, SoftLimitBytes: debug.SetMemoryLimit(-1)}
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[0] != "VmRSS:" && fields[0] != "RssAnon:" {
			continue
		}
		n, e := strconv.ParseUint(fields[1], 10, 64)
		if e != nil {
			continue
		}
		if fields[0] == "VmRSS:" {
			out.RSSBytes = n * 1024
		} else {
			out.AnonymousRSSBytes = n * 1024
		}
	}
	return out
}
