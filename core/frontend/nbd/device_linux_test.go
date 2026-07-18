//go:build linux

package nbd

import "testing"

func TestNBDNumberAcceptsOnlyWholeDevices(t *testing.T) {
	if got := nbdNumber("/dev/nbd12"); got != 12 {
		t.Fatalf("nbdNumber(/dev/nbd12)=%d want 12", got)
	}
	invalid := int(^uint(0) >> 1)
	for _, path := range []string{"/dev/nbd0p1", "/dev/nbd-control", "/dev/not-nbd"} {
		if got := nbdNumber(path); got != invalid {
			t.Fatalf("nbdNumber(%q)=%d want invalid", path, got)
		}
	}
}
