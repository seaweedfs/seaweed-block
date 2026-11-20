//go:build unix

package storeident

import (
	"os"
	"strings"
	"syscall"
)

var machineIDPaths = []string{"/etc/machine-id", "/var/lib/dbus/machine-id"}

func machineID() string {
	for _, p := range machineIDPaths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		if id := strings.TrimSpace(string(b)); id != "" {
			return id
		}
	}
	return ""
}

func devOf(dir string) uint64 {
	if dir == "" {
		return 0
	}
	var st syscall.Stat_t
	if err := syscall.Stat(dir, &st); err != nil {
		return 0
	}
	return uint64(st.Dev) //nolint:unconvert // int32 on some unix platforms
}
