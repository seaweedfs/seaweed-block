package testops

import (
	"strings"
	"testing"
)

// M2 restart: the shipped supervisor bounds retries after exit 75.
func TestBlockvolumeUnitsBoundStartupRetries(t *testing.T) {
	for _, name := range []string{"sw-blockvolume.service", "sw-blockvolume-ublk.service"} {
		t.Run(name, func(t *testing.T) {
			body := readRepoFile(t, "deploy", "systemd", name)
			section := ""
			seen := map[string]bool{}
			for _, line := range strings.Split(body, "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "[") {
					section = line
					continue
				}
				key, value, ok := strings.Cut(line, "=")
				if !ok {
					continue
				}
				want, checked := map[string]string{"StartLimitIntervalSec": "600", "StartLimitBurst": "10"}[key]
				if !checked {
					continue
				}
				if seen[key] || section != "[Unit]" || value != want {
					t.Errorf("%s: %s=%s in %s (duplicate=%t), want %s=%s once in [Unit]", name, key, value, section, seen[key], key, want)
				}
				seen[key] = true
			}
			for _, key := range []string{"StartLimitIntervalSec", "StartLimitBurst"} {
				if !seen[key] {
					t.Errorf("%s: missing [Unit] %s", name, key)
				}
			}
		})
	}
}
