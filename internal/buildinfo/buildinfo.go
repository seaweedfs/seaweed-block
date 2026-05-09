package buildinfo

import (
	"fmt"
	"runtime/debug"
)

// Version returns a compact provenance string suitable for lab artifacts.
func Version(component string) string {
	revision := "unknown"
	modified := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if setting.Value != "" {
					revision = setting.Value
				}
			case "vcs.modified":
				if setting.Value != "" {
					modified = setting.Value
				}
			}
		}
	}
	return fmt.Sprintf("%s revision=%s modified=%s", component, revision, modified)
}
