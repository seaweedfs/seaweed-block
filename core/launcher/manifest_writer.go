package launcher

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteRenderedManifests writes rendered manifests idempotently. It is a
// filesystem seam for QA/harness consumption; applying to Kubernetes remains a
// separate operator action in this slice.
func WriteRenderedManifests(dir string, manifests []RenderedManifest) error {
	if dir == "" {
		return fmt.Errorf("launcher: manifest dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("launcher: mkdir %q: %w", dir, err)
	}
	for _, manifest := range manifests {
		if manifest.Name == "" {
			return fmt.Errorf("launcher: manifest missing name")
		}
		path := filepath.Join(dir, manifest.Name+".yaml")
		tmp, err := os.CreateTemp(dir, manifest.Name+".*.tmp")
		if err != nil {
			return fmt.Errorf("launcher: temp %s: %w", manifest.Name, err)
		}
		tmpName := tmp.Name()
		if _, err := tmp.Write(manifest.YAML); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return fmt.Errorf("launcher: write temp %s: %w", manifest.Name, err)
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpName)
			return fmt.Errorf("launcher: close temp %s: %w", manifest.Name, err)
		}
		if err := os.Rename(tmpName, path); err != nil {
			_ = os.Remove(tmpName)
			return fmt.Errorf("launcher: rename %s: %w", manifest.Name, err)
		}
	}
	return nil
}
