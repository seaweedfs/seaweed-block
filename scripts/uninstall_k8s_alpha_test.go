package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUninstallK8sAlphaDeletesSeaweedISCSINodeRecords(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts", "uninstall-k8s-alpha.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"delete stale Seaweed Block iSCSI node records",
		"iscsi-nodes.before-scrub.txt",
		"iscsi-nodes.after-scrub.txt",
		"delete-iscsi-node-records.log",
		"awk '/io\\.seaweedfs/ {print $1, $2}'",
		"-m node -T \"$target\" -p \"$portal\" -o delete",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("uninstall script missing %q", want)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
