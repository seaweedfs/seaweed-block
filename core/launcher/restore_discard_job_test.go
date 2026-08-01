package launcher

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestPhase175RestoreDiscardJobIsNodePinnedTokenlessAndLeafScoped(t *testing.T) {
	volume := lifecycle.VolumeRecord{
		Spec:         lifecycle.VolumeSpec{VolumeID: "restored-a", SourceSnapshotID: "snap-abc", ReplicationFactor: 1},
		RestoreState: lifecycle.VolumeRestoreAbortRequested,
		RestoreAbort: &lifecycle.RestoreAbortRecord{
			OperationID: "abort-001", SnapshotID: "snap-abc",
			Replicas: []lifecycle.RestoreAbortReplica{{ServerID: "m01", KubernetesNodeName: "node-a", ReplicaID: "r1", State: lifecycle.RestoreDiscardPending}},
		},
	}
	manifest, err := RenderRestoreDiscardJob(volume, volume.RestoreAbort.Replicas[0], RestoreDiscardJobConfig{
		Namespace: "kube-system", Image: "sw-block:test", StateHostPathBase: "/var/lib/sw-block/replicas",
	})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := DecodeRestoreDiscardJobIdentity(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if identity.OperationID != "abort-001" || identity.KubernetesNodeName != "node-a" || identity.VolumeID != "restored-a" || identity.ReplicaID != "r1" {
		t.Fatalf("identity=%+v", identity)
	}
	raw := string(manifest.YAML)
	for _, want := range []string{
		"activeDeadlineSeconds: 120",
		"automountServiceAccountToken: false",
		"restartPolicy: Never",
		"kubernetes.io/hostname: node-a",
		"path: /var/lib/sw-block/replicas/restored-a/r1",
		"type: Directory",
		"--allow-activated",
		"--evidence-file=/dev/termination-log",
		"terminationMessagePolicy: FallbackToLogsOnError",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("manifest missing %q:\n%s", want, raw)
		}
	}
	for _, forbidden := range []string{"snapshot-runtime-identity", "serviceAccountName:", "privileged: true", "DirectoryOrCreate"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("manifest contains forbidden %q:\n%s", forbidden, raw)
		}
	}
}
