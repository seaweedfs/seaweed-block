package launcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPhase175KubernetesClientObservesAndOwnsOnlyExactRestoreDiscardJob(t *testing.T) {
	identity := RestoreDiscardJobIdentity{
		Namespace: "kube-system", Name: restoreDiscardJobName("abort-001", "restored-a", "r1"),
		OperationID: "abort-001", SnapshotID: "snap-abc", VolumeID: "restored-a", ReplicaID: "r1", KubernetesNodeName: "node-a",
	}
	labels := map[string]string{LabelApp: AppRestoreDiscard, LabelDiscardOperationHash: restoreDiscardIdentityHash(identity.OperationID)}
	annotations := restoreDiscardAnnotations(identity)
	var applied, deleted bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/batch/v1/namespaces/kube-system/jobs/"+identity.Name:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": identity.Name, "namespace": identity.Namespace, "creationTimestamp": "2026-08-01T12:00:00Z", "labels": labels, "annotations": annotations},
				"spec":     map[string]any{"activeDeadlineSeconds": restoreDiscardActiveDeadlineSeconds},
				"status":   map[string]any{"succeeded": 1},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/kube-system/pods":
			switch r.URL.Query().Get("labelSelector") {
			case launcherBlockVolumeSelectorForTest(identity.VolumeID, identity.ReplicaID):
				_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{
					"name": "old-blockvolume-pod", "namespace": identity.Namespace,
					"labels": map[string]string{LabelVolume: identity.VolumeID, LabelReplica: identity.ReplicaID},
				}}}})
				return
			case "job-name=" + identity.Name:
			default:
				t.Fatalf("selector=%q", r.URL.Query().Get("labelSelector"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
				"metadata": map[string]any{"name": identity.Name + "-pod", "namespace": identity.Namespace, "labels": labels, "annotations": annotations},
				"spec":     map[string]any{"nodeName": "node-a"},
				"status":   map[string]any{"phase": "Succeeded", "containerStatuses": []any{map[string]any{"name": "restore-discard", "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": `{"operation_id":"abort-001"}`}}}}},
			}}})
		case r.Method == http.MethodPatch && r.URL.Path == "/apis/batch/v1/namespaces/kube-system/jobs/"+identity.Name:
			applied = true
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/apply-patch+yaml") {
				t.Fatalf("content-type=%q", r.Header.Get("Content-Type"))
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && r.URL.Path == "/apis/batch/v1/namespaces/kube-system/jobs/"+identity.Name:
			deleted = true
			if r.URL.Query().Get("propagationPolicy") != "Foreground" {
				t.Fatalf("propagation=%q", r.URL.Query().Get("propagationPolicy"))
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	client := NewKubernetesDeploymentClient(KubernetesDeploymentClientConfig{BaseURL: server.URL})
	job, exists, err := client.GetRestoreDiscardJob(context.Background(), identity)
	if err != nil || !exists || job.Succeeded != 1 || job.CreatedAt.IsZero() || job.ActiveDeadlineSeconds != restoreDiscardActiveDeadlineSeconds {
		t.Fatalf("job=%+v exists=%t error=%v", job, exists, err)
	}
	pods, err := client.ListRestoreDiscardPods(context.Background(), identity)
	if err != nil || len(pods) != 1 || !pods[0].Terminated || pods[0].ExitCode != 0 || pods[0].NodeName != "node-a" {
		t.Fatalf("pods=%+v error=%v", pods, err)
	}
	blockPods, err := client.ListBlockVolumePods(context.Background(), identity.Namespace, identity.VolumeID, identity.ReplicaID)
	if err != nil || len(blockPods) != 1 || blockPods[0].Name != "old-blockvolume-pod" {
		t.Fatalf("blockPods=%+v error=%v", blockPods, err)
	}
	manifest := RenderedManifest{Name: identity.Name, YAML: []byte(`apiVersion: batch/v1
kind: Job
metadata:
  name: ` + identity.Name + `
  namespace: kube-system
  labels:
    app: sw-block-restore-discard
    sw-block.seaweedfs.com/discard-operation-hash: ` + restoreDiscardIdentityHash(identity.OperationID) + `
  annotations:
    sw-block.seaweedfs.com/discard-operation: abort-001
    sw-block.seaweedfs.com/discard-snapshot: snap-abc
    sw-block.seaweedfs.com/discard-volume: restored-a
    sw-block.seaweedfs.com/discard-replica: r1
    sw-block.seaweedfs.com/discard-node: node-a
`)}
	if err := client.ApplyRestoreDiscardJob(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteRestoreDiscardJob(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if !applied || !deleted {
		t.Fatalf("applied=%t deleted=%t", applied, deleted)
	}
}

func launcherBlockVolumeSelectorForTest(volumeID, replicaID string) string {
	return LabelVolume + "=" + volumeID + "," + LabelReplica + "=" + replicaID
}
