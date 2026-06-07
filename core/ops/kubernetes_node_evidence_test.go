package ops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPhase37D2KubernetesNodeEvidenceEnrichesReadyAndBlockedNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes":
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"m01"},"spec":{},"status":{"addresses":[{"type":"InternalIP","address":"192.168.1.181"}],"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"m02"},"spec":{"unschedulable":true},"status":{"addresses":[{"type":"InternalIP","address":"192.168.1.184"}],"conditions":[{"type":"Ready","status":"False"}]}}
			]}`))
		case "/api/v1/namespaces/kube-system/pods":
			if got := r.URL.Query().Get("labelSelector"); got != "app=sw-block-csi-node" {
				t.Fatalf("labelSelector=%q", got)
			}
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"csi-m01"},"spec":{"nodeName":"m01"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"csi-m02"},"spec":{"nodeName":"m02"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`))
		case "/apis/storage.k8s.io/v1/csidrivers/block.csi.seaweedfs.com":
			_, _ = w.Write([]byte(`{"metadata":{"name":"block.csi.seaweedfs.com"}}`))
		case "/apis/storage.k8s.io/v1/csinodes":
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"m01"},"spec":{"drivers":[{"name":"block.csi.seaweedfs.com"}]}},
				{"metadata":{"name":"m02"},"spec":{"drivers":[{"name":"block.csi.seaweedfs.com"}]}}
			]}`))
		default:
			t.Fatalf("unexpected Kubernetes API path %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &KubernetesStatusClient{BaseURL: server.URL, HTTPClient: server.Client()}
	cluster := ClusterEvidence{
		CapturedAt: time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC),
		Nodes: []NodeEvidence{{
			NodeName:        "m01",
			KubernetesNode:  "m01",
			LastHeartbeatAt: time.Date(2026, 6, 6, 11, 59, 0, 0, time.UTC),
			ReplicaCount:    1,
		}},
	}

	enriched, err := client.EnrichNodeEvidence(context.Background(), "kube-system", cluster)
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}
	if len(enriched.Nodes) != 2 {
		t.Fatalf("nodes=%+v", enriched.Nodes)
	}
	m01 := nodeByKubernetesName(t, enriched.Nodes, "m01")
	if !m01.Ready || !m01.Schedulable || m01.InternalIP != "192.168.1.181" || m01.ReplicaCount != 1 {
		t.Fatalf("m01 evidence=%+v", m01)
	}
	status, reason := classifyNodeReadiness(m01)
	if status != ManagedVolumeStatusReady || reason != ReasonNodeReady {
		t.Fatalf("m01 status=%s reason=%s", status, reason)
	}
	m02 := nodeByKubernetesName(t, enriched.Nodes, "m02")
	if m02.Ready || m02.Schedulable {
		t.Fatalf("m02 raw facts=%+v", m02)
	}
	status, reason = classifyNodeReadiness(m02)
	if status != ManagedVolumeStatusUnknown || reason != ReasonNodeNotReady {
		t.Fatalf("m02 status=%s reason=%s", status, reason)
	}
	if !conditionReason(m02.Conditions, ReasonNodeNotReady) || !conditionReason(m02.Conditions, ReasonNodeSchedulingDisabled) {
		t.Fatalf("m02 conditions=%+v", m02.Conditions)
	}
}

func TestPhase37D2KubernetesNodeEvidenceProjectsCSIRegistrationBlockers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes":
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"m01"},"spec":{},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"m02"},"spec":{},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
			]}`))
		case "/api/v1/namespaces/kube-system/pods":
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"csi-m01"},"spec":{"nodeName":"m01"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
				{"metadata":{"name":"csi-m02"},"spec":{"nodeName":"m02"},"status":{"conditions":[{"type":"Ready","status":"False"}]}}
			]}`))
		case "/apis/storage.k8s.io/v1/csidrivers/block.csi.seaweedfs.com":
			_, _ = w.Write([]byte(`{"metadata":{"name":"block.csi.seaweedfs.com"}}`))
		case "/apis/storage.k8s.io/v1/csinodes":
			_, _ = w.Write([]byte(`{"items":[
				{"metadata":{"name":"m01"},"spec":{"drivers":[{"name":"block.csi.seaweedfs.com"}]}},
				{"metadata":{"name":"m02"},"spec":{"drivers":[]}}
			]}`))
		default:
			t.Fatalf("unexpected Kubernetes API path %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &KubernetesStatusClient{BaseURL: server.URL, HTTPClient: server.Client()}
	enriched, err := client.EnrichNodeEvidence(context.Background(), "kube-system", ClusterEvidence{})
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}
	m02 := nodeByKubernetesName(t, enriched.Nodes, "m02")
	if !conditionReason(m02.Conditions, ReasonCSIDriverNotRegistered) || !conditionReason(m02.Conditions, ReasonCSINodePodNotReady) {
		t.Fatalf("m02 conditions=%+v", m02.Conditions)
	}
	status, reason := classifyNodeReadiness(m02)
	if status != ManagedVolumeStatusBlocked || reason != ReasonCSIDriverNotRegistered {
		t.Fatalf("m02 status=%s reason=%s conditions=%+v", status, reason, m02.Conditions)
	}
	crd := swBlockNodeStatuses([]NodeEvidence{m02})[0]
	if crd.Status != ManagedVolumeStatusBlocked || crd.ReasonCode != ReasonCSIDriverNotRegistered {
		t.Fatalf("CRD node status=%+v", crd)
	}
	assertCondition(t, crd.Conditions, ConditionReady, "False", ReasonCSIDriverNotRegistered)
	assertCondition(t, crd.Conditions, ConditionBlocked, "True", ReasonCSIDriverNotRegistered)
	if len(crd.EvidenceRefs) == 0 || !strings.Contains(strings.Join(crd.EvidenceRefs, ","), "kubernetes/csinode/m02") {
		t.Fatalf("missing CRD evidence refs: %+v", crd.EvidenceRefs)
	}
}

func TestPhase37D2KubernetesNodeEvidenceProjectsMissingCSIDriver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes":
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"m01"},"spec":{},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`))
		case "/api/v1/namespaces/kube-system/pods":
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"csi-m01"},"spec":{"nodeName":"m01"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`))
		case "/apis/storage.k8s.io/v1/csidrivers/block.csi.seaweedfs.com":
			http.NotFound(w, r)
		case "/apis/storage.k8s.io/v1/csinodes":
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"m01"},"spec":{"drivers":[{"name":"block.csi.seaweedfs.com"}]}}]}`))
		default:
			t.Fatalf("unexpected Kubernetes API path %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &KubernetesStatusClient{BaseURL: server.URL, HTTPClient: server.Client()}
	enriched, err := client.EnrichNodeEvidence(context.Background(), "kube-system", ClusterEvidence{})
	if err != nil {
		t.Fatalf("enrich: %v", err)
	}
	m01 := nodeByKubernetesName(t, enriched.Nodes, "m01")
	if status, reason := classifyNodeReadiness(m01); status != ManagedVolumeStatusBlocked || reason != ReasonCSIDriverNotRegistered {
		t.Fatalf("m01 status=%s reason=%s conditions=%+v", status, reason, m01.Conditions)
	}
}

func nodeByKubernetesName(t *testing.T, nodes []NodeEvidence, name string) NodeEvidence {
	t.Helper()
	for _, node := range nodes {
		if node.KubernetesNode == name {
			return node
		}
	}
	t.Fatalf("missing node %s in %+v", name, nodes)
	return NodeEvidence{}
}

func conditionReason(conditions []ObservationCondition, reason string) bool {
	for _, condition := range conditions {
		if condition.Reason == reason {
			return true
		}
	}
	return false
}
