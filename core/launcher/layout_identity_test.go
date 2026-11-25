package launcher

import (
	"strings"
	"testing"
)

func TestLauncherCarriesTheMasterLayoutIdentity(t *testing.T) {
	plan := sampleWorkloadPlan()
	plan.LayoutID = "1234567890abcdef1234567890abcdef"
	manifests, err := RenderBlockVolumeDeployments(plan, K8sRenderConfig{MasterAddr: "master:9333", DurableRootBase: "/data"})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(manifests[0].YAML)
	if !strings.Contains(raw, "--layout-id="+plan.LayoutID) {
		t.Fatal("master identity missing from launch args", raw)
	}
	if root := durableRoot(plan, plan.Replicas[0], K8sRenderConfig{DurableRootBase: "/data"}); root != "/data/r1/pvc-a-12345678" {
		t.Fatal("local root is not keyed by layout identity", root)
	}
	plan.LayoutID = "bad"
	if _, err := RenderBlockVolumeDeployments(plan, K8sRenderConfig{MasterAddr: "master:9333"}); err == nil {
		t.Fatal("launcher accepted a malformed layout identity")
	}
}
