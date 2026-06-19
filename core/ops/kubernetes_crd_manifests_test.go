package ops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPhase35D1CRDManifestsMatchManagedVolumeContract(t *testing.T) {
	contract := ManagedVolumeCRDContractDefinition()
	for _, tc := range []struct {
		path     string
		name     string
		kind     string
		plural   string
		singular string
	}{
		{
			path:     "charts/seaweed-block/crds/swblockclusters.block.seaweedfs.com.yaml",
			name:     "swblockclusters.block.seaweedfs.com",
			kind:     SwBlockClusterKind,
			plural:   "swblockclusters",
			singular: "swblockcluster",
		},
		{
			path:     "charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml",
			name:     "swblockvolumes.block.seaweedfs.com",
			kind:     SwBlockVolumeKind,
			plural:   "swblockvolumes",
			singular: "swblockvolume",
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			doc := readYAMLMap(t, tc.path)
			assertYAMLString(t, doc, "apiVersion", "apiextensions.k8s.io/v1")
			assertYAMLString(t, doc, "kind", "CustomResourceDefinition")

			metadata := yamlMap(t, doc, "metadata")
			assertYAMLString(t, metadata, "name", tc.name)

			spec := yamlMap(t, doc, "spec")
			assertYAMLString(t, spec, "group", contract.Group)
			assertYAMLString(t, spec, "scope", "Namespaced")

			names := yamlMap(t, spec, "names")
			assertYAMLString(t, names, "kind", tc.kind)
			assertYAMLString(t, names, "plural", tc.plural)
			assertYAMLString(t, names, "singular", tc.singular)

			versions := yamlSlice(t, spec, "versions")
			if len(versions) != 1 {
				t.Fatalf("%s versions=%d want 1", tc.kind, len(versions))
			}
			version := yamlMapFromValue(t, versions[0])
			assertYAMLString(t, version, "name", contract.Version)
			assertYAMLBool(t, version, "served", true)
			assertYAMLBool(t, version, "storage", true)
			if _, ok := yamlMap(t, version, "subresources")["status"]; !ok {
				t.Fatalf("%s missing status subresource", tc.kind)
			}

			statusSchema := yamlMap(t,
				yamlMap(t,
					yamlMap(t,
						yamlMap(t, version, "schema"),
						"openAPIV3Schema"),
					"properties"),
				"status")
			properties := yamlMap(t, statusSchema, "properties")
			if _, ok := properties["conditions"]; !ok {
				t.Fatalf("%s status schema missing conditions", tc.kind)
			}
			if _, ok := properties["evidenceRefs"]; !ok {
				t.Fatalf("%s status schema missing evidenceRefs", tc.kind)
			}
		})
	}
}

func TestPhase35D1SwBlockVolumeConditionEnumCoversContract(t *testing.T) {
	doc := readYAMLMap(t, "charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml")
	conditionType := yamlMap(t,
		yamlMap(t,
			yamlMap(t,
				yamlMap(t,
					yamlMap(t,
						yamlMap(t,
							yamlMap(t,
								yamlMap(t,
									yamlMap(t,
										yamlSlice(t, yamlMap(t, doc, "spec"), "versions")[0].(map[string]any),
										"schema"),
									"openAPIV3Schema"),
								"properties"),
							"status"),
						"properties"),
					"conditions"),
				"items"),
			"properties"),
		"type")
	enum := yamlStringSet(t, conditionType, "enum")
	for _, want := range []string{
		ConditionReady,
		ConditionRecovered,
		ConditionRecovering,
		ConditionBlocked,
		ConditionInvalid,
		ConditionCleanupRequired,
		ConditionEvidenceStale,
	} {
		if !enum[want] {
			t.Fatalf("condition enum missing %s: %+v", want, enum)
		}
	}
}

func TestPhase36D1SwBlockClusterActionabilitySchema(t *testing.T) {
	doc := readYAMLMap(t, "charts/seaweed-block/crds/swblockclusters.block.seaweedfs.com.yaml")
	statusProperties := crdStatusProperties(t, doc)
	for _, want := range []string{"nodes", "cleanup", "supportBundleRefs", "safeNextSteps"} {
		if _, ok := statusProperties[want]; !ok {
			t.Fatalf("SwBlockCluster status schema missing %s", want)
		}
	}

	nodeProperties := yamlMap(t, yamlMap(t, statusProperties, "nodes"), "items")
	nodeProperties = yamlMap(t, nodeProperties, "properties")
	for _, want := range []string{"name", "kubernetesNode", "internalIP", "schedulable", "ready", "status", "reasonCode", "conditions", "evidenceRefs"} {
		if _, ok := nodeProperties[want]; !ok {
			t.Fatalf("SwBlockCluster.status.nodes[] schema missing %s", want)
		}
	}

	cleanupProperties := yamlMap(t, yamlMap(t, statusProperties, "cleanup"), "properties")
	for _, want := range []string{"status", "k8sResidueCount", "iscsiResidueCount", "multipathResidueCount", "processResidueCount", "hostPathResidueCount", "failureCount", "evidenceRef"} {
		if _, ok := cleanupProperties[want]; !ok {
			t.Fatalf("SwBlockCluster.status.cleanup schema missing %s", want)
		}
	}

	installDriftProperties := yamlMap(t, yamlMap(t, statusProperties, "installDrift"), "properties")
	for _, want := range []string{"status", "reasonCode", "currentImage", "desiredImage", "currentCsiImage", "desiredCsiImage", "evidenceRef"} {
		if _, ok := installDriftProperties[want]; !ok {
			t.Fatalf("SwBlockCluster.status.installDrift schema missing %s", want)
		}
	}

	stepProperties := yamlMap(t, yamlMap(t, yamlMap(t, statusProperties, "safeNextSteps"), "items"), "properties")
	for _, want := range []string{"type", "mode", "command", "reasonCode", "mutationAllowed", "evidenceRefs"} {
		if _, ok := stepProperties[want]; !ok {
			t.Fatalf("SwBlockCluster.status.safeNextSteps[] schema missing %s", want)
		}
	}
}

func TestPhase38D3SwBlockVolumeAllowedActionEvaluationSchema(t *testing.T) {
	doc := readYAMLMap(t, "charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml")
	statusProperties := crdStatusProperties(t, doc)
	actionProperties := yamlMap(t, yamlMap(t, yamlMap(t, statusProperties, "allowedActions"), "items"), "properties")
	for _, want := range []string{"type", "mode", "sideEffectClass", "ownerExecutor", "decision", "decisionReason", "missingFacts", "mutationAllowed", "preconditions", "invariantRefs", "evidenceRequired", "evidenceRefs"} {
		if _, ok := actionProperties[want]; !ok {
			t.Fatalf("SwBlockVolume.status.allowedActions[] schema missing %s", want)
		}
	}
	if _, ok := actionProperties["evidence_required"]; ok {
		t.Fatalf("SwBlockVolume.status.allowedActions[] schema leaked snake_case evidence_required")
	}
	modeEnum := yamlStringSet(t, yamlMap(t, actionProperties, "mode"), "enum")
	for _, want := range []string{"read_only", "dry_run", "scripted"} {
		if !modeEnum[want] {
			t.Fatalf("SwBlockVolume.status.allowedActions[].mode enum missing %s: %+v", want, modeEnum)
		}
	}
}

func TestPhase39D2SwBlockVolumeDeleteSafetySchema(t *testing.T) {
	doc := readYAMLMap(t, "charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml")
	statusProperties := crdStatusProperties(t, doc)
	deleteSafetyProperties := yamlMap(t, yamlMap(t, statusProperties, "deleteSafety"), "properties")
	for _, want := range []string{"actionType", "decision", "state", "reason", "finalizerReleaseAllowed", "missingFacts", "evidenceRefs", "safeNextAction"} {
		if _, ok := deleteSafetyProperties[want]; !ok {
			t.Fatalf("SwBlockVolume.status.deleteSafety schema missing %s", want)
		}
	}
	for _, forbidden := range []string{"action_type", "finalizer_release_allowed", "safe_next_action"} {
		if _, ok := deleteSafetyProperties[forbidden]; ok {
			t.Fatalf("SwBlockVolume.status.deleteSafety schema leaked snake_case %s", forbidden)
		}
	}
}

func TestPhase46D2SwBlockVolumeReturnedReplicaSchema(t *testing.T) {
	doc := readYAMLMap(t, "charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml")
	statusProperties := crdStatusProperties(t, doc)
	returnedProperties := yamlMap(t, yamlMap(t, yamlMap(t, statusProperties, "replicaReintegrations"), "items"), "properties")
	for _, want := range []string{"replicaID", "state", "reasonCode", "frontendFenced", "frontendPrimaryReady", "ackEligible", "durableFrontierKnown", "durableFrontierLsn", "requiredFrontierKnown", "requiredFrontierLsn", "evidenceRefs"} {
		if _, ok := returnedProperties[want]; !ok {
			t.Fatalf("SwBlockVolume.status.replicaReintegrations[] schema missing %s", want)
		}
	}
	stateEnum := yamlStringSet(t, yamlMap(t, returnedProperties, "state"), "enum")
	for _, want := range []string{"fenced", "recovering", "ready", "blocked", "unknown"} {
		if !stateEnum[want] {
			t.Fatalf("SwBlockVolume.status.replicaReintegrations[].state enum missing %s: %+v", want, stateEnum)
		}
	}
	for _, forbidden := range []string{"replica_id", "frontend_primary_ready", "ack_eligible", "durable_frontier_lsn"} {
		if _, ok := returnedProperties[forbidden]; ok {
			t.Fatalf("SwBlockVolume.status.replicaReintegrations[] leaked snake_case %s", forbidden)
		}
	}
}

func TestOperatorStatusRBACIsStatusEventsOnly(t *testing.T) {
	raw := readRepoFile(t, "charts/seaweed-block/templates/operator-status-rbac.yaml")
	required := []string{
		`resources: ["nodes", "pods"]`,
		`resources: ["csidrivers", "csinodes"]`,
		`resources: ["swblockclusters", "swblockvolumes"]`,
		`verbs: ["get", "list", "watch"]`,
		`resources: ["swblockclusters/status", "swblockvolumes/status"]`,
		`verbs: ["get", "update", "patch"]`,
		`resources: ["events"]`,
		`verbs: ["create"]`,
	}
	for _, want := range required {
		if !strings.Contains(raw, want) {
			t.Fatalf("operator RBAC missing %q\n%s", want, raw)
		}
	}
	for _, forbidden := range []string{
		`resources: ["persistentvolumes"`,
		`resources: ["persistentvolumeclaims"`,
		`resources: ["deployments"`,
		`resources: ["secrets"`,
		`resources: ["storageclasses"`,
		`resources: ["swblockvolumes/finalizers"]`,
		`"delete"`,
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("operator status RBAC contains forbidden fragment %q\n%s", forbidden, raw)
		}
	}
}

func TestPhase35D1OperatorStatusRBACDefaultDisabled(t *testing.T) {
	values := readYAMLMap(t, "charts/seaweed-block/values.yaml")
	operatorStatus := yamlMap(t, values, "operatorStatus")
	assertYAMLBool(t, operatorStatus, "create", false)
	rbac := yamlMap(t, operatorStatus, "rbac")
	assertYAMLBool(t, rbac, "create", true)
}

func crdStatusProperties(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	return yamlMap(t,
		yamlMap(t,
			yamlMap(t,
				yamlMap(t,
					yamlMap(t,
						yamlSlice(t, yamlMap(t, doc, "spec"), "versions")[0].(map[string]any),
						"schema"),
					"openAPIV3Schema"),
				"properties"),
			"status"),
		"properties")
}

func TestPhase35D3OperatorStatusDeploymentCanRunDryRunOrStatusWriteMode(t *testing.T) {
	raw := readRepoFile(t, "charts/seaweed-block/templates/operator-status.yaml")
	for _, want := range []string{
		`kind: Deployment`,
		`name: sw-block-operator-status`,
		`serviceAccountName: {{ include "seaweed-block.fullname" . }}-operator-status`,
		`command: ["/usr/local/bin/sw-block"]`,
		`- "operator-status"`,
		`{{- if .Values.operatorStatus.dryRun }}`,
		`- "--dry-run"`,
		`- "--master-api={{ include "seaweed-block.blockmasterAddress" . }}"`,
		`- "--interval={{ .Values.operatorStatus.interval }}"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("operator-status deployment missing %q\n%s", want, raw)
		}
	}
}

func readRepoFile(t *testing.T, repoPath string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(repoPath)))
	if err != nil {
		t.Fatalf("read %s: %v", repoPath, err)
	}
	return string(raw)
}

func readYAMLMap(t *testing.T, repoPath string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := yaml.Unmarshal([]byte(readRepoFile(t, repoPath)), &out); err != nil {
		t.Fatalf("unmarshal %s: %v", repoPath, err)
	}
	return out
}

func yamlMap(t *testing.T, in map[string]any, key string) map[string]any {
	t.Helper()
	out, ok := in[key].(map[string]any)
	if !ok {
		t.Fatalf("key %q is %T, want map", key, in[key])
	}
	return out
}

func yamlMapFromValue(t *testing.T, in any) map[string]any {
	t.Helper()
	out, ok := in.(map[string]any)
	if !ok {
		t.Fatalf("value is %T, want map", in)
	}
	return out
}

func yamlSlice(t *testing.T, in map[string]any, key string) []any {
	t.Helper()
	out, ok := in[key].([]any)
	if !ok {
		t.Fatalf("key %q is %T, want slice", key, in[key])
	}
	return out
}

func assertYAMLString(t *testing.T, in map[string]any, key, want string) {
	t.Helper()
	if got, _ := in[key].(string); got != want {
		t.Fatalf("key %q=%q want %q", key, got, want)
	}
}

func assertYAMLBool(t *testing.T, in map[string]any, key string, want bool) {
	t.Helper()
	if got, _ := in[key].(bool); got != want {
		t.Fatalf("key %q=%t want %t", key, got, want)
	}
}

func yamlStringSet(t *testing.T, in map[string]any, key string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, item := range yamlSlice(t, in, key) {
		s, ok := item.(string)
		if !ok {
			t.Fatalf("enum item is %T, want string", item)
		}
		out[s] = true
	}
	return out
}
