# Current Plan: Phase 21 - Helm Activation MVP

Status: D1 dev pass. Helm chart skeleton renders and dry-runs for both
single-node loopback defaults and three-node external iSCSI/CHAP values.

Previous closed capability:

- `finished-plans/phase20_finishedplan_activation_day1_ops_mvp.md`
- v0.2 alpha Day-1 activation: script install, first PVC, writer/reader
  checksum, read-only report, and strict cleanup.

## Product Question

Can a Kubernetes user install Seaweed Block through a normal Helm path and
complete the same first-volume product loop without treating repo-local scripts
as the primary install surface?

```text
preflight
-> generate values.day1.yaml
-> helm install
-> rollout readiness
-> create PVC
-> writer/reader checksum
-> sw-block ops report
-> helm uninstall
-> host cleanup verification
```

## Release Target

`v0.3-alpha`: Helm activation for supported Kubernetes labs.

The release should make the install contract regular, reviewable, and closer to
what users expect from a Kubernetes storage product. It should not introduce an
operator lifecycle contract yet.

## Core Thesis

v0.2 proved the product loop with scripts. v0.3 should package that loop as a
Kubernetes-native chart while preserving the same evidence discipline:
readiness, PVC creation, data check, product-owned report, and cleanup.

Helm is the right next step before an operator because:

- it gives users a standard install/uninstall surface,
- it makes values, RBAC, images, StorageClass, and CHAP settings explicit,
- it keeps lifecycle logic simple while the product contract is still moving,
- it creates a stable base for a later operator with CRDs and Conditions.

## Non-Claims

Do not claim:

- production readiness,
- operator / CRD lifecycle management,
- automatic upgrades or rollback safety,
- backup, snapshot, or restore,
- mutating dashboard/admin actions,
- broad Kubernetes distro support,
- broad performance SLOs,
- physical-host-loss survival beyond the gated node-loss claim,
- transparent node-loss failover beyond already gated Stage 2/Node-Loss scopes,
- NVMe ANA parity.

## Scope

### D1: Helm Chart Skeleton

Create `charts/seaweed-block/` with templates for the existing install
surface:

- blockmaster Deployment and Service,
- CSI controller Deployment,
- CSI node DaemonSet,
- RBAC / ServiceAccounts / ClusterRoleBindings,
- CSIDriver,
- StorageClass,
- cluster-spec ConfigMap,
- optional CHAP Secret,
- image, pull policy, tag, and digest values,
- namespace and naming overrides,
- ACK profile and expected slots per volume,
- external iSCSI/status settings,
- Stage 2 multipath opt-in settings,
- launcher state hostPath settings.

The chart should template existing semantics. It must not fork product behavior
from `scripts/install-k8s-alpha.sh`.

Current D1 checkpoint:

- `helm lint charts/seaweed-block` passes.
- `helm template sw-block charts/seaweed-block --namespace kube-system` passes
  Kubernetes client dry-run.
- Three-node external values render external iSCSI/status, CHAP,
  sync-quorum, expected slots, Stage 2 multipath, non-loopback IPs, and
  loopback publish-target rejection.
- Chart render fails closed if external iSCSI is enabled without CHAP.

### D2: Day-1 Values Generator and Preflight

Add a small user-facing helper that turns a live cluster into a Helm values
file:

```text
scripts/generate-helm-values-day1.sh
```

Required behavior:

- detect Ready schedulable nodes and InternalIP values,
- choose loopback mode for single-node labs,
- choose external iSCSI/status + CHAP for multi-node labs,
- write a `values.day1.yaml`,
- print a concise activation summary,
- fail closed on missing Kubernetes access, missing iSCSI prerequisites, or
  unsafe topology,
- preserve the v0.2 distinction between local/internal images and immutable
  GHCR release images.

Current D2 checkpoint:

- `scripts/generate-helm-values-day1.sh` writes `values.day1.yaml` from
  `kubectl get nodes`.
- One Ready node generates loopback mode with the real Kubernetes node name.
- Multiple Ready nodes generate external iSCSI/status, CHAP, loopback publish
  rejection, and one `blockNodes` entry per Ready schedulable node.
- RF greater than discovered Ready node count fails closed.
- Generated three-node RF=3 sync-quorum values pass `helm lint`,
  `helm template`, and Kubernetes client dry-run.

### D3: Helm Install First-Volume Gate

Create a TestOps scenario that exercises the PM/user path:

```text
helm install sw-block charts/seaweed-block -f values.day1.yaml
-> wait for blockmaster, CSI controller, CSI node, StorageClass
-> create PVC through Kubernetes
-> writer pod writes /data/demo.bin
-> reader pod verifies /data/demo.bin
-> sw-block ops report emits HTML + JSON + JSONL timeline
```

Acceptance:

- PVC is Bound,
- blockvolume Deployment is stable,
- writer and reader checksum pass,
- report directory contains `index.html`, `cluster-evidence.json`,
  `timeline.jsonl`, and `summary.txt`,
- report is read-only and has no mutating actions,
- first-volume summary names the chart release, namespace, image tags/digests,
  StorageClass, PVC, volume ID, writer/reader results, and report path.

### D4: Helm Uninstall and Host Cleanup Gate

Helm uninstall only removes Kubernetes objects. The product still needs a
documented host cleanup/check step for iSCSI and multipath residue.

Acceptance:

- `helm uninstall sw-block` completes,
- StorageClass and product workloads are gone,
- demo PVC/pods are removed by the scenario,
- no active iSCSI sessions remain,
- no stale iSCSI node records remain for the test IQNs,
- no matching blockmaster/blockvolume/blockcsi processes remain,
- no test-scoped hostPath residue remains,
- cleanup failures produce support evidence instead of silent success.

### D5: Published Image Release Validation

Keep two image paths:

- local/internal images for fast engineering QA,
- immutable GHCR `sha-<commit>` images for PM/release validation.

Acceptance:

- Helm values support image tags and digests,
- activation/report output records both configured image and observed digest,
- mutable `:alpha` is documented as smoke/demo only,
- release validation uses immutable `sha-<commit>` tags.

### D6: User Docs and README

After D1-D5 are green:

- README default install path should become Helm,
- script activation should move to dev/lab fallback language,
- tutorial should show one-node and three-node expectations clearly,
- release note should identify `v0.3-alpha` as Helm activation, not new HA,
- non-claims must remain explicit.

## Test Strategy

Use TDD at the packaging boundary:

- chart render tests or golden `helm template` checks,
- preflight/values-generator unit checks for one-node and multi-node examples,
- TestOps red/green scenarios for install, first volume, report, and uninstall,
- local/internal image run before any GHCR validation,
- immutable GHCR run before release note.

Candidate scenarios:

- `testops/scenarios/helm-activation-install-chain.yaml`
- `testops/scenarios/helm-first-volume-chain.yaml`
- `testops/scenarios/helm-uninstall-cleanup-chain.yaml`

## Guardrails

- Do not add operator/CRD work in this phase.
- Do not add mutating dashboard actions.
- Do not hide host cleanup behind Helm if Helm cannot actually enforce it.
- Do not weaken existing safe-refusal, sync-quorum, or observation contracts.
- Do not make Helm values drift from the script path; if behavior differs,
  document and gate it explicitly.

## Known Risks

- RBAC scope may need one more tightening pass before public release. The chart
  should make namespace, ServiceAccounts, and ClusterRole use visible rather
  than implicit.
- Helm cannot verify host iSCSI/multipath cleanup by itself. The close gate must
  include a cleanup verifier.
- Multi-node defaults can regress into loopback mistakes. The values generator
  must make network mode and target IPs visible in the summary.
- Published image drift can reappear if release validation uses mutable tags.

## Definition of Done

Phase 21 closes only when a cold user/PM path can run:

```text
preflight
generate values.day1.yaml
helm install
first PVC writer/reader checksum
sw-block ops report
helm uninstall
cleanup verification
```

and the resulting bundle self-explains install status, volume status, report
artifacts, image identity, and cleanup result without SSH log spelunking.
