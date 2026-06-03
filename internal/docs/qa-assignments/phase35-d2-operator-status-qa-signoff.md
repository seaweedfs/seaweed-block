# QA Sign-off - Phase 35 D2 Dry-Run Operator Status Controller

Verdict: **PASS.** The dry-run operator-status controller packages cleanly,
the live k3s API accepts it, the `dryRun=false` guard rejects as designed, it
is disabled by default, and the **live smoke proves the controller runs
read-only: it observes cluster status via blockmaster and writes ZERO CRD
objects** (`mutation_allowed=false`). One minor transient (startup
connection-race) noted, non-blocking.

Date: 2026-06-02

Source commit: `6d8abba phase35: package dry-run operator status controller`
(branch `phase33-testops-failure-hardening`)

## Checks

| Check | Result | Evidence |
|---|---|---|
| Unit tests | PASS | `go test ./cmd/blockcsi ./core/ops ./cmd/sw-block` — all ok |
| Render (create=true) incl. operator-status Deployment | PASS | 3 Deployments (blockmaster, csi-controller, operator-status) + 2 CRDs + operator SA/RBAC |
| Live k3s API accepts manifests | PASS | `kubectl apply --dry-run=server` accepted `deployment.apps/sw-block-operator-status` + SA + ClusterRole + ClusterRoleBinding + both CRDs |
| Guard: `dryRun=false` rejected | PASS | `helm template --set operatorStatus.dryRun=false` → `Error: operatorStatus.create=true currently requires operatorStatus.dryRun=true; real CRD status writes are not wired yet` |
| Disabled by default | PASS | default render = 0 `sw-block-operator-status` objects |
| sw-block binary in image | PASS | fresh `sw-block:local` has `/usr/local/bin/sw-block` (19MB) |
| **Live smoke: controller runs read-only** | PASS | see below |

## Live Smoke Detail

Built fresh images (Dockerfile.sw-block now ships `/usr/local/bin/sw-block`),
installed with `--set operatorStatus.create=true`, CRDs auto-installed from
`crds/`.

operator-status pod: `1/1 Running` on m02, command
`/usr/local/bin/sw-block ops operator-status --dry-run`.

Successful dry-run iteration logged:

```text
operator_status=dry_run cluster=kube-system/sw-block volumes=0 events=0 mutation_allowed=false
cluster_status volumes=0 ready=0 blocked=0 stale=0
```

**Read-only proof:** after the controller ran its dry-run iteration,

```text
kubectl get swblockclusters,swblockvolumes -A
-> No resources found
```

The controller observed cluster status from blockmaster (read path) and wrote
**zero** objects to the SwBlockCluster / SwBlockVolume CRDs. `mutation_allowed=false`
is reported. This is exactly the D2 contract: a packaged controller that runs,
observes, and does not write — the dry-run foundation before real status
publication is wired.

## Non-Blocking Finding

### N1: transient blockmaster connection-refused on startup

The operator-status pod's first dry-run iterations logged:

```text
sw-block ops operator-status: rpc error: code = Unavailable desc = connection error:
  dial tcp 10.43.37.122:9333: connect: connection refused
sw-block ops operator-status: dry-run iteration failed exit=2; retrying in 30s
```

This is a startup-ordering race: the operator-status pod starts and runs its
first iteration before blockmaster's gRPC (`blockmaster:9333`) is listening.
It self-heals — the 30s retry succeeds once blockmaster is up (the successful
`operator_status=dry_run` line above is from a later iteration). The pod stays
`1/1 Running` (its own readiness does not depend on blockmaster), and
`helm install --wait` passed.

Not a blocker. Worth a small polish so first-boot logs are not alarming:
either a short initial backoff, or treat "blockmaster not yet reachable" as an
expected `EvidenceStale`/pending state rather than `exit=2 failed`. A cold
operator reading the logs should see "waiting for blockmaster," not an error.

## Non-Claims Held

D2 packages a controller that runs in **dry-run only**. It does not write CRD
status (proven: zero CR objects), `mutation_allowed=false`, and the chart guard
hard-rejects `dryRun=false` until real writes are wired. Disabled by default;
opt-in via `operatorStatus.create=true` (which still forces dryRun=true).

## Lab State

Clean — uninstalled, CRDs deleted (helm uninstall does not remove `crds/`), no
sw-block pods, no operator-status pod, no iSCSI sessions, no multipath.

## Bottom Line

- D2 dry-run operator-status controller: **PASS.**
- Packaging (render + server dry-run + guard + default-disabled): all green.
- Live runtime: controller starts, runs `--dry-run`, reads cluster status,
  reports `mutation_allowed=false`, and writes **zero** CRD objects.
- One non-blocking polish: tame the first-boot blockmaster connection-race log
  (expected-pending rather than error).
- D2 can close. Next slice (D3+) wiring real status writes should be
  QA-validated with `dryRun=false` actually allowed and the controller
  publishing real `SwBlockVolume.status` — at which point the read-only-proof
  flips to a "writes correct status, still no storage mutation" check.
