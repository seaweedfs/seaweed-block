# QA Sign-off - Phase 36 D3 Support Evidence Pointers

Verdict: **PASS, no findings.** Blocked operational status surfaces support
evidence pointers and a safe, read-only collection next-step across every
surface (CRD status, operator-snapshot, summary.txt, index.html, dashboard,
explain), the surfaces agree, and the controller suggests the collection command
without executing it or gaining any mutation power.

Date: 2026-06-05

Source commit: `c0bf3ec phase36: project support evidence pointers`
(branch `phase33-testops-failure-hardening`)

Environment: k3s `v1.34.4+k3s1`, write-mode operator-status, fresh `c0bf3ec`
images. Driver: CSI image-pull blocked bundle (kube-system pod dump with
`ImagePullBackOff`), projected via from-bundle under a dedicated
`--cluster-name d3-cluster` (so the live controller's `sw-block` writes don't
race the gate).

## G1 — Blocked / Support-Bundle Path — PASS

```text
operator_status=write_status cluster=kube-system/d3-cluster volumes=1 events=2 mutation_allowed=false  EXIT=0

SwBlockCluster.status.supportBundleRefs = ["/tmp/d3bb/demo/kube-system-pods-deploys.txt"]
SwBlockCluster.status.safeNextSteps[0]:
  type=observe.collect_bundle
  mode=read_only
  mutationAllowed=false
  reason=blocked
  command=bash scripts/collect-helm-support-bundle.sh "$PWD"
```

All G1 assertions hold: ≥1 support ref, `observe.collect_bundle` step,
`mode=read_only`, `mutationAllowed=false`, command mentions
`collect-helm-support-bundle.sh`.

## G2 — Report Agreement — PASS

`sw-block ops report --from-bundle /tmp/d3bb --out …`:

```text
summary.txt:
  support_bundle_ref=/tmp/d3bb/demo/kube-system-pods-deploys.txt
  safe_next_step=observe.collect_bundle mode=read_only mutation_allowed=false
    command="bash scripts/collect-helm-support-bundle.sh \"$PWD\"" reason=blocked

index.html:  "Support Evidence"  +  "Safe Next Steps"   (both sections present)

operator-snapshot.json:
  cluster.support_bundle_refs = ["/tmp/d3bb/demo/kube-system-pods-deploys.txt"]   (== CRD)
  cluster.safe_next_steps     = [{type:observe.collect_bundle, mode:read_only}]   (== CRD)
```

The operator-snapshot `support_bundle_refs` / `safe_next_steps` match the CRD
status exactly. PASS.

## G3 — From-Bundle Replay — PASS

```text
explain volume --from-bundle /tmp/d3bb unknown:
  volume unknown status=blocked reason=csi_node_image_pull_failed
  condition Attach severity=error reason=csi_node_image_pull_failed (ImagePullBackOff … sw-block-csi:local)
  support bundle: /tmp/d3bb/demo/kube-system-pods-deploys.txt      <- names reason + evidence ref

dashboard --from-bundle /tmp/d3bb --listen 0.0.0.0:18099,  GET /operator-snapshot.json:
  -> support_bundle_refs, safe_next_steps, observe.collect_bundle, kube-system-pods-deploys.txt

relocated bundle (cp -r /tmp/d3bb /tmp/d3replay) report:
  summary.txt still has support_bundle_ref=   (count 1)
```

`explain` names the same reason code and evidence ref; the dashboard
`/operator-snapshot.json` returns the same refs; the relocated-bundle report
keeps the support ref. PASS.

(Note: the `sw-block` container ships no `wget`/`curl`, so the dashboard endpoint
was queried from the host against the pod IP `10.42.0.183:18099`. Not a product
issue — just a test-harness detail.)

## G4 — Boundary — PASS

```text
ALLOWED:  patch swblockclusters --subresource=status: yes   create events: yes   get swblockclusters: yes
FORBIDDEN: patch swblockclusters (spec): no   create pods: no   patch deployments.apps: no
           create persistentvolumeclaims: no  create persistentvolumes: no
           create secrets: no                 create storageclasses.storage.k8s.io: no

command suggested, NOT executed:
  support-bundle dirs created in operator-status pod by the controller: 0
  PVC/PV created by operator-status: 0
  every reconcile: mutation_allowed=false
```

The `collect-helm-support-bundle.sh` command is published as a string in
`safeNextSteps[].command` and is **never run** by operator-status (no
support-bundle output, no extra pods, and the SA cannot exec/create pods
anyway). No PVC/PV/pod/deployment/secret/storageclass mutation. PASS.

## Non-Claims Verified

No automatic support-bundle upload, no automatic cleanup, no image
import/repair, no mutating operator action — the controller only patches CRD
`.status` and creates Events, and only *points* the user to the read-only
collection command.

## Lab State

Clean — `SwBlockVolume`/`SwBlockCluster` stubs + Events deleted, helm
uninstalled, both CRDs deleted; 0 sw-block pods, 0 CRDs, 0 iSCSI sessions.

## Bottom Line

- **D3 PASS, no findings.** A blocked volume's support evidence projects into
  `SwBlockCluster.status.supportBundleRefs[]` and a `observe.collect_bundle`
  `safeNextSteps[]` entry (`read_only`, `mutationAllowed=false`, command =
  `collect-helm-support-bundle.sh`); the report `summary.txt`/`index.html`,
  operator-snapshot, dashboard, and `explain` all agree; and the controller
  suggests the collection command without executing it or holding any
  storage/workload mutation power.
- **D3 can close.**
