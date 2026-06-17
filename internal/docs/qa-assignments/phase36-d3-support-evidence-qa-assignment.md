# Phase 36 D3 QA Assignment - Support Evidence Pointers

Status: ready for QA.

Source scope:

- `SwBlockCluster.status.supportBundleRefs[]`
- `SwBlockCluster.status.safeNextSteps[]`
- `operator-snapshot.json.cluster.support_bundle_refs[]`
- `operator-snapshot.json.cluster.safe_next_steps[]`
- report `summary.txt`
- report `index.html`
- from-bundle replay

## Goal

Verify that blocked or unknown operational status points users to support
evidence and safe collection/replay commands without mutating the cluster.

## Required Checks

### G1: Live Blocked Or Support-Bundle Path

Use an existing blocked path such as CSI image pull failure, or another
controlled support-bundle scenario.

Expected:

```text
SwBlockCluster.status.supportBundleRefs has at least one support/evidence ref
SwBlockCluster.status.safeNextSteps has type=observe.collect_bundle
safeNextSteps[0].mode=read_only
safeNextSteps[0].mutationAllowed=false
safeNextSteps[0].command mentions collect-helm-support-bundle.sh
```

### G2: Report Agreement

Run `sw-block ops report` on the same evidence.

Expected:

```text
summary.txt includes support_bundle_ref=...
summary.txt includes safe_next_step=observe.collect_bundle mode=read_only
index.html includes Support Evidence
index.html includes Safe Next Steps
operator-snapshot.json cluster.support_bundle_refs agrees with CRD status
operator-snapshot.json cluster.safe_next_steps agrees with CRD status
```

### G3: From-Bundle Replay

Replay the generated bundle away from the live cluster:

```text
sw-block ops report --from-bundle <bundle> --out <out>
sw-block ops explain volume --from-bundle <bundle> <volume>
sw-block ops dashboard --from-bundle <bundle> --listen 127.0.0.1:<port>
```

Expected:

```text
from-bundle summary keeps support_bundle_ref
from-bundle operator-snapshot keeps support_bundle_refs
from-bundle dashboard /operator-snapshot.json returns the same refs
ops explain names the same reason code and evidence refs
```

### G4: Boundary

Verify no new mutation power:

```text
operator-status may patch CRD status and create Events only
support collection command is only suggested, not executed by operator-status
no PVC/PV/pod/deployment/secret/storageclass mutation
```

## Pass Criteria

```text
G1 PASS
G2 PASS
G3 PASS
G4 PASS
cleanup residue clean
```

## Non-Claims

- No automatic support-bundle upload.
- No automatic cleanup.
- No automatic image import or repair.
- No mutating operator action.
