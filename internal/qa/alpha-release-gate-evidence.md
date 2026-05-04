# Alpha Release Gate Evidence — 2026-05-04

Status: gates 2-5 PASS, gate 1 BLOCKED on GHCR package visibility
Owner: QA
Lab host: m02 (192.168.1.184) — single-node k3s v1.34.4
Reviewer: maintainer / dev lead
Assignment: [internal/qa/alpha-release-assignment.md](alpha-release-assignment.md)

## Summary

| Gate | Result | Tree | Wall clock | Run ID |
|---|---|---|---|---|
| 1 — GHCR Fresh-User Demo | ❌ BLOCKED | `k8s-alpha/published-images@0f77efb` | n/a | `20260504T180543Z-gate1` |
| 2 — Install Then Use Own App | ✅ PASS | `k8s-alpha/published-images@0f77efb` | ~3 min | `20260504T180903Z-gate2-install` + `…181106Z-gate2-uninstall` |
| 3 — Larger Volume Smoke | ✅ PASS | `k8s-alpha/published-images@0f77efb` | ~25 s | `20260504T181131Z-gate3` |
| 4 — FIO Stability Smoke | ✅ PASS | `k8s-alpha/fio-smoke@2e4937b` | ~80 s | `20260504T181229Z-gate4` |
| 5 — pgbench Smoke | ✅ PASS | `k8s-alpha/pgbench-smoke@a93b596` | ~3 min | `20260504T182028Z-gate5` |

All artifact directories live under `V:\share\qa-alpha-gates\<RUN_ID>\` (Windows) =
`/mnt/smb/work/share/qa-alpha-gates/<RUN_ID>/` (m02 SMB mount).

## Gate 1 — GHCR Fresh-User Demo  ❌ BLOCKED

```
Tree:        k8s-alpha/published-images@0f77efb
Run ID:      20260504T180543Z-gate1
Command:     bash scripts/run-k8s-demo-ghcr.sh "$PWD"
Result:      blockmaster Deployment never reached Available
Artifacts:   /mnt/smb/work/share/qa-alpha-gates/20260504T180543Z-gate1/
```

Pre-cleanup excerpt from `blockmaster.log`:

```
Error from server (BadRequest): container "blockmaster" in pod
"sw-blockmaster-796886f766-szx6s" is waiting to start: trying and failing
to pull image
```

Direct verification of root cause from m02:

```
$ sudo docker pull ghcr.io/seaweedfs/seaweed-block:alpha
Error response from daemon: Head "https://ghcr.io/v2/seaweedfs/seaweed-block/manifests/alpha": denied

$ sudo docker pull ghcr.io/seaweedfs/seaweed-block-csi:alpha
Error response from daemon: Head "https://ghcr.io/v2/seaweedfs/seaweed-block-csi/manifests/alpha": denied
```

**Root cause: the GHCR packages `ghcr.io/seaweedfs/seaweed-block` and
`ghcr.io/seaweedfs/seaweed-block-csi` are still default-private; anonymous
pulls are denied.** The `1c5a1e8 ci(k8s): publish alpha images to GHCR`
commit publishes the packages but new GHCR packages default to private
visibility — they must be flipped to public via the package's GitHub
Settings page.

This is a release-process blocker, not a code issue. Once package
visibility is flipped, re-run is a simple repeat of the existing
`scripts/run-k8s-demo-ghcr.sh "$PWD"` with no other changes.

Manifest verification (the `block-stack.rendered.yaml` in the artifact dir)
confirms the script correctly substituted GHCR URLs and set
`imagePullPolicy: IfNotPresent`. The harness wiring is correct.

## Gate 2 — Install Then Use Own App  ✅ PASS

```
Tree:                k8s-alpha/published-images@0f77efb
Run IDs:             install: 20260504T180903Z-gate2-install
                     uninstall: 20260504T181106Z-gate2-uninstall
Image override:      SW_BLOCK_IMAGE=sw-block:local SW_BLOCK_CSI_IMAGE=sw-block-csi:local
                     (legitimate fallback while Gate 1's GHCR visibility
                     blocker is resolved; install script supports the
                     override via env)
```

Sequence and outcomes:

| Step | Command | Result |
|---|---|---|
| 1 install stack | `bash scripts/install-k8s-alpha.sh "$PWD"` | `[alpha-install] PASS: seaweed-block alpha stack installed` |
| 2 apply PVC | `kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml` | StorageClass + PVC created |
| 3 apply blockvolumes | `bash scripts/apply-k8s-alpha-blockvolumes.sh` | `[alpha-volumes] PASS: generated blockvolume workloads are available` |
| 4 writer pod | `kubectl apply -f examples/kubernetes/basic-app/writer-pod.yaml` + wait Succeeded + read log | pod log: `4096 bytes (4.0KB) copied`, `/data/demo.bin: OK` |
| 5 reader pod (replacement) | delete writer + apply reader-pod.yaml + wait Succeeded + read log | pod log: `[reader] reading existing data from PVC mounted at /data`, `/data/demo.bin: OK` |
| 6 cleanup | `kubectl delete pod sw-block-example-reader && kubectl delete pvc sw-block-example-pvc` | clean |
| 7 uninstall | `bash scripts/uninstall-k8s-alpha.sh "$PWD"` | `[alpha-uninstall] PASS: seaweed-block alpha stack uninstall requested` |

Post-cleanup verification:
- `iscsiadm -m session` → `iscsiadm: No active sessions.`
- `kubectl get all -A | grep sw-block` → empty

The reader pod was created **after** the writer pod was deleted and read
the same `/data/demo.bin` file from the same PVC — proving data persistence
across pod replacement (within a single PVC lifetime; not across
blockvolume restart per the alpha non-claim).

## Gate 3 — Larger Volume Smoke  ✅ PASS

```
Tree:        k8s-alpha/published-images@0f77efb
Run ID:      20260504T181131Z-gate3
Command:     bash scripts/run-k8s-alpha-large.sh "$PWD"
PVC size:    256 MiB
Workload:    dd if=/dev/urandom of=/data/payload.bin bs=1M count=32 + sha256 verify
Result:      [alpha-large] PASS: dynamic PVC create/delete completed checksum
             write/read and cleanup
```

`pod.log` evidence:
```
32+0 records in
32+0 records out
33554432 bytes (32.0MB) copied, 0.075s, ~426 MB/s
/data/payload.bin: OK
```

`blockvolume-generated.log` evidence (multi-PDU iSCSI WRITE handling
exercised, NOT the historical tiny-write path):
- max SCSI WRITE `transferLen` observed: **1024 blocks (4 MiB)** per PDU
- session errors: **0**

Post-cleanup: 0 active iSCSI sessions, 0 K8s residue.

## Gate 4 — FIO Stability Smoke  ✅ PASS

```
Tree:        k8s-alpha/fio-smoke@2e4937b
Run ID:      20260504T181229Z-gate4
Command:     bash scripts/run-k8s-alpha-fio.sh "$PWD"
fio params:  bs=128k, rw=randrw, runtime=60s, iodepth=1
PVC size:    256 MiB
```

fio totals from `pod.log`:
```
READ : bw=13.0MiB/s, total 780 MiB read,  6,250 ops, run=60005ms
WRITE: bw=13.1MiB/s, total 785 MiB written, 6,279 ops, run=60005ms
```

V3 server-side stability evidence from `blockvolume-generated.log`:
- max WRITE `transferLen` observed: 32 blocks (128 KiB) — matches fio bs=128k
- session errors: 0
- ~1.6 GB total I/O delivered with zero failed I/O reports

Post-cleanup: 0 active iSCSI sessions.

**Non-claim**: this is stability evidence, not a performance SLO. Throughput
numbers reflect single-thread depth-1 fio against a single-node k3s lab
with default networking; not a benchmark.

## Gate 5 — PostgreSQL / pgbench Smoke  ✅ PASS

```
Tree:        k8s-alpha/pgbench-smoke@a93b596
Run ID:      20260504T182028Z-gate5
Command:     bash scripts/run-k8s-alpha-pgbench.sh "$PWD"
Backend:     walstore
Result:      [alpha-pgbench] PASS: dynamic PVC create/delete completed
             checksum write/read and cleanup
```

pgbench summary from `pod.log`:
```
transaction type:                            TPC-B (sort of)
scaling factor:                              1
number of clients:                           4
number of threads:                           2
duration:                                    60 s
number of transactions actually processed:   22,221
number of failed transactions:               0 (0.000%)
tps:                                         370.33 (without initial conn)
```

**Non-claim**: production database use is not claimed. This is alpha
DB-style smoke evidence — PostgreSQL initializes on the alpha PVC, runs
pgbench TPC-B without failures, cleanup leaves no residue.

Post-cleanup: 0 active iSCSI sessions, 0 K8s residue.

## Gate 5 sub-finding (worth flagging)

The first attempt at Gate 5 hit `blockmaster: flag provided but not defined:
-launcher-durable-impl` because the `sw-block:local` image cached on m02 was
built from `k8s-alpha/published-images` (no `--launcher-durable-impl` flag
yet) but `k8s-alpha/pgbench-smoke` adds that flag in `block-stack.yaml`.
Rebuilding `sw-block:local` from the `pgbench-smoke` source unblocked the
run. Worth noting that the alpha smoke harnesses assume image and manifest
come from the same tree; if a reviewer runs gates back-to-back across
branches, they must rebuild between branch switches. Captured in the run
artifact `apply-block-stack.log`.

## Artifact Requirements (per assignment)

Each gate's artifact directory contains:

```
run.log                          command transcript
pod.log                          smoke pod stdout (writer/reader/fio/pgbench)
pod.describe.txt                 pod state on failure (Gate 1 only had the failure path)
blockmaster.log                  master daemon log
blockvolume-generated.log        launcher-generated blockvolume daemon log
blockcsi-controller.log          CSI controller log
csi-provisioner.log              external-provisioner sidecar log
csi-attacher.log                 external-attacher sidecar log
apply-*.log                      kubectl apply transcripts
app-storage.txt /
app-storage.after-delete.txt     pv/pvc/pod state before + after cleanup
kube-system-pods-deploys.txt /
…after-delete.txt                cluster-system pods/deploys before + after cleanup
block-stack.rendered.yaml        the rendered manifest (image substitution evidence)
csi-controller.rendered.yaml
csi-node.rendered.yaml
cleanup.log                      teardown transcript
poll.log                         polling stderr (silenced from main output)
```

Gate 1's artifact dir contains the apply logs and the `blockmaster.log`
recording the GHCR pull failure; pod.describe.txt was lost to the cleanup
trap before the diagnostic could land — the failure mode is captured by
the direct `docker pull` evidence above instead.

## Non-Claims (carried from assignment)

- not production-ready
- not multi-node validated as a Kubernetes product
- not durable across blockvolume pod restart while the alpha manifest still
  uses pod-local `emptyDir` state
- not yet a full operator (current Gate 2 manual `apply-k8s-alpha-blockvolumes.sh`
  step is operator debt, called out as such in the assignment)
- no failover-under-mounted-PVC claim
- no NVMe-oF CSI claim
- no performance/soak claim — Gate 4 fio and Gate 5 pgbench numbers are
  stability evidence only
- Gate 1 is BLOCKED, not PASS — release gate is not satisfied until the
  GHCR packages flip to public visibility

## Reviewer Checklist

- [x] GHCR fresh-user path uses GHCR images **(blocked — visibility flip needed before re-run)**
- [x] install-own-app path uses the install/apply/app sequence (Gate 2 used the documented 7 steps, not `run-k8s-demo.sh`)
- [x] larger-volume path covers multi-PDU iSCSI writes (Gate 3 server log shows transferLen=1024 blocks vs historical max=36)
- [x] fio is reported as stability, not benchmark (Gate 4 explicit non-claim)
- [x] pgbench is reported as alpha DB smoke, not production DB support (Gate 5 explicit non-claim)
- [x] cleanup evidence is present for every gate (iscsiadm + kubectl get all -A both verified per gate)

## Reviewer Decision

Per the assignment "Reviewer Decision" criteria:

1. ✅ four of five required gates PASS; gate 1 is documented as BLOCKED with explicit release-blocker root cause (GHCR package visibility), keeping the PR open
2. ✅ evidence reproducible from artifact directory + commit SHA per gate
3. ✅ cleanup proven for every PASS gate
4. ✅ claims stay within alpha scope

Recommend: merge this evidence document; flip GHCR package visibility to
public; re-run Gate 1 only (single command, no rebuild) and append the
result here as a follow-up commit on the same PR before declaring the
alpha release gate satisfied.
