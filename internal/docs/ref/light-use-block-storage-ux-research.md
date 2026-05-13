# Light-Use Block Storage UX Research

Research date: 2026-05-12.

Scope: compare current install, first-volume, operations, and teardown patterns
from Kubernetes block-storage systems. This is not a feature benchmark. It is a
UX input for Seaweed Block's light-use install/lifecycle operations plan.

Sources used are official project/vendor docs:

- Longhorn install and troubleshooting docs.
- OpenEBS installation docs.
- Rook/Ceph quickstart and block storage docs.
- Piraeus Datastore getting-started and Helm docs.
- Amazon EKS EBS CSI docs as a managed-service reference point.

## Common Shape

The simple successful products all present a short operational ladder:

```text
preflight/prereqs -> install -> wait/verify components -> create StorageClass ->
create PVC + app -> verify bound/running/I/O -> inspect status -> teardown ->
collect support data on failure
```

Seaweed Block has most of the pieces, but they are scattered across scripts,
TestOps scenarios, QA reports, and internal plans. The current plan should make
the ladder explicit and executable.

## Comparison

| System | Light-user install shape | First-volume shape | Operations/status shape | Teardown/failure posture | Lesson for Seaweed Block |
|---|---|---|---|---|---|
| Longhorn | Multiple install paths: Rancher, `kubectl`, Helm, GitOps. Docs emphasize prerequisites such as `open-iscsi` and root/privileged requirements. `longhornctl` can check/install prerequisites. | Kubernetes CSI path with StorageClass/PVC. | Strong UX: UI, API/CLI, support bundle, settings, version/upgrade signals. | Support bundle is a first-class troubleshooting artifact. | Keep `sw-block ops status` bundle central, but add preflight and make prerequisites explicit before install. |
| OpenEBS | Helm-first. Default install brings multiple engines; docs also show disabling replicated storage for a simpler local path. | StorageClass-driven PVC flow, engine chosen by install/StorageClass. | Verification starts with `kubectl get pods -n openebs`; operational UX is Kubernetes-native plus engine docs. | Good install verification, but engine selection can confuse light users. | Pick one default engine/protocol for the first run. Do not present every option up front. |
| Rook/Ceph | Quickstart is operator + cluster manifests or Helm. Docs warn to test in a VM and require raw devices/partitions/LVs or equivalent. | After cluster readiness, create block StorageClass and run sample apps. | Strong expert operations: Ceph dashboard, toolbox pod, kubectl plugin, Prometheus metrics. | Teardown is explicit; docs call out reclaim-policy consequences such as retained RBD images. | Be explicit about destructive/resource assumptions and cleanup ownership. Do not hide retained state or manual cleanup. |
| Piraeus/LINSTOR | Operator install by one manifest or Helm. Then create `LinstorCluster`, storage pool config, StorageClass, PVC, consumer pod. | Tutorial uses `WaitForFirstConsumer`, then proves mount with `df` and LINSTOR resource listing. | Status is explicit via `linstor node list`, storage-pool list, and resource list-volumes from the controller pod. | The tutorial makes internal state visible after each stage. | Show both Kubernetes state and Seaweed Block state after each major step. |
| AWS EBS CSI / EKS Auto Mode | Managed add-on or Auto Mode reduces install burden. StorageClass is still required for Auto Mode; IAM is the major prerequisite. | StorageClass + PVC + sample app. | Managed service absorbs many operations; docs surface IAM/provisioning errors such as unauthorized volume creation. | Vendor-managed lifecycle, but docs are explicit about unsupported node types and driver separation. | We cannot match managed UX yet, so the alpha path needs precise preflight and failure explanation. |

## Patterns Worth Copying

### 1. Preflight Before Install

Longhorn makes prerequisites concrete: host packages, kernel/mount behavior,
privileged/root needs, and a CLI preflight path. Our current scripts discover
many failures late.

Seaweed Block action:

- Add a quick preflight section to the first-volume runbook.
- Gate at least these: `kubectl`, kubeconfig access, k3s/container runtime
  expectations, `iscsiadm`/iscsid for the iSCSI path, image import/pull path,
  and writable artifact directory.

### 2. One Default Path First

OpenEBS and Rook support multiple engines/protocols, but the quick path chooses
a concrete install flow. Longhorn offers multiple install mechanisms, but the
docs still funnel users through prerequisite and readiness checks.

Seaweed Block action:

- Default first-volume path should be iSCSI + `walstore` + single-node k3s.
- NVMe, mounted failover, RF2/RF3, and performance claims stay behind separate
  gates.

### 3. Verify At Every Boundary

Piraeus is a good model here: after operator install, it waits for pods; after
cluster creation, it lists LINSTOR nodes; after storage-pool config, it lists
storage pools; after PVC/Pod, it shows the mounted filesystem and resource
state.

Seaweed Block action:

- The runbook should not just say "run script and see PASS".
- It should show Kubernetes pods ready, StorageClass/PVC bound, generated
  blockvolume present, app write/read proof, `sw-block ops status` summary, and
  cleanup checks.

### 4. Make Support Bundle A First-Class Failure Step

Longhorn's troubleshooting flow centers on support bundles. We already built a
one-volume bundle; the next plan should wire it into the first-volume scenario.

Seaweed Block action:

- On failure after volume identity exists, capture the ops bundle.
- If no volume identity exists, emit a clear marker:

```text
ops-status-unavailable: no volume id reached
```

### 5. Be Honest About Cleanup And Retained State

Rook docs explicitly warn about retained RBD images under `Retain` reclaim
policy. This is the right posture: tell the user what will and will not be
deleted.

Seaweed Block action:

- The first-volume artifact should separate product/Kubernetes owner-reference
  cleanup, host session cleanup, TestOps guardrail cleanup, and known
  non-claims.

### 6. Avoid UI Before The CLI Contract Is Stable

Longhorn and Ceph have strong UI/dashboard stories, but they sit on top of
clear APIs/status models. For Seaweed Block, a UI now would likely hide product
gaps.

Seaweed Block action:

- Keep this plan CLI/runbook/TestOps focused.
- Move UI/dashboard to a later observation plan after cluster-wide list/status
  exists.

## What Good Looks Like For This Plan

The close evidence should look more like a product quickstart than a test log:

```text
1. preflight ok
2. install/launch ok
3. storage class installed
4. pvc bound
5. blockvolume generated and ready
6. writer pod checksum ok
7. reader/replacement pod checksum ok
8. delete ok
9. cleanup attribution ok
10. support bundle path recorded, or not-needed because success
```

If a step fails, the bundle should answer where:

```text
phase=install | pvc | blockvolume | attach | app_io | cleanup
status=failed
ops_status=collected|unavailable
next_artifact=<path>
```

## Recommended Product Positioning

For the next close, the claim should be:

```text
Seaweed Block has a single-node Kubernetes alpha path where a user can create
and use one block-backed PVC with a documented install/run/delete flow and
self-describing troubleshooting artifacts.
```

The non-claim should stay explicit:

```text
This is not a production HA, multi-node, upgrade, performance, or operator-grade
storage claim.
```

## Source Links

- Longhorn install: https://longhorn.io/docs/latest/deploy/install/
- Longhorn troubleshooting/support bundle: https://longhorn.io/docs/latest/troubleshoot/troubleshooting/
- OpenEBS install: https://openebs.io/docs/main/quickstart-guide/installation
- Rook quickstart: https://rook.io/docs/rook/latest/Getting-Started/quickstart/
- Rook block storage: https://www.rook.io/docs/rook/v1.9/Storage-Configuration/Block-Storage-RBD/block-storage/
- Piraeus getting started: https://piraeus.io/docs/v2/tutorial/get-started/
- Piraeus Helm install: https://piraeus.io/docs/stable/how-to/helm/
- AWS EBS CSI for EKS: https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html
- AWS EKS Auto Mode StorageClass: https://docs.aws.amazon.com/eks/latest/userguide/create-storage-class.html
