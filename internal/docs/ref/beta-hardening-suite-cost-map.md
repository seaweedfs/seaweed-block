# Beta Hardening Suite Cost Map

Evidence source: first full green `beta-hardening-gate` run.

- Run id: `20260511-031605-8258`
- Product commit: `8822f20e91c2b88727ead9e49f9bf75eec28c791`
- Runner commit: `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`
- Suite wall clock: `1305.86s` (`21m46s`)
- Validator: `swblock validate-bundle --profile beta-hardening --expect-commit 8822f20`
  returned `VALID`

## Per-Child Cost

| Child | Wall Clock | Share | Class | Recommendation |
| --- | ---: | ---: | --- | --- |
| `iscsi-p8-compat-soak` | `659s` (`10m59s`) | `50.5%` | release/periodic integration | Keep as a release and periodic soak gate. Do not make it the normal developer loop. Add smaller component contracts only for concrete failures it discovers. |
| `nvme-p5-csi-protocol` | `273s` (`4m33s`) | `20.9%` | K8s integration plus componentizable protocol-shape checks | Keep one K8s end-to-end smoke. Move StorageClass protocol extraction, lifecycle protocol persistence, and launcher manifest argument rendering into component tests. |
| `csi-rf1-durable-restart` | `165s` (`2m45s`) | `12.6%` | restart integration plus componentizable state contracts | Keep one restart integration. Move durable-root selection, status/projection refresh, and reattach expectations into component contracts. |
| `nvme-p4-multipath-failover` | `138s` (`2m18s`) | `10.6%` | kernel integration | Keep. Linux NVMe multipath, ANA, namespace grouping, and mounted failover need the real kernel path. Keep field-level assertions to make failures local. |
| `iscsi-p6-alua-failover` | `36s` | `2.8%` | integration | Keep. It is cheap and covers real initiator ALUA behavior. |
| `iscsi-returned-replica` | `27s` | `2.1%` | integration | Keep. It is cheap and covers the authority/returned-replica behavior against a live target. |
| `cleanup-residue` | `3s` | `0.2%` | hygiene gate | Keep as a final suite child. It is cheap and prevents false confidence after PASS. |
| `csi-lifecycle-component` | `2s` | `0.2%` | component | Keep in the default loop. |
| `operations-status-diagnostics` | `2s` | `0.2%` | component | Keep in the default loop. Expand when operations contracts are added. |
| `returned-replica-component` | `1s` | `0.1%` | component | Keep in the default loop. |

## What This Means

The suite cost is dominated by three children:

1. `iscsi-p8-compat-soak` is about half the suite.
2. `nvme-p5-csi-protocol` is about one fifth.
3. `csi-rf1-durable-restart` is about one eighth.

The correct response is not to remove integration coverage. The release gate
exists because Linux initiators, K8s lifecycle, and real process restarts have
already exposed bugs that were not caught by the existing lower-level coverage.

The correct response is to stop using the full suite as the first debugging
tool. Fast component and contract tests should catch protocol-shape bugs before
the 20-minute suite is needed.

## First Componentization Target

Target: `nvme-p5-csi-protocol`.

Reason:

- It costs `273s`, roughly `21%` of the suite.
- Its past failures were mostly shape/provenance issues:
  - protocol parameter lost or stale in the StorageClass to CSI path,
  - lifecycle spec missing or not persisting `protocol`,
  - launcher rendering iSCSI args for an NVMe volume,
  - stale image/version evidence.
- These are deterministic contracts and do not require a full K8s data-path
  run to validate at development time.

Recommended lower-level contracts:

- CSI CreateVolume parameter extraction maps supported aliases to the same
  internal frontend protocol:
  - `protocol`,
  - `sw-block.seaweedfs.com/protocol`,
  - `frontendProtocol`.
- CSI CreateVolume rejects conflicting or invalid protocol parameters with a
  deterministic error before lifecycle state is written.
- Lifecycle volume spec persists explicit `protocol` for both `nvme` and
  default `iscsi`.
- Launcher manifest rendering emits:
  - NVMe: `--nvme-listen=`, `--nvme-subsysnqn=`, `--nvme-ns=1`, and no iSCSI
    args.
  - iSCSI: `--iscsi-listen=`, `--iscsi-iqn=`, and no NVMe args.
- Version/provenance checks reject stale images before a workload starts.

Keep the runner-native `nvme-p5-csi-protocol` child as the end-to-end proof
that the component contracts still compose inside k3s.

## Second Componentization Target

Target: `csi-rf1-durable-restart`.

Reason:

- It costs `165s`, roughly `13%` of the suite.
- It covers real restart semantics that must remain integrated, but several
  invariants can be made cheap:
  - durable root path and block file identity are preserved across restart,
  - blockvolume re-registers with the expected volume/replica identity,
  - master observes the restarted frontend status,
  - CSI can reattach only after status/projection is valid.

Keep one live restart scenario because process death, master observation, and
K8s attachment timing are integration concerns.

Initial runner-native fast gate:
`testops/scenarios/csi-rf1-durable-restart-component-gate.yaml`.

## Do Not Componentize These Away

- `nvme-p4-multipath-failover`: requires Linux NVMe/TCP multipath and ANA
  behavior. Component tests can cover Identify/ANA bytes, but the gate needs the
  kernel.
- `iscsi-p6-alua-failover`: requires initiator multipath behavior. It is also
  cheap enough to keep.
- `iscsi-p8-compat-soak`: should remain a release/periodic soak. Split only if
  a stable fast smoke can preserve the release signal and the long soak remains
  scheduled somewhere else.

## Next Plan Input

After beta repeatability is stamped, start with the `nvme-p5-csi-protocol`
component contract slice. The delivery should be:

- fast component tests for protocol extraction, persistence, and render shape,
- runner-native fast gate:
  `testops/scenarios/nvme-p5-protocol-component-gate.yaml`,
- the existing runner-native P5 child still passing,
- a short doc note explaining which failures now fail in seconds and which still
  intentionally require K8s.
