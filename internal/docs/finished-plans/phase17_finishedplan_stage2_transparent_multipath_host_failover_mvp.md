# Current Plan: Stage 2 Transparent Multipath Host Failover MVP

Status: active, opened after closing
`finished-plans/phase16_finishedplan_stage1_mounted_recovery_ack_profile_mvp.md`,
100% complete. D1 cold product spec and D2 strict QA hard gate are closed. D3
has its first TDD slice: CSI can now deliberately request iSCSI multipath,
fail closed with fewer than two portals, log in all provided portals, and mount
the dm-multipath device instead of a raw portal-specific path. Master status can
now expose all observed replica frontend facts for a volume, while CSI only
uses them as multipath under an explicit Stage 2 opt-in. The alpha install path
now has a Stage 2 multipath opt-in flag and the CSI image/manifests carry the
host multipath prerequisites needed for the first runner scenario. The first
runner-native Stage 2 baseline scenario now exists; it is a wiring/prereq gate
and explicitly does not claim transparent failover yet. QA found that NodeStage
could still accept an early one-portal publish context under Stage 2 opt-in; the
node path now waits for refreshed multi-portal publish evidence before staging.
Internal review then found the scenario was collecting host path evidence after
CSI could already unstage; the baseline now stops with a writer pod still
mounted and captures `iscsiadm`, `multipath -ll`, and `sg_rtpg` inside that live
mount window. QA then confirmed multi-portal NodeStage but found the first
non-root writer-hold pod could not write `/data/demo.bin`; the alpha CSIDriver
now explicitly sets `fsGroupPolicy: File` so kubelet applies pod `fsGroup`
ownership to filesystem PVC mounts, and the writer-mounted stop path preserves
host-path diagnostics even when the writer check fails.
QA rerun then produced the first full Stage 2 baseline host evidence: three
sessions to one IQN, one dm-multipath device with three ALUA paths, and `sg_rtpg`
states `0x00/0x02/0x02`. The remaining failure was only a scenario grep pinned
to the wait-path log line; assertions now use the always-emitted staged summary
line.
The Stage 2 baseline gate passed in QA run `20260515-092817-61f9`. D4 now has
its first mounted-failover scenario draft:
`stage2-iscsi-alua-multipath-failover-chain.yaml`; it keeps the writer pod
mounted, stops the live primary derived from inventory, and verifies data from
the same pod without reader recreate. QA's first D4 run
`20260515-094955-bed0` reached the mounted writer setup and host multipath
evidence, then stopped before failure injection because `sw-block ops
inventory` rejected the new `stage2-iscsi-alua-multipath` claim profile. That
profile is now a first-class CLI/product claim label and remains valid only
under sync ACK profiles, not `best-effort`. QA's second D4 run
`20260515-103621-5d61` then proved the core Stage 2 claim for the first time:
the same writer pod UID stayed mounted, r1 was stopped, r2 became the only
primary, ALUA state moved r2 to active/optimized, the original pod verified the
pre-failure data and wrote new data, and the bundle recorded
`transparent_failover_claimed=true`. One cleanup assert still saw terminating
`blockmaster`/`blockcsi` k3s pod processes after uninstall; the uninstall script
now waits for CSI and blockmaster deletion, and the scenario cleanup timeout was
raised to cover that stricter wait.
QA's third D4 run `20260515-104802-a618` passed strictly: 48/48 actions, 9/9
phases, reproduced the no-pod-recreate mounted failover, and left no iSCSI
sessions, sw-block processes, port-forwards, blockvolume Deployments, or
run-scoped host paths. `docs/operations-v1.md` now has an explicit Stage 2
iSCSI ALUA/dm-multipath section, so the plan is ready for formal QA close
against the hard gate. QA then issued the formal close report at
`qa-assignments/stage2-transparent-multipath-host-failover-close-report.md`:
PASS (strict), HG-0 through HG-13 all PASS, no blocking findings.

QA needed now: no for this plan. Stage 2 is clear to move to
`finished-plans/`.

## Product Question

Can a Kubernetes user keep a mounted block volume usable through primary
failure without pod recreate, under a documented multipath protocol path, while
the product proves:

```text
master promotion
-> stale primary fenced
-> protocol path state changes
-> Linux host path switches to the promoted path
-> mounted workload verifies data
```

Stage 1 already proved safe recovery through CSI/node re-stage on pod recreate.
Stage 2 must not relabel that as transparent failover. Stage 2 only closes when
the original mounted host path continues or recovers through multipath behavior
under the specific protocol being claimed.

## Product Position

This plan serves `ref/product-positioning-v1.md`: lightweight Kubernetes block
storage with enterprise protocol discipline and fail-closed recovery semantics.

The product value is not "we have ALUA/ANA code" or "master changed primary."
The value is:

```text
an operator can run a documented Kubernetes path, inject primary failure, and
see the mounted workload recover through standard Linux multipath semantics,
with support-bundle evidence explaining authority, path state, and data check.
```

## Scope Decision

Stage 2 starts with one protocol path, not both at once.

Default candidate: **iSCSI ALUA + dm-multipath**.

Reason:

- current alpha Kubernetes frontend default is iSCSI,
- `docs/operations-v1.md` and quickstart paths already assume iSCSI,
- existing P6 references show ALUA/MPIO protocol substrate and mounted
  failover evidence outside the Kubernetes product path,
- it lets the user-facing path mature before adding protocol selection.

NVMe ANA remains the second path. It has strong lab substrate evidence in
`ref/nvme-p4-multipath-failover-design.md`, but Kubernetes CSI protocol
selection and NVMe attach UX should not be mixed into the first Stage 2 close.

## Non-Negotiable Semantics

- Authority decides primary; CSI and protocol code do not promote.
- Protocol state reflects authority/frontier facts; it does not invent safety.
- Old primary must not acknowledge successful data I/O after authority moves.
- Multipath identity must make all paths for one volume appear as one logical
  device to the Linux host.
- The mounted workload must verify pre-failure data after failover.
- If multipath is not actually configured or does not switch, the product must
  fail closed and name the missing boundary.
- Kubernetes attach, mount, path switch, and post-failure I/O waits must be
  bounded and observable. A stuck PVC/pod/session/multipath map is a product
  failure, not a test timeout detail.
- Every bounded failure must emit a user-readable blocker reason and support
  bundle pointer before cleanup.
- A green Stage 1 CSI/pod-recreate recovery is not sufficient for Stage 2.

## Allowed Simplifications

- `alpha_non_claim`: first Stage 2 close may use same physical node with
  multiple logical Seaweed Block server identities if the host sees multiple
  protocol paths and Linux multipath owns the device.
- `alpha_non_claim`: no node-loss claim unless a later gate runs on distinct
  Kubernetes nodes with non-loopback frontends.
- `temporary_internal`: scenario may use TestOps to inject primary failure and
  capture support bundles.
- `safe_refusal`: if multipath prerequisites are absent, the command/scenario
  must fail with a clear issue class instead of falling back to pod recreate.

## Explicit Non-Claims

- No node-loss survival.
- No broad multi-distro multipath compatibility.
- No performance/RTO/SLO claim.
- No NVMe ANA Kubernetes claim in the first iSCSI Stage 2 close.
- No Windows MPIO claim.
- No online upgrade/uninstall safety claim.
- No automatic repair/rebuild/failback claim.

## D1: Cold Product Spec And Gap Audit

Produce `internal/docs/ref/stage2-transparent-multipath-host-failover-spec.md`.

It must answer:

- Which protocol closes first: iSCSI ALUA or NVMe ANA?
- What is the exact user-visible command/runbook path?
- What does CSI publish: one path, multiple paths, or a multipath staging
  helper?
- What identity makes the host treat replicas as paths of one volume?
- What evidence proves stale primary fencing?
- What evidence proves Linux multipath switched paths?
- What support-bundle fields make this understandable without internal logs?

Inputs to read:

- `ref/iscsi-alua-technical-note.md`
- `ref/iscsi-p6-alua-mpio-design.md`
- `ref/nvme-ana-technical-note.md`
- `ref/nvme-p4-multipath-failover-design.md`
- `ref/engine-automata-design-note.md`
- `docs/operations-v1.md`
- CSI node/controller code for publish/stage behavior

Exit criteria:

- one protocol selected for first close,
- all non-negotiable semantics pinned,
- explicit list of code gaps,
- no claim drift from Stage 1.

Status: drafted. First protocol is iSCSI ALUA + Linux dm-multipath. Current
known code gaps are single-target CSI publish/stage, raw path mounting instead
of multipath device mounting, ALUA product-fact wiring, and missing host-path
inventory evidence.

## D2: QA Hard Gate Draft

Create
`internal/docs/qa-assignments/stage2-transparent-multipath-host-failover-close-hard-gate.md`.

The hard gate must include binary clauses for:

- documented runbook entry,
- multipath prerequisites detected before test,
- CSI/node path setup does not use pod recreate as the recovery mechanism,
- Linux sees one multipath device with multiple paths,
- pre-failure mounted writer checksum,
- primary failure is scoped and derived from live inventory,
- master publishes exactly one new primary,
- old primary path cannot return successful stale data I/O,
- protocol path state changes are visible (`sg_rtpg`/multipath for iSCSI or
  ANA/native multipath evidence for NVMe),
- mounted workload verifies data after failover,
- support bundle explains authority, path state, host path, and data check,
- cleanup leaves no sessions/devices/processes/port-forwards/run-scoped paths,
- non-claims remain explicit.

The gate must fail if the proof is only:

```text
pod recreated -> CSI restaged -> reader checksum passed
```

That is Stage 1, not Stage 2.

Status: drafted at
`qa-assignments/stage2-transparent-multipath-host-failover-close-hard-gate.md`
with HG-0 through HG-13.

## D3: Minimal Implementation Slice

Implementation starts only after D1/D2 are reviewed.

Expected iSCSI-first code areas:

- CSI node staging: publish/configure multiple target portals for one volume.
- iSCSI utility layer: discover sessions/devices by IQN plus portal and
  multipath identity.
- Frontend state provider: map current authority/frontend facts to ALUA state.
- Operations inventory/status: expose multipath readiness, active path,
  standby/unavailable path, and host-path evidence.
- Reliability/observability: add bounded waits and explicit blocker issue
  classes for attach, mount, multipath map creation, ALUA/RTPG state read,
  authority movement, stale-primary fencing, and post-failure I/O.
- TestOps scenario: run mounted writer, inject primary failure, verify mounted
  data path without pod recreate.

No broad refactor. Any engine/protocol changes must preserve the automata
boundaries from `ref/engine-automata-design-note.md`.

Status:

- TDD slice complete in `core/csi`: `stage2_multipath=true` plus multiple
  `iscsiAddrs` forces NodeStage to log in every portal and mount the multipath
  device returned by the iSCSI utility.
- Safe refusal added: `stage2_multipath=true` with fewer than two portals fails
  before discovery/login.
- Controller/master lookup seam added: if master status exposes multiple iSCSI
  frontend facts with the same IQN and `blockcsi` is started with the explicit
  Stage 2 multipath lookup path, CSI publish context carries `iscsiAddrs=<a,b>`
  plus `stage2_multipath=true`. Different IQNs are not merged into one
  multipath target.
- Master status fan-out added: `QueryVolumeStatus` can return frontend facts
  for all observed replica slots of the volume instead of only the currently
  assigned replica. This is read-only evidence; it does not let CSI choose or
  promote authority.
- Existing Stage 1 behavior remains gated off from multipath by default:
  `NewControlStatusLookup` and normal `blockcsi` startup preserve single-target
  publish behavior unless `--stage2-multipath` is used.
- Alpha deployment opt-in added: `SW_BLOCK_STAGE2_MULTIPATH=1` renders
  `--stage2-multipath` into both CSI controller and CSI node. Default alpha
  installs remain unchanged.
- CSI node host prerequisites added: image includes `multipath-tools` and
  `sg3-utils`; node DaemonSet loads `dm_multipath` and `scsi_dh_alua` and
  mounts `/run/udev` read-only for host device observation.
- Linux utility hardened: multipath map discovery refreshes maps with bounded
  5s subprocess calls and resolves IQN -> raw devices via `/dev/disk/by-path`
  before matching them to a `/dev/mapper/<map>` entry in `multipath -ll`.
- Runner baseline added:
  `testops/scenarios/stage2-iscsi-alua-multipath-baseline-chain.yaml`.
  It is deliberately named as a baseline, not the eventual D4 close path, to
  avoid claim drift. It enables
  `SW_BLOCK_STAGE2_MULTIPATH=1`, runs RF=3 `sync-quorum`, proves the rendered
  CSI manifests include `--stage2-multipath`, checks r1/r2/r3 generated
  blockvolume rows share one iSCSI IQN, requires CSI node logs to show
  `multipath=true`, captures `multipath -ll` and `sg_rtpg`, asserts required
  host modules are loaded, flushes Seaweed Block multipath maps during cleanup,
  and writes `stage2-claim-boundary.txt` with
  `transparent_failover_claimed=false`.
- QA baseline blocker addressed: the first QA runs showed
  `multipath=true` in the node log but only `portal=127.0.0.1:3260`, so no
  dm-multipath map or `sg_rtpg` evidence appeared. `NodeStageVolume` now treats
  Stage 2 multipath as a required multi-portal boundary: it waits up to a
  bounded publish-target window for refreshed master status to return at least
  two iSCSI portals, logs `multipath publish target ready portals=...`, and
  only then logs into all paths and mounts the mapper device. If the boundary
  never appears, it falls back to the existing fail-closed one-portal rejection.
- Internal review/QA timing blocker addressed: the baseline no longer tries to
  prove host multipath after the short-lived reader path may have unstaged the
  session. It renders the RF3 writer-hold manifest, stops at
  `SW_BLOCK_DEMO_STOP_AFTER=writer-verified`, captures
  `iscsi-sessions.writer-mounted.txt`, `multipath.writer-mounted.txt`, and
  `sg-rtpg.writer-mounted.txt` while the PVC is still mounted, and writes
  `controlled-stop-writer-verified.txt` with the claim boundary. Scenario
  assertions now grep that live-mount evidence.
- QA non-root writer blocker addressed: Stage 2's writer-hold pod runs as
  UID/GID 1000 with `fsGroup: 1000`. The alpha `CSIDriver` now declares
  `fsGroupPolicy: File` so Kubernetes applies filesystem ownership for mounted
  PVCs instead of leaving the ext4 root `root:root 0755`. If the writer still
  fails, the demo captures mounted host-path evidence before cleanup and records
  `phase=writer-verified-failed` for diagnosis.
- QA baseline evidence from run `20260515-091810-6d74`: writer succeeded,
  `iscsi-sessions.writer-mounted.txt` showed three sessions to the same IQN,
  `multipath.writer-mounted.txt` showed one dm-multipath map with ALUA handler
  and three paths, and `sg-rtpg.writer-mounted.txt` showed one
  active/optimized path plus two standby paths. Scenario grep has been corrected
  to accept the fast path where the publish context is already complete before
  NodeStage waits.
- QA baseline pass: run `20260515-092817-61f9` passed 62/62 actions and
  confirmed the Stage 2 baseline contract end-to-end.
- Verification passed:
  `go test ./core/csi ./core/host/master ./cmd/blockcsi -count=1`,
  `bash -n scripts/run-alpha-app-demo.sh`, YAML parse of the Stage 2 baseline,
  and `git diff --check`.
- Closed: QA baseline run `20260515-092817-61f9` proved host multipath setup;
  D4 runs `20260515-103621-5d61` and `20260515-104802-a618` proved mounted
  failover without pod recreate; formal close report passed all 14 hard-gate
  clauses.

## D4: Runner-Native Stage 2 Gate

Add one runner-native scenario for the selected protocol, likely:

```text
testops/scenarios/stage2-iscsi-alua-multipath-failover-chain.yaml
```

Required artifact shape:

```text
multipath-prereq.txt
pre-failure-inventory/
sg-inq.txt
sg-rtpg.before.txt
multipath-before.txt
writer.log
primary-failure.txt
authority-after.txt
sg-rtpg.after.txt
multipath-after.txt
workload-after-failover.log
support-bundle/
bounded-waits.txt
cleanup-audit.txt
```

Status: first baseline scenario added at
`testops/scenarios/stage2-iscsi-alua-multipath-baseline-chain.yaml`. It proves
Stage 2 opt-in and host multipath evidence before failure injection. The
eventual close scenario should use the D4 filename above only when it proves the
original mounted workload survives primary failure without pod recreate.

Current D4 slice: `testops/scenarios/stage2-iscsi-alua-multipath-failover-chain.yaml`
now exists. It uses the same RF3 sync-quorum writer-held mount, keeps resources
alive after `writer-verified`, derives the primary replica from live inventory,
scales that primary Deployment to zero, waits for one promoted primary, and
execs into the original writer pod to verify and write data after failure. It
records `pod_recreate_used=false`,
`data_check_after_failover=mounted_workload_checksum_passed`, ALUA/RTPG
before/after files, multipath before/after files, and bounded waits. QA has not
only run it, but reproduced the product behavior twice. The strict close run
`20260515-104802-a618` passed 48/48 actions with clean residue.

## D5: Operations Manual Update

Only after the runner gate passes, update `docs/operations-v1.md`:

- Stage 1 recovery: CSI/pod recreate.
- Stage 2 recovery: selected protocol multipath path.
- Exact prerequisites and non-claims.
- How to inspect support bundles.

## Gates To Close

This plan closes only when:

1. A cold product spec names the selected protocol and exact user-visible
   failover contract.
2. A strict QA hard gate exists before implementation is treated as complete.
3. Fast tests protect protocol state mapping and stale-primary rejection.
4. A runner-native gate proves mounted failover through multipath, not pod
   recreate.
5. Support bundles explain authority, protocol path state, host multipath
   state, bounded waits, blocker reasons, and data verification.
6. User-facing docs distinguish Stage 1 CSI/pod-recreate recovery from Stage 2
   multipath recovery.
7. QA validates independently and reports no blocking issue.
