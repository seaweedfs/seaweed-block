# Seaweed Block Roadmap

> Document status: public roadmap summary. It is intentionally high level and
> may trail the internal execution plan between releases. For current
> user-facing claims, use [`quickstart-kubernetes.md`](quickstart-kubernetes.md)
> and [`releases/README.md`](releases/README.md).

This roadmap is intentionally practical. The goal is to make a Kubernetes block
service that is easy to try, review, and improve without hiding unfinished
storage semantics.

## Current Alpha MVP

Already demonstrated:

- CSI static PV path on Kubernetes.
- CSI dynamic PVC create path.
- CSI dynamic PVC delete/cleanup path.
- iSCSI mount into a real pod.
- Pod write/read checksum through the mounted volume.
- TestOps registry and CLI MVP.
- Deeper recovery/failover components tested below the K8s surface.

Current alpha constraints:

- single-node Kubernetes lab evidence
- iSCSI and NVMe-oF frontends are release-gated on a single-node lab
- `walstore` backend only
- launcher-generated blockvolume state uses `emptyDir`
- harness applies generated blockvolume manifests
- generated blockvolume Deployments can use PVC owner references for cleanup
- no production operator yet; only the bounded v0.5 beta-candidate
  lifecycle-owner mutates `SwBlockVolume.metadata.finalizers`

## Near-Term MVP Hardening

1. Package a clean Kubernetes install surface.

   - v0.2 alpha: script-based Day-1 activation path
   - v0.3 alpha: Helm chart for the same supported path
   - document image names, immutable release tags, prerequisites, and cleanup
   - remove gate-specific naming from user paths where possible

2. Replace `emptyDir` for blockvolume state.

   - add durable root configuration
   - support hostPath/local-path style lab persistence
   - keep `emptyDir` only for explicit throwaway smoke tests

3. Add a minimal operator/controller loop after Helm.

   - do not jump directly from scripts to an operator
   - first stabilize install values, chart ownership, and uninstall behavior
   - read-only CRDs, Conditions, Events, node readiness, support evidence refs,
     cleanup visibility, and safe next-step status are now gated as the first
     operator foundation
   - mutating lifecycle ownership, finalizers, upgrade execution, repair,
     rebuild, failback, and automatic cleanup remain separate future phases

4. Improve TestOps usability.

   - remote shell execution for K8s scenarios
   - stable result bundles
   - scenario registry index
   - stronger negative-path gates for blocked, stale, unreachable, corrupt
     evidence, and cleanup-residue cases

## Current Capability Snapshot

Seaweed Block can already demonstrate a narrow Kubernetes block-storage loop:

- Helm install on supported lab clusters.
- Dynamic PVC provisioning through CSI.
- App pod mount, write, read, and replacement-reader verification.
- iSCSI frontend with gated ALUA/dm-multipath failover evidence.
- RF=3 multi-volume lab path with independent volume identity and publish
  target.
- Restart persistence for supported hostPath gates.
- Read-only CLI/report/dashboard/support-bundle flows.
- Read-only CRD status and Events for `SwBlockCluster` and `SwBlockVolume`.
- Live node evidence for Kubernetes Ready/SchedulingDisabled, CSI registration,
  CSI image-pull blockers, host-prereq evidence replay, and loopback
  cross-node attach blockers.
- Negative-first status: blocked, stale, unreachable, or corrupt evidence must
  not become false `Ready=True`.
- Cleanup verification for Kubernetes, iSCSI, multipath, process, and hostPath
  residue.
- Executable lifecycle action contracts: dry-run actions can be allowed with
  evidence, unsafe/future-mutating actions are rejected with stable reasons, and
  CRD/report/dashboard/operator-snapshot surfaces agree on the decision.
- Delete-safety status and cleanup-required visibility without finalizer or
  cleanup mutation.
- Bounded `SwBlockVolume` finalizer lifecycle in the v0.5 beta-candidate path:
  CSI-created identity CRs, lifecycle-owner protection finalizer, evidence-driven
  hold/release, Events, and multi-volume isolation.
- NVMe/TCP CSI multipath attach is gated for the supported lab path; RoCE and
  NVMe/RDMA remain explicit non-claims until host preflight plus live I/O gates
  prove them.
- Cross-node loopback NVMe/TCP topology is explicitly blocked. The
  non-loopback NVMe/TCP publish path is wired through blockvolume,
  blockmaster, Helm, and generated values, and the supported lab path now has a
  live cross-node writer/reader gate against a routable NVMe/TCP target. This
  is still not a RoCE, performance/SLO, broad compatibility, or production HA
  claim.
- Install drift status for current versus desired chart/app/image identity
  without upgrade execution.
- CRD/RBAC status-writer conformance coverage for the failures that previously
  escaped mock tests and only failed in live QA.

The product model is converging around one read-only control-plane pattern:
truth owners publish facts, the status layer aggregates judgment, executors stay
bounded, and evidence explains why the status is allowed. The next work should
preserve that model rather than add isolated scripts or separate status systems.

Recommended order from here:

1. Operator-status foundation release: complete in v0.4 beta. It claims
   status/events-only visibility, not lifecycle mutation.
2. Operation Layer v0.5 beta candidate: code and QA complete through Phase 44;
   release images still need publish/pinned-image smoke before marking it
   shipped. The team is intentionally skipping that release step for the next
   development slice, so v0.5 must not be marked released from roadmap wording.
   It claims a bounded `SwBlockVolume` protection-finalizer lifecycle, not
   automatic cleanup or broad operator automation.
   - Phase 41: lifecycle-owner foundation. **Closed 2026-06-14, QA PASS**
     (`internal/docs/finished-plans/phase41_finishedplan_lifecycle_owner_foundation.md`).
     Observer/lifecycle-owner/executor roles defined; delete-safety preconditions
     and a dry-run finalizer-release action shipped.
   - Phase 42: real API/admission proof. **Closed 2026-06-15, QA PASS**
     (`internal/docs/finished-plans/phase42_finishedplan_lifecycle_owner_admission_gate.md`).
     The lifecycle-owner patch boundary is proven with real Kubernetes
     ValidatingAdmissionPolicy.
   - Phase 43: first bounded lifecycle mutation. **Closed 2026-06-15, QA PASS**
     (`internal/docs/finished-plans/phase43_finishedplan_bounded_finalizer_lifecycle.md`).
     Finalizer add/release works as isolated gates.
   - Phase 44: delete lifecycle close gate. **Closed 2026-06-17, QA PASS**
     (`internal/docs/finished-plans/phase44_finishedplan_delete_lifecycle_close_gate.md`).
     The integrated PVC -> protected CR -> hold/release -> zero-residue path is
     validated end-to-end.
3. Phase 46: productize returned-replica rebuild/reintegration status and
   decisioning. **Closed 2026-06-19, QA PASS**
   (`internal/docs/finished-plans/phase46_finishedplan_returned_replica_reintegration_productization.md`).
   Returned replicas are now Kubernetes/product-surface visible, fenced from
   frontend/ACK eligibility until evidence supports reintegration, and
   volume-scoped across multi-volume reports. Automatic failback or broad
   rebuild execution remains a later executor phase.
4. Phase 47: returned-replica executor admission. **Closed 2026-06-20, QA PASS**
   (`internal/docs/finished-plans/phase47_finishedplan_returned_replica_executor_admission.md`).
   `authority.reintegrate_returned_replica` is allowed only as a dry-run,
   non-mutating action when exact fencing and frontier evidence is present.
   This is not automatic failback or rebuild execution.
5. Phase 48: returned-replica live evidence close. **Closed 2026-06-20, QA PASS**
   (`internal/docs/finished-plans/phase48_finishedplan_returned_replica_live_evidence.md`).
   The live iSCSI returned-replica scenario now emits same-run managed-volume
   evidence proving r1 remains fenced, r2 remains the primary, and the returned
   replica covers the required frontier before any future executor mutates
   authority or storage state.
6. Phase 49: returned-replica executor preflight. **Closed 2026-06-20, local PASS**
   (`internal/docs/finished-plans/phase49_finishedplan_returned_replica_executor_preflight.md`).
   The returned-replica dry-run action now has a typed preflight that is ready
   only when the target is unique, frontend/ACK fenced, and durable frontier
   evidence covers the required frontier. It remains non-mutating and does not
   claim automatic reintegration, rebuild, failback, or frontend publication.
7. Phase 50: returned-replica preflight status schema. **Closed 2026-06-20, local PASS**
   (`internal/docs/finished-plans/phase50_finishedplan_returned_replica_preflight_status_schema.md`).
   The preflight is now machine-readable in operator-snapshot JSON and
   SwBlockVolume `.status.executorPreflights[]`, with OpenAPI schema and
   status-writer coverage. It is still non-mutating.
8. Phase 51: returned-replica ACK evidence gate. **Closed 2026-06-21, QA PASS**
   (`internal/docs/finished-plans/phase51_finishedplan_returned_replica_ack_evidence_gate.md`).
   The executor preflight now distinguishes explicit ACK non-eligibility from
   missing ACK evidence. A future executor may not treat default
   `ack_eligible=false` as proof; preflight readiness requires
   `ack_eligibility_known=true` and `ack_eligible=false`.
9. Phase 52: returned-replica executor contract. **Closed 2026-06-21, QA PASS**
   (`internal/docs/finished-plans/phase52_finishedplan_returned_replica_executor_contract.md`).
   The future executor boundary is published as a non-mutating contract:
   execution remains disabled, only ACK eligibility is named as the future
   allowed mutation class, frontend publication/rebuild traffic/failback remain
   forbidden, and terminal evidence is required before any later executor can
   claim completion.
10. Phase 53: returned-replica authority executor skeleton. **Closed 2026-06-22, QA PASS**
    (`internal/docs/finished-plans/phase53_finishedplan_returned_replica_executor_skeleton.md`).
    The executor process boundary is now disabled-by-default with
    read-only `SwBlockVolume` access. It consumes executor contracts and fails
    closed on execution-enabled or mutating contracts, but still performs no ACK
    eligibility mutation, frontend publication, rebuild traffic, or failback.
11. Phase 54: returned-replica reintegration executor milestone. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase54_finishedplan_returned_replica_reintegration_executor.md`).
    This is intentionally larger than the previous safety-latch phases. It
    groups executor policy, the ACK eligibility mutation target, admission/RBAC,
    terminal evidence, failure states, multi-volume isolation, and a live close
    gate into one milestone. The first allowed mutation remains narrow:
    returned-replica ACK eligibility only. D1-D7 passed live QA through the RBAC
    boundary, executor call-site terminal-evidence gate, negative/hold matrix,
    dedicated multi-volume isolation, and the live returned-replica close gate.
    Frontend publication, rebuild traffic, and automatic failback remain
    non-claims.
12. Phase 55: release and documentation hardening. **Closed 2026-06-23, local PASS**
    (`internal/docs/finished-plans/phase55_finishedplan_release_docs_hardening.md`).
    This phase aligns README, release notes, user capabilities, and developer
    wiki pages with the Phase 54 boundary. It should not add a new storage
    feature or broaden the claim beyond ACK eligibility target status.
13. Phase 56: returned-replica rebuild/catch-up contract. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase56_finishedplan_returned_replica_rebuild_contract.md`).
    The product now surfaces `authority.rebuild_returned_replica` as a disabled
    future action when a returned replica is frontend-fenced and behind the
    required frontier. It does not start rebuild traffic.
14. Phase 57: rebuild progress target. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase57_finishedplan_rebuild_progress_target.md`).
    `SwBlockReplicaRebuild.status` exists as the narrow target for planned
    rebuild progress. The authority executor may write only planned status under
    the `rebuild_traffic` mutation class; it still does not copy data, publish a
    frontend, or fail back a replica.
15. Phase 58: rebuild target owner. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase58_finishedplan_rebuild_target_owner.md`).
    A separate target-owner identity can create `SwBlockReplicaRebuild` main
    objects from ready rebuild contracts. It cannot write status or mutate
    volume, workload, frontend, or storage state.
16. Phase 59: rebuild planning close gate. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase59_finishedplan_rebuild_planning_close_gate.md`).
    This gate connects Phases 56-58 as one product path:
    rebuild contract -> target-owner-created target CR -> executor planned
    status. It is still planning/status only, not real rebuild data movement.
17. Phase 60: rebuild/catch-up data-path gate. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase60_finishedplan_rebuild_catchup_datapath_gate.md`).
    The existing engine/adapter/transport/recovery stack now has a repeatable
    gate proving catch-up traffic, dual-lane rebuild traffic, session close,
    durable ack, live WAL during rebuild, same-LBA arbitration, and byte-equal
    convergence. This proves the data path beneath the planning model; it does
    not yet wire the Kubernetes authority executor to trigger that traffic in a
    live blockvolume pod.
18. Phase 61: authority executor runtime call-site. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase61_finishedplan_authority_executor_runtime_callsite.md`).
    The authority executor now has a bounded `AuthorityRebuildRuntime` seam.
    With no runtime it preserves the Phase 59 `planned` behavior; with a
    runtime it writes `running`, invokes the runtime, and maps terminal evidence
    to `caught_up` or `blocked`. This still does not wire a concrete
    blockvolume RPC/HTTP/gRPC transport.
19. Phase 62: authority executor HTTP runtime. **Closed 2026-06-23, QA PASS**
    (`internal/docs/finished-plans/phase62_finishedplan_authority_executor_http_runtime.md`).
    The executor can call a typed HTTP runtime seam and preserve the
    no-frontend/no-failback/non-ACK-mutation boundary.
20. Phase 63: rebuild runtime target contract. **Closed 2026-06-24, QA PASS**
    (`internal/docs/finished-plans/phase63_finishedplan_rebuild_runtime_target_contract.md`).
    Runtime targets carry the explicit endpoint/session/frontier fields needed
    before a blockvolume runtime call can be attempted.
21. Phase 64: blockvolume runtime endpoint. **Closed 2026-06-24, QA PASS**
    (`internal/docs/finished-plans/phase64_finishedplan_blockvolume_runtime_endpoint.md`).
    The blockvolume status server exposes the bounded `/runtime/rebuild`
    endpoint for rebuild/catch-up traffic under explicit opt-in.
22. Phase 65: runtime terminal evidence. **Closed 2026-06-24, QA PASS**
    (`internal/docs/finished-plans/phase65_finishedplan_runtime_terminal_evidence.md`).
    Rebuild runtime results now distinguish running/caught_up/blocked without
    claiming frontend publication or failback.
23. Phase 66: caught-up publication preflight. **Closed 2026-06-24, QA PASS**
    (`internal/docs/finished-plans/phase66_finishedplan_caught_up_publication_preflight.md`).
    Caught-up rebuild evidence feeds the next publication preflight while
    keeping ACK eligibility, frontend publication, and failback disabled.
24. Phase 67: ACK eligibility publication. **Closed 2026-06-25, QA PASS**
    (`internal/docs/finished-plans/phase67_finishedplan_ack_eligibility_publication.md`).
    Terminal caught-up evidence can now record ACK eligibility, still without
    frontend publication, rebuild traffic, or failback.
25. Phase 68: frontend publication preflight. **Closed 2026-06-25, QA PASS**
    (`internal/docs/finished-plans/phase68_finishedplan_frontend_publication_preflight.md`).
    The product surfaces frontend-publication as an explicit disabled
    preflight rather than implying it from ACK eligibility.
26. Phase 69: frontend publication target contract. **Closed 2026-06-25, QA PASS**
    (`internal/docs/finished-plans/phase69_finishedplan_frontend_publication_target_contract.md`).
    A typed `SwBlockFrontendPublication` target exists, but it creates no
    frontend or authority side effect.
27. Phase 70: frontend publication executor boundary. **Closed 2026-06-25, QA PASS**
    (`internal/docs/finished-plans/phase70_finishedplan_frontend_publication_executor_boundary.md`).
    The executor writes disabled status only.
28. Phase 71: frontend publication live API boundary. **Closed 2026-06-25, QA PASS**
    (`internal/docs/finished-plans/phase71_finishedplan_frontend_publication_live_api_boundary.md`).
    Live Kubernetes API/RBAC proves the executor remains status-only.
29. Phase 72: frontend publication runtime contract. **Closed 2026-06-25, QA PASS**
    (`internal/docs/finished-plans/phase72_finishedplan_frontend_publication_runtime_contract.md`).
    A generic typed HTTP runtime seam exists for future publication work.
30. Phase 73: frontend publication authority owner guard. **Closed 2026-06-25, local PASS**
    (`internal/docs/finished-plans/phase73_finishedplan_frontend_publication_authority_owner_guard.md`).
    The returned-replica pipeline now blocks independent frontend publication
    when `primaryUnchanged=true`, because making a returned replica active is an
    authority/failback operation, not a standalone runtime status success.
31. Phase 74: returned-replica failback contract. **Closed 2026-06-25, local PASS**
    (`internal/docs/finished-plans/phase74_finishedplan_returned_replica_failback_contract.md`).
    The ACK-after returned-replica state now surfaces an explicit
    `authority.failback_returned_replica` action and disabled executor contract.
    It names `failback` as the future mutation envelope, requires terminal
    evidence for authority ownership, epoch advance, single-primary state, and
    publish-target swap, and still performs no failback or frontend publication.
32. Phase 75: returned-replica failback target owner. **Closed 2026-06-25, local PASS**
    (`internal/docs/finished-plans/phase75_finishedplan_failback_target_owner.md`).
    A disabled-by-default target owner can convert a ready
    `authority.failback_returned_replica` contract into a
    `SwBlockReplicaFailback` handoff CR only after ACK eligibility, frontend
    fencing, durable-frontier coverage, and identity-isolation evidence are
    present. It can create the target object only; it cannot write status,
    finalizers, Events, storage/workloads, authority state, frontend
    publication, or failback execution.
33. Phase 76: returned-replica failback executor boundary. **Closed 2026-06-25, local PASS**
    (`internal/docs/finished-plans/phase76_finishedplan_failback_executor_boundary.md`).
    The failback executor can read `SwBlockReplicaFailback` targets and write
    disabled/blocked status through `swblockreplicafailbacks/status`. It remains
    status-only: no failback, authority epoch advance, primary reassignment,
    publish-target swap, frontend publication, or storage mutation is executed.
34. Phase 77: returned-replica failback runtime contract. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase77_finishedplan_failback_runtime_contract.md`).
    The failback executor now has an explicit runtime request/response contract
    and execution-policy gate. Default behavior remains disabled. A test/fake
    runtime can prove the terminal evidence shape for authority epoch advance,
    single-primary state, publish-target swap, and no storage mutation, but no
    real blockmaster failback endpoint or authority mutation is shipped.
35. Phase 78: failback authority runtime seam. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase78_finishedplan_failback_authority_runtime.md`).
    Returned-replica failback now has a first product-owned authority seam:
    validated endpoint and expected-current evidence can mint
    `Publisher.apply(IntentReassign)` and prove epoch advance, single-primary
    state, and publish-target swap. Default deployed behavior remains disabled;
    no automatic failback call-site, frontend publication, storage mutation, or
    workload mutation is enabled.
36. Phase 79: failback authority call-site. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase79_finishedplan_failback_authority_callsite.md`).
    The failback executor can now invoke an in-process
    `AuthorityFailbackRuntime` adapter under the explicit execution-policy
    gate. The adapter advances the Publisher authority line and writes
    `failed_back` only after terminal evidence. Stale expected-current evidence
    blocks the call-site; frontend publication and storage mutation remain
    false. The deployed controller loop still does not perform automatic
    failback.
37. Phase 80: master failback runtime factory. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase80_finishedplan_master_failback_runtime_factory.md`).
    Blockmaster now exposes a master-owned failback authority runtime factory
    backed by its live Publisher. A host-level gate proves product-loop-seeded
    authority can advance through the runtime while no public failback RPC,
    automatic failback loop, frontend publication, or storage mutation is
    enabled.
38. Phase 81: failback service RPC. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase81_finishedplan_failback_service_rpc.md`).
    Blockmaster now registers a `FailbackService.ExecuteFailback` RPC that is
    disabled by default behind `--failback-runtime-rpc`. When explicitly
    enabled, it delegates to the master-owned failback runtime and can advance
    the live Publisher with expected-current and terminal evidence. Default
    installs still do not expose an active failback mutation path.
39. Phase 82: failback executor gRPC runtime. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase82_finishedplan_failback_executor_grpc_runtime.md`).
    The failback executor can now call blockmaster's FailbackService through a
    gRPC runtime transport when `--enable-execution`, `--execution-policy`, and
    `--failback-runtime-grpc-addr` are all explicit. HTTP runtime remains
    supported; HTTP and gRPC transports are mutually exclusive. Default
    behavior remains status-only.
40. Phase 83: failback runtime chart wiring. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase83_finishedplan_failback_chart_runtime_wiring.md`).
    Helm can now render the disabled-by-default blockmaster failback RPC and
    failback-executor gRPC runtime flags when every execution switch is
    explicit. Default chart behavior remains non-mutating. The chart fails fast
    for dry-run execution, missing execution policy, and ambiguous HTTP/gRPC
    runtime transports.
41. Phase 84: failback integrated gRPC smoke. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase84_finishedplan_failback_integrated_grpc_smoke.md`).
    A local integrated test now drives the failback executor through
    `GRPCFailbackRuntime` into a real blockmaster `FailbackService`, which then
    advances the master-owned Publisher. This proves executor status,
    authority-epoch advance, and publish-target swap through the real service
    path while keeping frontend publication and storage mutation false.
42. Phase 85: failback executor policy safety. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase85_finishedplan_failback_executor_policy_safety.md`).
    Execution flags alone are now explicitly tested as insufficient: with no
    target, the executor performs zero runtime calls; with an invalid target it
    writes blocked status and still performs zero runtime calls. A valid target
    remains the only path that may call the failback runtime.
43. Phase 86: failback gRPC runtime endpoint decoupling. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase86_finishedplan_failback_grpc_runtime_endpoint_decoupling.md`).
    Explicit gRPC failback runtime no longer depends on the legacy target-local
    HTTP `runtimeEndpoint` field. HTTP endpoint fallback remains supported, and
    invalid targets still block without runtime calls.
44. Phase 87: failback documentation alignment. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase87_finishedplan_failback_docs_alignment.md`).
    README, product roadmap, and engineering wiki now agree that returned-replica
    failback runtime is opt-in/source-gated, automatic deployed failback is not
    claimed, and frontend publication after failback was still future work at
    that point. Phase 98 later closed the explicit opt-in frontend-publication
    and workload-I/O gate.
45. Phase 88: failback deployed suite packaging. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase88_finishedplan_failback_deployed_suite.md`).
    Helm can now render the complete opt-in failback component suite:
    blockmaster failback RPC, failback target owner, failback executor, explicit
    execution policy, and gRPC runtime address. The gate also adds
    `failbackTargetOwner` values-schema coverage and keeps default installs
    non-mutating. This remains a packaging gate, not an automatic live failback
    release claim.
46. Phase 89: SwBlockVolume authority facts. **Closed 2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase89_finishedplan_swblockvolume_authority_facts.md`).
    `SwBlockVolume.status`, operator-snapshot, and reports now carry the
    current primary replica, publish target, authority epoch, and endpoint
    version so later failback planning does not infer authority from stale or
    side-channel state.
47. Phase 90: failback targets require current authority facts. **Closed
    2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase90_finishedplan_failback_target_authority_gate.md`).
    The failback target owner refuses to create targets unless the source
    `SwBlockVolume.status` contains positive current-authority evidence, and it
    stamps expected-current replica/epoch guards on the target.
48. Phase 91: explicit failback target activation policy. **Closed
    2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase91_finishedplan_failback_target_activation_policy.md`).
    Failback target activation is default-off and requires both an explicit
    activation policy and a runtime endpoint before the target can be marked
    executable.
49. Phase 92: failback target-owner -> executor handoff. **Closed
    2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase92_finishedplan_failback_target_executor_handoff.md`).
    The target owner and executor now share an executable target contract that
    preserves expected-current authority guards and terminal returned-replica
    evidence through the handoff.
50. Phase 93: multi-volume failback handoff isolation. **Closed 2026-06-26,
    local PASS**
    (`internal/docs/finished-plans/phase93_finishedplan_failback_handoff_isolation.md`).
    Multi-volume target-owner/executor handoff keeps volume IDs, returned
    replicas, expected-current authority, and target data/control addresses
    isolated.
51. Phase 94: deployed failback suite render + gRPC smoke. **Closed
    2026-06-26, local PASS**
    (`internal/docs/finished-plans/phase94_finishedplan_failback_deployed_grpc_smoke.md`).
    The full opt-in Helm suite renders with blockmaster failback RPC,
    target-owner activation, and executor gRPC runtime flags, and the executor
    can call a real blockmaster FailbackService in a local Go smoke.
52. Phase 95: live deployed failback suite smoke. **Closed 2026-06-26, live PASS**
    (`internal/docs/finished-plans/phase95_finishedplan_failback_live_deployed_suite.md`).
    This gate pays the real k3s cost: fresh images, Helm install, first-volume
    authority line, returned-replica failback contract injection, target
    creation, executor gRPC call to live blockmaster, terminal `failed_back`
    status, RBAC boundary, and cleanup. It still does not claim frontend
    publication or workload-visible path switch.
53. Phase 96: failback terminal evidence to frontend publication target. **Closed
    2026-06-26, runner PASS**
    (`internal/docs/finished-plans/phase96_finishedplan_failback_frontend_publication_target.md`).
    The frontend-publication target owner now accepts terminal
    `SwBlockReplicaFailback.status=failed_back/failback_completed` evidence and
    creates a disabled `SwBlockFrontendPublication` target with explicit
    failback-source fields. The executor remains default-off; no frontend path
    is published.
54. Phase 97: frontend publication executor call-site. **Closed 2026-06-26,
    runner PASS**
    (`internal/docs/finished-plans/phase97_finishedplan_frontend_publication_executor_callsite.md`).
    The executor can invoke a frontend-publication runtime for an enabled
    failback-source target only under explicit policy and writes
    `frontend_published` only from valid terminal evidence. Workload-visible
    I/O remains a separate gate.
55. Phase 98: failback frontend workload close gate. **Closed 2026-06-26,
    live PASS**
    (`internal/docs/finished-plans/phase98_finishedplan_failback_frontend_workload_close.md`).
    The deployed opt-in suite now proves the full returned-replica operation
    loop through live k3s: first PVC writer/reader, returned-replica failback,
    product-owned frontend publication, post-publication writer/reader I/O, and
    zero-residue cleanup. Default automatic failback remains off.
56. Phase 99: NVMe ANA baseline. **Closed 2026-06-26, runner PASS**
    Current code already has ANA Identify/Get Log Page behind an ANA provider,
    blockvolume projection-backed ANA state, direct-host P4 ANA/multipath
    gates, and P5 Kubernetes CSI single-path NVMe protocol selection. The next
    parity gap is Kubernetes CSI NVMe multipath attach: multiple NVMe frontend
    paths for one NQN/NSID must survive master status, CSI publish context,
    NodeStage, app writer/reader, and cleanup evidence.
57. Operation milestone release readiness. **Active, image-publish blocked**
    The operation layer is release-ready from code/QA perspective, but it must
    not be marked shipped until matching `seaweed-block` and
    `seaweed-block-csi` images are published from the same commit and pass the
    pinned-image Day-1 smoke. NVMe multipath is not part of this release claim.
58. Phase 100: Kubernetes CSI NVMe multipath attach. **Closed 2026-06-27,
    live k3s PASS**
    The supported lab path now proves dynamic PVC `protocol=nvme`,
    `replicationFactor=2`, two NVMe frontend paths for one NQN/NSID, CSI
    publish/NodeStage multipath attach, mounted writer/reader I/O, and zero
    Seaweed Block NVMe residue after delete. This is not a RoCE, performance,
    broad host compatibility, production HA, or soak claim.
59. Phase 101: NVMe Hardening And Soak. **Closed 2026-06-28, runner PASS**
    The supported-lab NVMe path now has status-surface identity and health
    projection, a one-path-loss gate that prevents false Ready, a repeated
    stage/unstage zero-residue gate, and a bounded mounted writer/reader soak.
    This remains a supported-lab claim only: no RoCE, production HA, broad host
    compatibility, performance/SLO, or backup/restore claim.
60. Phase 102: NVMe Release Artifact Smoke. **Active, image-publish blocked**
    Validate that matching published `seaweed-block` and `seaweed-block-csi`
    images contain the Phase 100/101 NVMe behavior. The gate pulls the release
    images, runs Kubernetes CSI NVMe multipath attach against the image pair,
    and runs the Phase 101 standalone hardening gates using binaries extracted
    from the published `seaweed-block` image. If images are missing, the gate
    blocks as an artifact-readiness issue, not a product failure.
61. Phase 103: NVMe Multi-Host / RoCE Preflight. **Closed 2026-06-29,
    runner PASS**
    Adds a read-only host capability gate for NVMe/TCP and RoCE candidacy. RDMA
    hardware plus `nvme-rdma` module availability can make a host a candidate,
    but still does not permit a RoCE or performance product claim.
62. Phase 104: RoCE Live-I/O Feasibility Boundary. **Closed 2026-06-29,
    runner PASS**
    Makes the current target boundary explicit: `--nvme-transport=rdma` is
    rejected because only NVMe/TCP is implemented. This is a refusal gate, not a
    RoCE implementation.
63. Phase 105: NVMe/TCP Multi-Host Topology Boundary. **Closed 2026-06-29,
    runner PASS**
    Cross-node loopback NVMe/TCP evidence now blocks with
    `publish_target_loopback_cross_node`, never emits false `Ready=True`, and
    surfaces the read-only `observe.inspect_publish_target_topology` action
    instead of an iSCSI remediation.
64. Phase 106: NVMe/TCP Cross-Node Non-Loopback Live Attach. **Closed
    2026-06-29, live k3s PASS**
    Proved the positive multi-host path with a routable NVMe/TCP target:
    blockvolume on `m01`, workload on `m02`, publish target
    `192.168.1.181:4420`, protocol `nvme`, managed-volume
    `ready/first_volume_verified`, writer/reader verified, and strict cleanup
    audit `cleanup_status=ok`. This still excludes RoCE, performance/SLO,
    broad compatibility, and production HA.
65. Phase 107: NVMe/TCP Multi-Volume Cross-Node Isolation. **Closed
    2026-06-29, live k3s PASS**
    Proved the new routable NVMe/TCP path with two PVCs in the supported lab:
    `protocol=nvme`, app pods pinned to `m02`, `writer_verified_count=2`,
    `reader_verified_count=2`, managed volumes
    `ready/first_volume_verified`, two distinct volume IDs, two distinct NVMe
    NQNs, no loopback publish target, no cross-volume identity mix-up, and
    strict cleanup audit `cleanup_status=ok`. This still excludes RoCE,
    performance/SLO, broad compatibility, production HA, and broader scale.
66. Phase 108: NVMe/TCP Multi-Volume Lifecycle Soak. **Closed 2026-06-29,
    live k3s PASS**
    Ran two consecutive two-PVC NVMe/TCP cross-node lifecycle cycles with app
    pods pinned to `m02`. Each cycle verified two writers and two readers, then
    helper cleanup waited for generated blockvolume pods, matching PVs, and
    SeaweedFS NVMe subsystems to drain before declaring `cleanup_status=ok`.
    Terminal evidence showed `cycle_1_nvme_residue_count=0`,
    `cycle_1_k8s_residue_count=0`, `cycle_2_nvme_residue_count=0`, and
    `cycle_2_k8s_residue_count=0`; final strict cleanup returned
    `cleanup_status=ok`.
67. Phase 109: NVMe/TCP Status Surface Evidence. **Closed 2026-06-29,
    live k3s PASS**
    Proved two supported-lab NVMe/TCP PVCs publish matching protocol, NQN,
    namespace ID, address, path count, readiness, and reason across
    `SwBlockVolume.status.nvme`, report summary, report
    `operator-snapshot.json`, dashboard `/operator-snapshot.json`, and
    `ops explain`. The gate owns its CRD baseline so stale live CRDs cannot
    silently prune new status fields.
68. Phase 110: NVMe/TCP Path-Loss Status Surface Honesty. **Closed
    2026-06-29, runner PASS**
    Reused the live mounted NVMe/TCP one-path-loss gate from Phase 101 and
    replayed its after-failover `cluster-after-failover.json` through the
    support-bundle surfaces. Report summary, report `operator-snapshot.json`,
    dashboard `/operator-snapshot.json`, and `ops explain` all preserve
    `blocked/nvme_multipath_path_missing`, `path_count=1`,
    `multipath_observed=false`, `mutation_allowed=false`, and zero false
    `Ready=True`. This is support-surface replay of real standalone path-loss
    evidence, not a live Kubernetes CRD negative-path-loss claim.
69. Phase 111: NVMe/TCP K8s Path-Loss CRD Honesty. **Closed 2026-06-29,
    runner PASS**
    Closed the Phase 110 non-claim in the live Kubernetes path. A real RF=2
    NVMe/TCP PVC first reached `SwBlockVolume.status.nvme.pathCount=2` and
    `Ready=True/first_volume_verified`; then one generated blockvolume
    deployment was scaled to zero, reducing the observed NVMe path count to one.
    `SwBlockVolume.status`, report summary, report `operator-snapshot.json`,
    dashboard `/operator-snapshot.json`, and `ops explain` all converged on
    `blocked/nvme_multipath_path_missing` with no false volume `Ready=True`,
    `mutation_allowed=false`, and zero cleanup residue.
70. Phase 112: NVMe/TCP K8s Mounted Path-Loss I/O. **Closed 2026-06-29,
    runner PASS**
    Closed the remaining user-visible gap after Phase 111. The live
    Kubernetes RF=2 NVMe/TCP path now keeps an already-mounted pod on the same
    PVC while one generated blockvolume deployment is scaled to zero. The same
    pod UID survives, the pod writes and reads after path loss through the
    remaining NVMe path, and the CRD/report/operator-snapshot/dashboard/explain
    surfaces still report `blocked/nvme_multipath_path_missing` with no false
    volume `Ready=True`, `mutation_allowed=false`, and zero cleanup residue.
71. Phase 113: NVMe/TCP K8s Mounted Path Restore. **Closed 2026-06-29,
    runner PASS**
    Closed the matching restoration loop for the supported-lab path. After the
    Phase 112 one-path-loss state, the removed blockvolume deployment is scaled
    back to one replica. The same mounted pod UID survives, the pod writes and
    reads after restore, and `SwBlockVolume.status`, report,
    operator-snapshot, and explain converge back to two observed NVMe paths and
    `Ready=True/first_volume_verified`, with zero cleanup residue.
72. Phase 114: NVMe/TCP K8s Multi-Volume Mounted Path Isolation. **Closed
    2026-06-30, runner PASS**
    Extends the Phase 112/113 single-volume mounted path-loss/restore proof to
    two independent RF=2 NVMe/TCP PVCs. When one generated blockvolume
    deployment is scaled to zero, the affected volume reports
    `blocked/nvme_multipath_path_missing` with one live host path, the
    untouched volume remains `ready/first_volume_verified` with two live host
    paths, both mounted pods keep their UIDs, both continue I/O, and there is
    no cross-volume reason mix-up. After the removed deployment is restored,
    both volumes return to `Ready=True/first_volume_verified` with two live
    host paths and mounted I/O still works, with zero cleanup residue.
73. Phase 115: NVMe/TCP Mounted Multi-Volume Path Churn Soak. **Closed
    2026-06-30, runner PASS**
    Extends Phase 114 from a one-shot multi-volume loss/restore proof to a
    bounded churn proof: alternate path loss and restore across two mounted
    RF=2 NVMe/TCP PVCs for multiple cycles, preserving mounted pod identity,
    writer/reader I/O, volume identity, publish-target isolation, reason-code
    isolation, two-path restoration, and zero cleanup residue.
74. Phase 116: NVMe/TCP Supported-Lab Release Claim Packaging. **Closed
    2026-06-30, docs PASS**
    Convert the closed Phase 100-115 evidence into a user-facing supported-lab
    claim: README/docs wording, feature/status matrix, release non-claims, and
    a concise release-smoke plan that uses matching published
    `seaweed-block`/`seaweed-block-csi` images when available. This is a
    packaging and claim-boundary phase, not a new transport feature.
75. Phase 117: NVMe/TCP Published-Image Release Smoke. **Active, gate
    implemented; waiting for images**
    Run the representative NVMe/TCP supported-lab smoke against matching
    published `seaweed-block` and `seaweed-block-csi` images via
    `scripts/run-phase117-nvme-release-image-smoke-gate.sh` /
    `testops/scenarios/nvme-tcp-release-image-smoke-chain.yaml`. If images are
    not available, this phase blocks as artifact-readiness, not as product
    failure. Do not mark the NVMe/TCP path as a published-image release claim
    until this smoke passes.
76. Phase 118: NVMe/RDMA Transport Seam. **Implemented locally; QA pending**
    Start the RoCE/NVMe-RDMA implementation track without making a false RoCE
    claim. The NVMe target now has an explicit transport selector and listener
    seam: TCP remains the default implemented path, RDMA returns a typed
    unsupported error at the target layer, and `blockvolume
    --nvme-transport=rdma` still refuses publicly until a real RDMA listener
    lands.
77. Phase 119: Mono RDMA Evidence And NVMe/RDMA Decision. **Closed
    2026-07-02, evidence decision**
    Use the current mono RDMA/VFS/RustVolume/NIXL work under
    `C:\work\rdma\seaweed-mono-rdma-refresh` as read-only evidence before
    adding more block RDMA code. That work proves real VFS/object acceleration
    and NIXL-shaped object compatibility, but it is not an NVMe-oF/RDMA target
    implementation. Phase 119 records the reusable components and performance
    evidence, keeps RoCE/NVMe-RDMA as a non-claim, and chooses the conservative
    next step: first run a block NVMe/TCP performance baseline to find the
    actual bottleneck.
78. Phase 120: NVMe/TCP Performance Baseline. **Active**
    Add a supported-lab Kubernetes PVC gate that measures the current NVMe/TCP
    path before any RoCE/NVMe-RDMA investment. The gate records sequential
    write/read MiB/s and small-write IOPS as baseline evidence only, with
    explicit non-claims for RoCE, NVMe/RDMA, performance SLO, GPU/cuObject,
    NIXL, broad compatibility, and published-image support. Any
    `publish_target=<ip>:4420` evidence is a TCP target address, not a
    RoCE/RDMA address.

The internal release-train contract is
`internal/docs/ref/operation-layer-v0.5-release-train.md`. Phases 41-44 close
the operation-layer loop for a bounded `SwBlockVolume` protection finalizer.
Phase 46 reused this same fact -> judgment -> action -> evidence pattern for
returned-replica reintegration before any larger storage feature is enabled.
Phases 47-54 close the executor-admission, preflight, status-schema, ACK
evidence, executor-contract, disabled executor-process bridge, and first
bounded ACK eligibility mutation without allowing frontend publication, rebuild
traffic, or failback. Phases 56-59 extend that same pattern to returned-replica
rebuild planning: contract, target CR, target owner, and planned status before
any real rebuild/catch-up traffic is enabled. Phases 60-65 prove and wire the
bounded rebuild/catch-up runtime path through terminal evidence. Phases 66-67
record ACK eligibility after terminal caught-up evidence. Phases 68-73 define
and then deliberately constrain frontend publication: the generic runtime seam
exists, but returned-replica publication remains blocked until a real
authority/failback owner is implemented and gated. Phase 74 names that missing
owner contract explicitly as `authority.failback_returned_replica` while
keeping execution disabled. Phase 75 adds the first target-owner seam for that
contract: `SwBlockReplicaFailback` can be planned as a handoff object, but no
failback, authority mutation, or frontend publication is executed. Phase 76 adds
the matching executor identity and status-only boundary for those failback
targets, still keeping all real failback side effects disabled. Phase 77 adds
the typed failback runtime contract and opt-in execution gate. Phase 78 adds
the first authority-owned failback seam through `Publisher.apply(IntentReassign)`.
Phase 79 wires that seam to the failback executor as an explicit-policy-gated
in-process call-site. Phase 80 exposes the corresponding master-owned factory
from the component that owns the live Publisher. Automatic deployed failback,
frontend publication, and storage/workload mutation remain disabled. Phase 81
adds the disabled-by-default RPC boundary that a separate failback executor can
call in a later phase; default installs still keep the mutation path off.
Phase 82 adds that executor-side gRPC runtime transport, still requiring
explicit execution policy and an explicit blockmaster address. Phase 83 packages
that path in Helm while preserving the default-off boundary and adding render
guardrails for incoherent execution values. Phase 84 closes the local
fake-service gap by proving the executor gRPC runtime against the real
blockmaster service and master Publisher. Phase 85 proves the deployed-loop
safety invariant: explicit execution flags do not cause runtime calls without a
valid executable target. Phase 86 removes the stale coupling that made the gRPC
runtime path require an unrelated HTTP target endpoint. Phase 87 aligns the
public/internal docs with that source-gated state. Phase 88 packages the
complete failback target-owner/executor/RPC suite behind explicit Helm values
and schema coverage, while still deferring the live automatic failback claim to
a later Kubernetes smoke. Phase 89-95 add the current-authority facts,
activation policy, executor handoff, multi-volume isolation, deployed render,
real blockmaster gRPC smoke, and finally a live Kubernetes smoke that writes
terminal `failed_back` evidence. Phase 96 consumes that terminal evidence to
create a disabled frontend-publication target. Phase 97 wires the
explicit-policy executor call-site for that target and proves terminal
frontend-publication evidence. Phase 98 closes the deployed user-visible loop:
product-owned frontend publication after failback is followed by workload
writer/reader verification and zero-residue cleanup.

The practical rule is:

```text
Do not add a new data-plane feature if the product cannot yet explain who owns
the lifecycle action, what evidence authorizes it, how it is blocked, and where
the user sees the result.
```

## Future Read-Write Control Plane

The operation layer should evolve from status-only and bounded finalizer
mutation into broader read-write behavior in stages, not as one large
"operator does everything" jump.

Current proven boundary:

- CSI owns `SwBlockVolume` identity/spec creation.
- operator-status owns `.status` and Events.
- lifecycle-owner owns only the Seaweed Block protection finalizer.
- cleanup, repair, rebuild, failback, backup, and upgrade execution are not
  automatic operator actions.

The next read-write stages should be ordered by blast radius:

1. `safe_k8s`: repair only Seaweed Block-owned Kubernetes objects, with
   admission/RBAC confinement and terminal evidence.
2. `host_cleanup`: clean stale iSCSI/multipath/hostPath residue through a
   node-scoped executor, never through operator-status.
3. `authority_mutating`: productize returned-replica rebuild/reintegration and
   failback with fencing, frontier evidence, and multi-volume isolation.
4. `data_lifecycle`: snapshot, backup, and restore after the consistency and
   identity model is explicit.

The design reference is
[`wiki/deep-dives/read-write-control-plane-roadmap.md`](wiki/deep-dives/read-write-control-plane-roadmap.md).
The product rule remains: a mutating action needs live facts, preconditions,
an owner executor, an admission/RBAC boundary, user-visible status, and QA
evidence before it can be enabled.

## Future GPU Data Paths

GPU-oriented storage should be treated as a future design train, not as a
small flag on the current PVC path. There are three different tracks:

1. `cuFile` over a mounted Seaweed Block PVC: first prove API compatibility
   and byte-correct reads/writes on a supported Linux GPU node.
2. `cuObject` / object path: design a SeaweedFS object/S3-style GPU API with
   its own consistency, auth, and commit semantics.
3. Protocol-level GPU/RDMA/NVMe path: investigate only after the file/object
   claims are separated and the fencing/backpressure model is clear.

The first acceptable claim is narrow:

```text
On one supported Linux GPU node, a pod can use cuFile against a mounted
Seaweed Block PVC and verify byte-correct data transfer with documented
environment prerequisites.
```

It must not claim broad GPUDirect acceleration, zero-copy, multi-node failover,
or object API support until the evidence proves those separately. The design
reference is
[`wiki/deep-dives/gpudirect-cufile-cuobject.md`](wiki/deep-dives/gpudirect-cufile-cuobject.md).

## Future Non-Kubernetes Adapters

Kubernetes CSI remains the primary product surface. A Docker integration is
possible later, but it should be scoped as a Docker Volume Plugin, not a Docker
graph/storage driver.

Potential Docker Volume Plugin path:

- `docker volume create -d seaweed-block ...`
- local attach/mount/unmount through a small node agent or local daemon
- reuse ManagedVolume status, cleanup verification, support bundles, and
  negative-first evidence
- target single-node Docker, lab, edge, or developer workflows first

Explicit non-goals for the Docker path:

- no Docker graph/storage driver to replace `overlay2`
- no broad Docker Swarm HA claim before the K8s lifecycle model is stable
- no production failover claim without fencing, cleanup, and residue gates

Windows Docker Desktop support is an investigation item, not a current claim.
The likely path is a Linux Docker Desktop VM or WSL2 backend running the
Seaweed Block node component, with Windows accessing the mounted volume through
Docker. Native Windows block-device attachment is out of scope until the Linux
control-plane path is mature.

## Availability And Recovery Follow-Ups

1. Multi-node Kubernetes test.

   - at least two nodes
   - non-loopback frontend target
   - dynamic PVC attach from a pod on another node

2. Failover while mounted.

   - pod writes
   - primary dies
   - authority moves
   - frontend reconnect path is validated

3. Productized returned replica lifecycle.

   - observed replica returns after promotion
   - returned replica stays frontend-fenced until recovery evidence is current
   - candidate -> syncing/rebuilding -> ready is visible in CRD/report/dashboard
   - ready status gates placement/ACK eligibility
   - rebuild/reintegration/failback action owner is explicit and audited
   - current authority facts are visible in `SwBlockVolume.status`
     (`primaryReplicaID`, `publishTarget`, `authorityEpoch`) before any
     failback activation path consumes them
   - failback handoff targets carry `expectedCurrentReplicaID` and
     `expectedCurrentEpoch` from volume status, while failback execution and
     frontend publication remain separate opt-in gates
   - failback target activation is explicit and default-off: policy plus runtime
     endpoint are required before a target can be stamped `enabled`
   - target-owner to executor handoff is locally gated: expected-current
     authority facts reach the runtime request and terminal evidence controls
     `failed_back`; live deployed failback remains a separate release gate
   - multi-volume handoff isolation is gated before live runtime testing, so
     expected-current facts and target addresses cannot silently cross volumes
   - the full opt-in failback suite renders coherently and the executor can call
     a real blockmaster gRPC FailbackService in local test; live Kubernetes PVC
     failback remains a separate release gate

4. Flow-control and pressure behavior.

   - pin too slow
   - WAL retention pressure
   - sync/full-ack unavailable policy

## Protocol And Backend Roadmap

1. iSCSI and NVMe-oF are the current release-gated frontends.

   iSCSI remains the default path for broad compatibility. NVMe-oF is now
   covered by ANA-aware direct-host multipath/failover, CSI protocol selection,
   a supported-lab Kubernetes CSI NVMe multipath attach gate, and Phase 101
   path-status/stage-unstage/bounded-soak hardening. Broader host
   compatibility, RoCE, performance/SLO, and long-soak claims remain future
   work. Mono SeaweedFS RDMA/VFS/object evidence is tracked separately from
   Seaweed Block NVMe/RDMA because RDMA memory movement and NVMe-oF/RDMA host
   initiator compatibility are different protocol problems.

   The runtime should be managed as a cluster of explicit mini-protocols:
   authority/epoch, replication recovery, iSCSI, NVMe, CSI lifecycle, and
   TestOps run-control. Each mini-protocol needs structured status and tests;
   avoid a single large protocol abstraction until repeated transition logic
   proves it is worth extracting. See `runtime-state-machines.md`.

2. CSI dispatch is protocol-aware.

   The CSI layer dispatches by frontend target protocol:

   ```text
   protocol=iscsi -> iscsiadm path
   protocol=nvme  -> nvme connect path
   ```

3. `walstore` remains the MVP backend.

   `smartwal` should be introduced behind an explicit gate with the same K8s
   scenarios, not silently switched into the MVP.

## Contributor-Friendly Work Items

Good first technical areas:

- improve Kubernetes manifests and docs
- add TestOps scenarios
- improve logs and diagnostic collection
- add protocol-neutral CSI target dispatch tests
- reduce `g7-debug` log noise behind a flag

Requires deeper storage context:

- recovery/failover semantics
- WAL retention and flow control
- replica reintegration
- multi-node authority/publisher changes
- broader V2 parity and long-running protocol regression coverage

## Definition Of Beta

A reasonable beta bar:

- K8s install path is a Helm chart with documented values and immutable images.
- Dynamic PVC create/delete works repeatedly.
- Volume data survives blockvolume pod restart.
- Multi-node attach works.
- Basic failover is tested under an attached workload.
- Read-only status and support evidence are available without SSH log spelunking.
- Negative and stale states are surfaced without false Ready claims.
- Operator lifecycle is either present behind a clear beta gate or explicitly
  listed as the next release boundary.
- TestOps can run the protocol release gate and produce stable artifacts.
- Non-claims are documented and visible to users.

Detailed post-alpha execution planning is kept in `internal/docs/` so the
public roadmap stays focused on user-visible behavior and non-claims.
