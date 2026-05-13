# Current Plan: Basic Mounted Failover And Reattach MVP

Status: active, opened after closing
`finished-plans/phase14_finishedplan_multi_node_attach_and_placement_mvp.md`,
0% implementation.

QA needed now: no. First dev slice is D1/D2 audit and contract. Ask QA after a
runner-native gate exists, or earlier only if the audit finds ambiguous product
behavior that needs independent scenario design.

## Product Question

Can an early Kubernetes user recover from a primary `blockvolume` failure while
an app is using a PVC, and understand the recovery through inventory/support
bundles without reading internal logs?

The last plans proved the light-use path:

```text
first volume -> cluster inventory -> product-owned lifecycle -> durable
restart/reattach -> same-node placement/attach
```

This plan moves the next visible beta gap:

```text
PVC writes data -> primary blockvolume fails -> authority moves or recovery is
refused safely -> app reattaches/restarts -> data is still readable -> inventory
explains the timeline
```

The narrow target claim is:

```text
On a supported alpha Kubernetes lab, Seaweed Block can run an RF=2 failover
exercise for an iSCSI PVC, preserve previously acknowledged data across a
controlled primary failure, reattach the workload through the documented path,
and publish enough status/inventory evidence to explain the authority epoch,
old primary fencing, new primary, and any non-claim.
```

This is not a production HA claim. It does not claim transparent zero-disruption
I/O, arbitrary node loss, automatic cross-node scheduling, RF=3 live Kubernetes
operation, full quorum durability, rebuild completion, upgrade safety,
performance SLOs, or UI.

## Why This Is Next

The product is now usable enough that the next user question is availability:

```text
What happens when the backing blockvolume dies while my pod is mounted?
```

We should not jump directly to broad HA. Existing protocol gates already proved
important pieces:

- iSCSI ALUA failover and mounted failover are release-gated in runner-native
  chains.
- NVMe multipath failover is release-gated separately.
- Durable restart/reattach is closed for the RF=1 Kubernetes path.
- Same-node placement and inventory now make endpoint ownership visible.
- Returned-replica component and iSCSI returned-replica chains exist, but the
  user-facing Kubernetes recovery loop is not yet a product claim.

The next plan should connect these pieces through the app/PVC path, while being
strict about what is actually guaranteed.

## Current Honest State

What already works:

- A generated RF=1 iSCSI PVC can be used by an app pod under documented
  same-node loopback placement.
- Two live PVCs can coexist on the same alpha node with distinct ports.
- Product-owned lifecycle creates generated blockvolume Deployments.
- Durable hostPath restart preserves data for the RF=1 path.
- `sw-block ops inventory` exposes PVC/PV/workload/node/frontend/status/support
  bundle evidence.
- Protocol-level iSCSI and NVMe failover chains exist and are green in prior
  gates.

What is still weak or unknown:

- Live RF=2/RF=3 Kubernetes lifecycle is still a non-claim.
- The alpha frontend remains loopback-oriented for the supported attach path.
- It is not yet proven that Kubernetes app write/read survives a controlled
  primary failure through the normal PVC path.
- We need to define exactly which write acknowledgements are protected in the
  MVP: best-effort, quorum, or full-ack.
- We need to prove stale-primary fencing and returned-replica behavior in the
  same evidence model users see, not only in component logs.
- A failing recovery must be safe and explainable; silent promotion is worse
  than an honest refusal.

## Scope

In scope:

- Audit current RF=2/RF=3 failover behavior across existing protocol gates,
  master assignment, launcher rendering, CSI attach, durable status, and
  inventory fields.
- Define the MVP recovery contract: what failure is injected, what ACK profile
  is claimed, what reattach means, and what must be fenced.
- Add fast tests for authority movement, stale-primary visibility, inventory
  failover fields, and support-bundle issue wording.
- Add a runner-native failover gate that exercises a PVC write/read path through
  a controlled primary failure or, if the current product cannot safely do that,
  proves the safe refusal with an actionable bundle.
- Add negative fixtures for unsafe promotion conditions, stale primary, missing
  durable evidence, or insufficient replica coverage.
- Update `docs/operations-v1.md` with failover usage, limitations, and support
  bundle interpretation.

Out of scope:

- Transparent I/O continuation without pod restart.
- Arbitrary Kubernetes node loss.
- Production HA.
- RF=3 live Kubernetes close unless the audit shows it is already safe enough
  to gate.
- Automatic rebuild completion and returned-replica reintegration as a broad
  claim.
- Performance, stress, upgrade, backup/restore, UI, or enterprise operations.

## Top Blocking Issues

### P0: ACK Profile Must Be Explicit

A user cannot trust a failover claim unless the product says what kind of writes
are protected.

Close requirement: the plan documents and gates the exact ACK profile used by
the scenario. If the current path is best-effort, the failover claim must be
limited to writes proven durable by evidence.

### P0: Stale Primary Must Not Be Ambiguous

After authority moves, the old primary must be fenced, degraded, or otherwise
reported as unsafe. It must not look like a valid writer.

Close requirement: inventory/support bundles expose old primary state, new
primary state, epoch/endpoint version, and an issue line when stale-primary
fencing is incomplete.

### P0: App-Path Recovery Must Be Real Or Honestly Refused

The product should either complete the controlled recovery through PVC
write/read, or fail closed with an explainable bundle.

Close requirement: the runner-native gate proves one of those outcomes with
real app data, not only internal PASS lines.

### P1: Returned Replica Must Have A Safe State Machine

Returned replicas should not become ready from heartbeat alone.

Close requirement: at minimum, the negative fixtures demonstrate that returned
or unplaced replicas are not promoted to healthy without the required durable
frontier/rebuild evidence.

## Deliverables

### D1: Failover Reality Audit

Read existing code, docs, and gates to answer:

- what RF=2/RF=3 paths already exist,
- how master promotion is triggered,
- how CSI attach/reattach behaves after a primary move,
- where authority epoch and endpoint version are recorded,
- how old-primary/stale-primary state is exposed,
- how returned-replica state is represented,
- what the current ACK profile really is.

Output: `internal/docs/ref/mounted-failover-reattach-audit.md`.

### D2: Recovery Contract

Add a reference contract describing:

- supported MVP topology,
- failure injection,
- ACK profile,
- expected authority transition,
- app reattach behavior,
- inventory/support-bundle fields,
- explicit non-claims.

Output: `internal/docs/ref/mounted-failover-reattach-contract.md`.

### D3: Fast Tests

Add or tighten tests for:

- authority epoch movement and stale-primary issue wording,
- inventory before/after failover fields,
- returned-replica not-ready wording,
- support-bundle timeline fields,
- safe refusal when required durable evidence is missing.

### D4: Runner-Native Happy Or Safe-Refusal Gate

Add a scenario that:

```text
pre_clean
install/build alpha path
create RF=2 PVC or the nearest supported failover fixture
write/check data before failure
inject controlled primary failure
observe authority move or safe refusal
restart/reattach app as required by the contract
read/check data after recovery
collect inventory/support bundle before cleanup
collect_and_cleanup(always)
```

If the current product cannot safely recover through the app path, the gate must
fail closed intentionally and assert the exact issue class.

### D5: Negative Fixtures

Add at least two focused fixtures:

- stale primary still visible after authority move,
- missing durable frontier / returned replica not eligible,
- insufficient replica coverage for the claimed ACK profile,
- endpoint unreachable after promotion.

The output should be an actionable inventory/support bundle.

### D6: Operations Manual Update

Update `docs/operations-v1.md` and related quickstart docs with:

- how to run the failover exercise,
- how to read before/after authority evidence,
- what app restart/reattach behavior is expected,
- what remains a non-claim.

### D7: QA Close Gate

Create a strict QA assignment that validates:

- runbook clarity,
- app-path write/read before and after failure or honest safe refusal,
- stale-primary/new-primary evidence,
- support-bundle self-explanation,
- cleanup hygiene,
- docs do not over-claim HA.

## Gates To Close

This plan closes only when:

1. ACK profile and topology are documented.
2. Fast tests cover authority/fencing/inventory wording.
3. A runner-native gate proves app-path recovery or intentional safe refusal.
4. Negative fixtures show unsafe states are visible and not promoted.
5. Inventory/support bundles explain the recovery timeline.
6. Cleanup leaves no active sessions, processes, port-forwards, or generated
   workloads.
7. QA validates independently and reports no blocking usability issue.

## Success Statement

After this plan, Seaweed Block can make a narrow beta-facing availability
statement:

```text
For the documented MVP topology and ACK profile, a controlled primary failure
is either recovered through the PVC app path with data verified after reattach,
or refused safely with a support bundle that explains exactly why recovery is
not yet claimed.
```
