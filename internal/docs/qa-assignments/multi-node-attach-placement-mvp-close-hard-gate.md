# QA Hard Gate: Multi-Node Attach And Placement MVP

## Verdict Rule

This is a strict gate. Any `FAIL` blocks plan close.

The plan does not claim remote-node attach. The expected close claim is:

```text
On the supported alpha Kubernetes path, RF=1 iSCSI PVCs use same-node attach:
the generated blockvolume and writer/reader app pods are pinned to the same
selected Kubernetes node, data passes through the normal CSI path, and inventory
explains PVC, node, frontend, status endpoint, and support-bundle ownership.
```

## Required Runs

Run both scenarios from the assignment:

- `testops/scenarios/same-node-alpha-attach-chain.yaml`
- `testops/scenarios/same-node-alpha-attach-negative-chain.yaml`

Use:

```text
internal/docs/qa-assignments/same-node-alpha-attach-validation.md
```

## HG Clauses

### HG-0 Documentation Entry

Pass:

- `docs/operations-v1.md` describes same-node alpha attach.
- `docs/quickstart-kubernetes.md` describes same-node alpha attach.
- Both docs explicitly state that remote-node attach to loopback frontends is
  not claimed.

Fail:

- Docs still imply generic multi-node or remote attach works.

### HG-1 Contract And Audit Present

Pass:

- `internal/docs/ref/multi-node-attach-placement-audit.md` exists and names the
  loopback/frontend constraint.
- `internal/docs/ref/same-node-alpha-placement-contract.md` exists and names
  same-node RF=1 attach as the supported model.

Fail:

- Contract is missing, or contradicts the docs/scenarios.

### HG-2 App Node Pinning Is Default

Pass:

- `scripts/run-alpha-app-demo.sh` logs `app_node=<node>`.
- It logs `pin_app_node=1` by default.
- `demo/demo-app.rendered.yaml` contains a Pod `nodeSelector`.
- `demo/demo-app-reader.rendered.yaml` contains a Pod `nodeSelector`.

Fail:

- Writer/reader pods can land on arbitrary nodes on the default happy path.

### HG-3 Blockvolume Placement Evidence

Pass:

- `demo/generated-blockvolume.yaml` contains
  `kubernetes.io/hostname: <node>`.
- It contains `--iscsi-listen=127.0.0.1:<port>`.
- The app node from `run.log` matches the inventory replica `node=<node>`.

Fail:

- Blockvolume node is not visible, or app/blockvolume node alignment is not
  proven.

### HG-4 Functional Attach

Pass:

- Happy-path scenario reaches `pass`.
- `writer.log` contains writer checksum success.
- `reader.log` contains reader checksum success.
- The scenario stops after reader verification and collects live inventory.

Fail:

- It only proves manifest rendering without actual writer/reader I/O.

### HG-5 Inventory Explains Ownership

Pass:

- `ops-inventory-reader-verified/volume-inventory-summary.txt` contains:
  - `pvc=sw-block-demo-pvc`
  - `node=<same app node>`
  - `frontend=127.0.0.1:<port>`
  - `support_bundle=volumes/<volume>/r1`
- `nested-ops-status-bundles.json` contains
  `"command": "sw-block ops status"`.

Fail:

- Inventory lacks PVC, node, frontend, or support-bundle evidence.

### HG-6 Negative Fixture Is Actionable

Pass:

- Negative scenario reaches `pass`.
- It exits the demo with code `45`.
- `unsupported-cross-node-loopback-attach.txt` contains:
  - `issue=unsupported_cross_node_loopback_attach`
  - `app_node=sw-block-not-the-blockvolume-node`
  - `blockvolume_node=<actual node>`
  - `frontend=127.0.0.1:<port>`
  - reason text saying loopback requires same-node placement.
- It also writes an inventory bundle under
  `ops-inventory-unsupported-placement/`.

Fail:

- The negative path becomes a generic timeout, ImagePullBackOff, or unschedulable
  pod failure without the explicit issue class and inventory bundle.

### HG-7 Cleanup Hygiene

Pass after both runs:

- no active iSCSI sessions,
- no iSCSI node DB entry for Seaweed Block,
- no `blockmaster`, `blockvolume`, `blockcsi`, `iscsi-target` processes,
- no blockmaster `kubectl port-forward`,
- no `app=sw-blockvolume` Deployments.

Fail:

- Any active session/process/port-forward/generated Deployment remains.

### HG-8 Fast Tests Pin The Contract

Pass:

```bash
go test ./core/launcher ./core/ops -count=1
```

must pass, and include tests for:

- renderer same-node loopback fields,
- inventory node/frontend/status/support-bundle evidence.

Fail:

- Only the live scenario pins behavior.

### HG-9 Non-Claims Honest

Pass:

Docs and support text do not claim:

- remote-node attach,
- node-loss survival,
- blockvolume rescheduling,
- RF=2/RF=3 live Kubernetes lifecycle,
- failover while mounted,
- rebuild,
- performance SLOs,
- production operator behavior.

Fail:

- Any user-facing doc implies those are supported by this plan.

## Close Report Template

QA should write:

```text
QA Close — Multi-Node Attach And Placement MVP

Verdict: PASS|FAIL

Product commit:
Runner commit:

Happy run:
Negative run:

HG-0 documentation entry                  PASS|FAIL
HG-1 contract and audit present           PASS|FAIL
HG-2 app node pinning default             PASS|FAIL
HG-3 blockvolume placement evidence       PASS|FAIL
HG-4 functional attach                    PASS|FAIL
HG-5 inventory explains ownership         PASS|FAIL
HG-6 negative fixture actionable          PASS|FAIL
HG-7 cleanup hygiene                      PASS|FAIL
HG-8 fast tests pin contract              PASS|FAIL
HG-9 non-claims honest                    PASS|FAIL

Blocking findings:
Non-blocking findings:
Residue audit:
```
