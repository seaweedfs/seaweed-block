# QA Validation - Phase 29 D4 Deterministic Cleanup

Verdict: **PASS**

Date: 2026-05-24

Validated source commit: `205b103 docs: assign phase29 cleanup QA`
Branch carries the three required commits:
- `1d4e53c scripts: fix multi-volume cleanup race`
- `f0f57ec docs: define phase29 cleanup ownership`
- `102fc74 ops: surface cleanup evidence in reports`

## Run Summary (independent QA replay)

| Scenario | QA run ID | Result | Dev baseline |
|---|---:|---|---|
| `helm-multi-volume-rf3-readiness-chain.yaml` | `20260524-152543-25d9` | 35/35 PASS | `20260524-144856-f4b3` |
| `helm-multi-volume-rf3-reattach-recovery-chain.yaml` | `20260524-152815-3a6d` | 29/29 PASS | `20260524-145058-6289` |
| `helm-multi-volume-rf3-mounted-failover-chain.yaml` | `20260524-153215-0d1e` | 48/48 PASS | `20260524-145513-41d0` |
| `helm-multi-volume-rf3-interleaved-failover-chain.yaml` | `20260524-153618-bf3c` | 56/56 PASS | `20260524-145901-6d11` |
| `cleanup-residue-chain.yaml` | `20260524-153905-b0ca` | 13/13 PASS | `20260524-150146-f4e5` |

**Total: 181/181 actions, matching dev baseline exactly.**

Result bundles:

```text
C:\work\seaweed_block\results\phase29-d4-deterministic-cleanup-qa\<run-id>\
```

Artifact share roots:

```text
/v/share/g15d-k8s/<run-id>-<scenario>/...
```

## Pass-Criteria Compliance

### Residue requirements - PASS

Direct host audit after the full 5-scenario QA cycle:

```text
helm release sw-block:                  none
iSCSI active sessions:                  none
iSCSI nodes DB (io.seaweedfs):          none (no seaweed nodes)
sw-block dm-multipath / dmsetup:        empty / No devices found
sw-block pods + Deployments + StorageClass + CSI driver: none
per-host product processes (m01/m02/tp01): none
```

### Evidence vocabulary - PASS

All 5 scenarios' `cleanup-summary.txt` files use the Phase 29 field
vocabulary. Sample (`20260524-152543-25d9-helm-multi-volume-rf3`):

```text
cleanup_status=ok
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

`cleanup-summary.txt` itself does not emit `iscsi_residue_count`
explicitly (it carries `iqn_substr` as the filter and is paired with the
`assert_no_active_iscsi_sessions` runner check). The report-side renderer
synthesizes `iscsi_residue_count=0` when the cleanup-summary is clean - see
"Carry-through to report/dashboard/operator snapshot" below.

### Carry-through to report/dashboard/operator snapshot - PASS

Direct test: ran `sw-block ops report --from-bundle
/mnt/smb/work/share/g15d-k8s/20260524-152543-25d9-helm-multi-volume-rf3
--out /tmp/p29-report`. The output bundle carries cleanup evidence on all
three required surfaces.

**summary.txt** (new fields surfaced):

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
failure_count=0
cleanup_evidence=/mnt/smb/work/share/g15d-k8s/20260524-152543-25d9-helm-multi-volume-rf3/cleanup/verify/cleanup-summary.txt
```

**dashboard index.html** carries:

```html
<section><h2>Lifecycle Cleanup</h2>
  <table>
    <thead><tr>
      <th>Status</th><th>K8s</th><th>iSCSI</th><th>Multipath</th>
      <th>Processes</th><th>HostPath</th><th>Failures</th><th>Evidence</th>
    </tr></thead>
    <tbody><tr>
      <td class="ok">ok</td><td>0</td><td>0</td><td>0</td>
      <td>0</td><td>0</td><td>0</td>
      <td>/mnt/smb/.../cleanup/verify/cleanup-summary.txt</td>
    </tr></tbody>
  </table>
</section>
```

**operator-snapshot.json** carries:

```json
"cleanup": {
  "status": "ok",
  "evidence_ref": "/mnt/smb/.../cleanup/verify/cleanup-summary.txt"
}
```

All three surfaces use the same vocabulary defined in
`internal/docs/ref/phase29-lifecycle-evidence-contract.md`. The
operator-snapshot's `cleanup` block stays within the read-only contract -
no mutating cleanup action is exposed; `non_claims` still includes
`no_cleanup_mutation`.

### Scenario summary cleanliness - PASS

No scenario reported ambiguous cleanup status. Every `cleanup-summary.txt`
shows `cleanup_status=ok`, all residue counts 0, `failure_count=0`. No
`failed_phase=` line appeared in any of the 5 QA runs.

## Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| All 5 scenarios PASS | PASS (181/181 actions, matches dev baseline) |
| No active `io.seaweedfs` iSCSI sessions after run | PASS |
| No matching iSCSI node records | PASS |
| No Seaweed Block dm-multipath or dmsetup residue | PASS |
| No sw-block pods / deploys / Helm release / StorageClass / CSI / RBAC | PASS |
| No `blockmaster`/`blockvolume`/`blockcsi`/`iscsi-target` host processes | PASS |
| `cleanup-summary.txt` uses Phase 29 vocabulary | PASS |
| Report bundle (when produced) carries same cleanup evidence in `summary.txt` | PASS |
| Dashboard HTML carries same cleanup evidence | PASS |
| `operator-snapshot.json` carries same cleanup evidence | PASS |
| Operator snapshot remains read-only on cleanup contract | PASS |

## Blocking Findings

**None.**

## Non-Blocking Findings

### N1: `cleanup-summary.txt` does not emit `iscsi_residue_count` field directly

The contract in
`internal/docs/ref/phase29-lifecycle-evidence-contract.md` lists
`iscsi_residue_count` as a required field on the `cleanup summary` surface.
The actual `cleanup-summary.txt` emitted by `verify-helm-cleanup.sh` does
not include this field name; it includes `iqn_substr=io.seaweedfs` as the
filter and relies on the runner's `assert_no_active_iscsi_sessions` to
gate iSCSI residue.

The report-side renderer synthesizes `iscsi_residue_count=0` correctly
when cleanup-summary indicates a clean run. So the field is present on the
report/dashboard/operator-snapshot surfaces, just not in the helper's
cleanup-summary itself.

Fix shape (carry into a small `verify-helm-cleanup.sh` patch):

```bash
echo "iscsi_residue_count=$ISCSI_RESIDUE_COUNT"
```

where `ISCSI_RESIDUE_COUNT` is computed from `iscsiadm -m session | grep
-c io.seaweedfs` plus `iscsiadm -m node | grep -c io.seaweedfs` after
cleanup.

Not blocking because:
- The runner's own `assert_no_active_iscsi_sessions` enforces zero iSCSI
  residue independently.
- The report/dashboard/operator-snapshot surfaces all show
  `iscsi_residue_count=0`.

Resolution in D5 hardening:

- `scripts/verify-helm-cleanup.sh` now emits `iscsi_residue_count` directly.
- Quick validation: `cleanup-residue-chain.yaml` run
  `20260524-215539-4285`, PASS, 13/13 actions.

## Classification of Failures

None observed across the 5 QA scenarios.

## Recommendation

**Phase 29 D4 is ready to close.** Recommend writing the D5 close
report and finished plan with this validation as the QA evidence. The
non-blocking N1 finding can either be deferred or rolled into the D5
hardening pass.

QA validation report committed to:
`internal/docs/qa-assignments/phase29-deterministic-cleanup-qa-validation.md`
