# Current Plan: Phase 116 NVMe/TCP Supported-Lab Release Claim Packaging

Status: planned.

Phase 115 is closed:

```text
QA run: 20260630-123456-c28b
Result: 25/25 PASS
Sign-off: internal/docs/qa-assignments/phase115-nvme-k8s-multivolume-mounted-path-churn-soak-qa-signoff.md
```

## Why This Is Next

Phases 100-115 moved NVMe/TCP from a basic CSI protocol option to a
supported-lab storage path with multipath attach, status evidence, path-loss
honesty, mounted I/O survival, restore, multi-volume isolation, and bounded
multi-volume churn.

The next risk is not another small path-loss gate. The risk is product wording:
README, roadmap, and release notes can easily over-claim RoCE, production HA,
performance, broad compatibility, or unbounded churn. Phase 116 packages the
evidence into a clear supported-lab release claim that users and PM can
understand.

## Product Goal

Produce a user-facing NVMe/TCP claim boundary:

- what works today;
- what was validated live;
- what remains a non-claim;
- what image/pinned-release smoke must pass before release marking;
- where developers can find the detailed Phase 100-115 evidence.

## Deliverables

1. README feature/status update.

   Add NVMe/TCP as an optional supported-lab frontend, distinct from the default
   iSCSI path. Mention dynamic PVC, RF=2 multipath attach, mounted path
   loss/restore, multi-volume isolation, and bounded churn.

2. Documentation release note.

   Add a concise NVMe/TCP supported-lab section under `docs/` or
   `internal/docs/`, referencing the Phase 100-115 QA evidence and listing
   exact non-claims.

3. Release smoke checklist.

   Define the image-publish smoke for matching `seaweed-block` and
   `seaweed-block-csi` images. This should not require re-running every Phase
   100-115 gate, but must prove the shipped image pair can run the representative
   Day-1 NVMe/TCP multipath path.

4. Roadmap consistency.

   Ensure `docs/roadmap.md`, `internal/docs/product-roadmap.md`, and README
   agree on the same claim boundary.

## Verification

Local:

```text
go test ./core/csi ./core/frontend/nvme ./cmd/blockvolume ./core/ops
```

Docs:

```text
rg "NVMe|RoCE|performance|production HA|supported-lab" README.md docs internal/docs
```

Gate:

No new live storage behavior is required for Phase 116 unless the docs add a
new claim. If wording adds a stronger claim, add or reuse a matching TestOps
gate before closing.

## Non-Claims

Phase 116 does not implement RoCE/NVMe-RDMA, performance/SLO, broad distro or
kernel compatibility, production HA, node-loss survival, backup/restore, or
unbounded arbitrary path churn. It packages the evidence already closed by
Phases 100-115.
