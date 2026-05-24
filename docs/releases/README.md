# Seaweed Block Release Notes

Seaweed Block release notes use narrow product claims. Each alpha version names
what a user or reviewer can actually run, what QA evidence backs it, and what
is still outside the supported boundary.

- [v0.1 Alpha — Kubernetes Block Foundation](v0.1-alpha.md)
- [v0.2 Alpha — Day-1 Activation And First Volume](v0.2-alpha.md)
- [v0.3 Alpha — Helm Install And Observable First Volume](v0.3-alpha.md)
- [v0.3.1 Alpha — Helm Lifecycle Hardening](v0.3.1-alpha.md)
- [v0.3.2 Alpha — Multi-Volume HA Independence](v0.3.2-alpha.md)
- [v0.3.3 Alpha — Productized Operations And Read-Only Operator Foundation](v0.3.3-alpha.md)

## Version Boundary

- `v0.1-alpha` is the engineering alpha foundation: CSI/PVC plumbing,
  product-owned blockvolume lifecycle, inventory, recovery gates, iSCSI
  ALUA/dm-multipath proof, node-loss recovery, and read-only control-plane
  evidence.
- `v0.2-alpha` is the first user-facing activation loop: one documented
  Kubernetes path from install to first PVC, writer/reader verification, local
  status report, and cleanup.
- `v0.3-alpha` is the Helm-first alpha loop: generated chart values, Helm
  install, first PVC, writer/reader verification, local read-only
  report/dashboard, and cleanup verification.
- `v0.3.1-alpha` hardens the Helm alpha loop with chart hygiene, a narrow
  upgrade/rollback smoke, three-PVC Day-1 smoke, and cold support-bundle
  replay.
- `v0.3.2-alpha` adds scenario-gated RF=3 multi-volume HA independence:
  three PVC-backed volumes, per-volume CSI reattach recovery, mounted
  transparent failover, interleaved failover isolation, and cleanup hardening.
- `v0.3.3-alpha` is the current Phase 28 packaging target for productized
  operations: stable ManagedVolume vocabulary, CRD/Condition/Event contract,
  read-only operator snapshot, report/dashboard alignment, and close-gate QA.
  D12 close passed on 2026-05-24; immutable GHCR image pins are recorded in
  the v0.3.3 release note.

Mutating operator packaging is not included in these alphas. v0.3 introduces
Helm as the preferred Kubernetes alpha install path; the script path remains
available for development and fallback validation. The first operator-facing
surface is read-only/status-first.
