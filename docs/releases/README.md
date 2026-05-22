# Seaweed Block Release Notes

Seaweed Block release notes use narrow product claims. Each alpha version names
what a user or reviewer can actually run, what QA evidence backs it, and what
is still outside the supported boundary.

- [v0.1 Alpha — Kubernetes Block Foundation](v0.1-alpha.md)
- [v0.2 Alpha — Day-1 Activation And First Volume](v0.2-alpha.md)
- [v0.3 Alpha — Helm Install And Observable First Volume](v0.3-alpha.md)

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

Operator packaging is not included in these alphas. v0.3 introduces Helm as
the preferred Kubernetes alpha install path; the script path remains available
for development and fallback validation.
