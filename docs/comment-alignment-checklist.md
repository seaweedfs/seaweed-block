# Code Comment Alignment Checklist

This checklist is for cleaning code comments, script output, and public-facing
strings so they match the alpha documentation.

The goal is not to erase development history. The goal is to avoid confusing
external contributors with internal phase names, old gate labels, or comments
that imply stale semantics.

## Scope

Check these areas first:

- `cmd/blockmaster`
- `cmd/blockvolume`
- `cmd/blockcsi`
- `core/lifecycle`
- `core/authority`
- `core/host`
- `core/recovery`
- `core/replication`
- `core/frontend/iscsi`
- `deploy/k8s/alpha`
- `scripts/run-k8s-alpha.sh`

## Public-Facing Rule

Public-facing docs, manifests, CLI help, and alpha scripts should use product
language:

- alpha
- blockmaster
- blockvolume
- blockcsi
- lifecycle
- authority
- recovery
- iSCSI
- Kubernetes
- dynamic PVC

They should avoid internal planning labels such as gate names, phase numbers, or
temporary branch names.

## Internal Comment Rule

Internal comments may mention historical test gates only when the reference is
needed to explain a pinned invariant. Otherwise prefer the product concept.

Examples:

| Prefer | Avoid |
|---|---|
| dual-lane recovery | G7-redo path |
| recovery-aware WAL feeding | hardware-fix path |
| alpha Kubernetes smoke path | dynamic-gate path |
| authority publisher | product-loop gate |
| old-primary fail-closed | stale-fence gate |

## Semantic Checks

When updating comments, preserve these design facts:

- placement intent is not authority
- verified placement is not assignment
- authority moved is not data continuity proven
- frontend fact is not storage readiness
- recovery close is not target-LSN crossing
- one peer has one WAL-feeding decision owner
- old primary must fail closed after supersede
- CSI alpha path is iSCSI-first, not NVMe-oF yet
- current alpha storage uses `walstore`

Do not simplify comments in a way that loses these boundaries.

## Suggested Grep Pass

Run:

```bash
rg "P15|G15|G9|G8|G7|Phase 15|g7-debug|targetLSN|target LSN" README.md docs deploy scripts cmd core
```

Classify each hit:

- public-facing leak: fix now
- internal invariant reference: keep or rewrite to product language
- old debug log: put behind a flag or rename when touching that module
- stale semantic wording: fix with a small test if behavior is affected

## Done Criteria

This cleanup is done when:

- `README.md`, `docs/architecture.md`, `docs/roadmap.md`, and
  `deploy/k8s/alpha/README.md` contain no internal gate names
- alpha script output does not require internal history to understand
- code comments near lifecycle/authority/recovery use current vocabulary
- any kept internal references have a clear reason
- tests still pass for the touched packages

