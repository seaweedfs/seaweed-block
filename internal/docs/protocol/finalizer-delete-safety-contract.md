# Finalizer Delete Safety Contract

Status: Phase 39 D1 contract.

## Goal

Define the first bounded mutating operator behavior before implementing it:
`SwBlockVolume` finalizer/delete safety.

The operator may eventually patch only:

```text
SwBlockVolume.metadata.finalizers
SwBlockVolume.status
Kubernetes Events
```

It must not delete or mutate PVCs, PVs, Pods, Deployments, StorageClasses,
Helm releases, images, iSCSI state, multipath state, dmsetup devices, hostPath
data, replica authority, rebuild/failback state, backup, or restore state.

## Finalizer

```text
block.seaweedfs.com/swblockvolume-protection
```

The finalizer protects the Seaweed Block lifecycle object from disappearing
while evidence says deletion is unsafe or unknown.

It does not claim ownership of the user PVC lifecycle. PVC finalizers are out of
scope for Phase 39.

## Action Contract

Finalizer release is represented as an action decision using the Phase 38
vocabulary:

```text
action_type=safe_k8s.release_swblockvolume_finalizer
decision=allowed|rejected
```

The action is allowed only when the delete state is `releasable`. Missing
cleanup evidence, residue, or no active delete request rejects the action.

## Delete States

| State | Meaning |
|---|---|
| `not_requested` | No delete request is active. |
| `requested` | Delete request exists; evidence has not yet been classified. |
| `blocked` | Delete was requested, but cleanup/evidence is missing or unsafe. |
| `releasable` | Delete was requested and cleanup evidence is clean. The operator may remove the finalizer. |
| `released` | Finalizer removal has been observed or emitted as an event. |

## Required Facts

The delete decision requires:

```text
identity.volume_id
identity.pvc_name
identity.pv_name
kubernetes.swblockvolume.deletion_timestamp
cleanup.status
cleanup.k8s_residue_count
cleanup.iscsi_residue_count
cleanup.multipath_residue_count
cleanup.process_residue_count
cleanup.hostpath_residue_count
```

If cleanup evidence is missing, the safe decision is:

```text
state=blocked
decision=rejected
reason=cleanup_evidence_missing
safe_next_action=observe.verify_cleanup
```

If residue evidence exists, the safe decision is:

```text
state=blocked
decision=rejected
reason=<verifier reason or cleanup_required>
safe_next_action=observe.verify_cleanup
```

If cleanup evidence is clean, the safe decision is:

```text
state=releasable
decision=allowed
reason=finalizer_releasable
```

## Evidence

The release evidence is:

```text
cleanup-summary.txt with cleanup_status=ok and all residue counts 0
```

Blocking evidence may include:

```text
cleanup-summary.txt
iscsi node/session residue
multipath maps
dmsetup devices
Kubernetes generated resources
hostPath residue
process residue
```

## Non-Claims

Phase 39 D1 does not implement mutation. Later D3-D5 may implement bounded
finalizer mutation, but still do not claim:

- PVC finalizer ownership,
- automatic cleanup execution,
- PV/PVC deletion,
- Pod/Deployment deletion,
- iSCSI or multipath mutation,
- hostPath deletion,
- promotion/fencing/rebuild/failback,
- backup/snapshot/restore,
- NVMe ANA lifecycle.

## Review Rule

Any future implementation must keep this invariant:

```text
The operator can release a SwBlockVolume finalizer only after the delete-safety
decision is releasable. If evidence is missing or residue remains, deletion is
blocked with status, reason, event, evidence refs, and a non-mutating next step.
```
