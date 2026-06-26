# Phase 93 QA Sign-off: Failback Handoff Isolation

Status: pending QA.

## Scope

Validate multi-volume isolation for the failback target-owner -> executor
handoff.

## Gate

Run:

```text
bash scripts/run-phase93-failback-handoff-isolation-gate.sh .
swblock validate testops/scenarios/failback-handoff-isolation-chain.yaml
```

## Required Evidence

The summary must contain:

```text
phase93_failback_handoff_isolation_status=ok
go_test_core_ops_failback_handoff_isolation=pass
multi_volume_target_create_count=2
multi_volume_runtime_request_count=2
cross_volume_expected_current_mixup=false
cross_volume_target_addr_mixup=false
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
```

## Pass Criteria

- Two source volumes create two enabled targets.
- Executor makes two runtime requests.
- Each runtime request carries the matching volume's returned replica,
  expected-current replica, expected-current epoch, data address, and control
  address.
- No frontend publication or storage mutation is claimed.

## Non-Claims

This is not a live deployed failback test. It is the local isolation gate before
the live runtime smoke.
