# Phase 96 QA Sign-off: Failback -> Frontend Publication Target

Verdict: PASS.

Source: local working tree after Phase 96 implementation.

Runner:

```text
swblock run testops/scenarios/failback-frontend-publication-target-chain.yaml
run=20260626-154640-206b
result=PASS 16/16
```

## Gate Result

| Check | Result |
|---|---|
| Core ops tests | PASS |
| CLI tests | PASS |
| Scenario validation | PASS |
| Terminal failback creates frontend target | PASS |
| Non-terminal failback rejected | PASS |
| Executor accepts failback-source target as disabled | PASS |
| Frontend publication attempts | 0 |
| Failback attempts | 0 |
| Storage mutation allowed | false |

## Evidence

```text
phase96_failback_frontend_publication_target_status=ok
phase96_scope=failed_back_terminal_evidence_to_frontend_publication_target
core_ops_phase96_tests=pass
cmd_sw_block_phase96_tests=pass
frontend_publication_target_schema_accepts_failback_source=true
frontend_publication_target_owner_reads_failbacks_only=true
frontend_publication_writer_camel_case=true
terminal_failed_back_creates_frontend_publication_target=true
non_terminal_failback_rejected=true
executor_accepts_failback_target_as_disabled=true
cmd_terminal_failback_creates_target=true
terminal_failback_state_required=failed_back
terminal_failback_reason_required=failback_completed
publish_target_swapped_after_failback_required=true
frontend_publication_target_created_from_failback=true
frontend_publication_target_source_failback_name=true
frontend_publication_decision=disabled
frontend_publication_reason=frontend_publication_policy_disabled
frontend_publication_mutation_allowed=false
frontend_publication_status_writes_allowed=false
frontend_publication_executor_default_off=true
frontend_publication_attempts=0
failback_attempts=0
failback_status_mutation_allowed=false
storage_mutation_allowed=false
```

## Boundary

The gate proves a disabled `SwBlockFrontendPublication` target can be created
from terminal `SwBlockReplicaFailback` evidence.

It does not claim:

- frontend publication execution;
- workload-visible data-path switch;
- failback re-entry;
- storage mutation.

Those require later gates.

## Notes

The local Bash gate cannot be run from the Windows WSL shell because that shell
still has Go 1.18 and the module requires a newer Go toolchain. The same script
passed through the runner on m02, where the current Go toolchain is available.
