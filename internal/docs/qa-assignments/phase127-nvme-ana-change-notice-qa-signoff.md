# Phase 127 QA Sign-Off: NVMe ANA Change Notice

Status: PASS on 2026-07-03.

## Scope

Phase 127 closes the source/component side of the NVMe ANA Change Notice gap.
It does not claim live Linux host AER behavior or Kubernetes dynamic reconnect.

Validated local gate and runner entry point:

```text
scripts/run-phase127-nvme-ana-change-notice-gate.sh
testops/scenarios/nvme-ana-change-notice-chain.yaml
```

The scenario syntax is valid and exists for subagent/QA execution. This
sign-off's PASS verdict is based on the source/component gate, not on live Linux
host AER evidence.

## Evidence

Local gate:

```text
phase127_nvme_ana_change_notice_status=ok
nvme_transport=tcp
host_live_aer_claim=false
k8s_dynamic_reconnect_claim=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
go_binary=go.exe
go_version=go version go1.25.6 windows/amd64
ana_provider_oaes_ana_change_notice=true
no_provider_oaes_zero=true
aer_completes_on_ana_change=true
aer_completion_event_type=notice
aer_completion_event_info=ana_change
aer_completion_log_page=ana
aer_limit_still_enforced=true
projection_change_count_source=lineage
cleanup_status=ok
```

Validation:

```text
go test ./core/frontend/nvme ./cmd/blockvolume
bash -n scripts/run-phase127-nvme-ana-change-notice-gate.sh
swblock validate testops/scenarios/nvme-ana-change-notice-chain.yaml
bash scripts/run-phase127-nvme-ana-change-notice-gate.sh .
```

## Verdict

PASS.

- OAES ANA Change Notice is advertised only when an `ANAProvider` exists.
- No-provider Identify Controller still reports OAES as zero.
- A parked AER completes when `ANAChangeCount()` advances.
- The completion DW0 is pinned as Notice / ANA Change / ANA log page.
- The existing single pending AER limit remains enforced.
- The provider change count remains tied to blockvolume projection lineage.

## Non-Claims

This sign-off does not claim:

- live Linux host receipt/handling of the AER completion;
- Kubernetes CSI dynamic reconnect/restage after primary or node failover;
- NVMe/RDMA or RoCE;
- DSM/TRIM, Write Zeroes, NVMe authentication, or broad optional-command
  parity;
- performance, throughput, latency, or production SLO.

## Next

The next correctness gate should be either:

1. live Linux host validation that the kernel receives and reacts to the ANA
   Change Notice; or
2. Kubernetes NVMe dynamic reconnect/restage after primary failover.

Backend write optimization remains deferred behind those correctness gates.
