# Phase 106 D1/D2 NVMe/TCP Cross-Node Publish QA Sign-off

Status: PASS.

Validated source branch: `phase106-nvme-tcp-cross-node-live-attach`.

QA run: `20260629-005939-14c1`

Scenario: `testops/scenarios/nvme-tcp-cross-node-publish-chain.yaml`

Result: 18/18 PASS.

## Scope

This gate validates the D1/D2 publish-contract layer for cross-node NVMe/TCP:

- `blockvolume` keeps NVMe/TCP loopback-only by default.
- `--allow-external-nvme-bind` is required for non-loopback NVMe/TCP.
- `blockmaster` accepts and forwards `--launcher-external-nvme`.
- The launcher renders node-address NVMe/TCP targets and the blockvolume opt-in flag.
- Helm exposes `network.externalNVMe` and `ports.nvmeBase`.
- `sw-block ops generate-helm-values --protocol nvme` emits external NVMe values, not external iSCSI or CHAP.

This gate does not claim D3 live cross-node writer/reader I/O.

## Terminal Evidence

```text
phase106_nvme_tcp_cross_node_publish_status=ok
live_io_claim=false
performance_claim_allowed=false
roce_claim_allowed=false
default_loopback_preserved=true
external_nvme_requires_opt_in=true
external_nvme_auth_claim=false
go_test_blockvolume=pass
go_test_launcher=pass
go_test_blockmaster=pass
go_test_sw_block=pass
generate_values_external_nvme=pass
generated_protocol=nvme
generated_external_nvme=true
generated_external_iscsi=false
generated_chap_enabled=false
helm_template_external_nvme=pass
helm_rendered_launcher_external_nvme=true
helm_rendered_launcher_external_iscsi=false
helm_rendered_chap=false
helm_external_status_guard=pass
```

## Verdict

D1/D2 can close. The product now has an explicit, charted, generated,
non-loopback NVMe/TCP publish path for the next live attach gate.

D3 remains open: prove CSI publish/stage agreement and app writer/reader I/O
across Kubernetes nodes using the routable NVMe/TCP target.
