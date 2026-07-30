# Phase 168 D1 io_uring Capability Gate

Status: PASS at exact commit `ea1a44c`.

## Scope

This gate answers one narrow question before any product integration:

> Can the supported Linux build use a bounded `io_uring` to submit
> non-contiguous file writes, consume every completion, issue a durability
> barrier, and verify the bytes after close/reopen?

It does not add a storage selector, change `parallelwal`, claim a performance
gain, or enable a mounted path.

## Dependency Decision

The spike uses the repository's existing `golang.org/x/sys/unix` dependency
for syscall numbers, `mmap`, and `munmap`. The small Linux UAPI structures
needed by the probe are local to `cmd/io-uring-probe`.

| Property | Decision |
|---|---|
| New module | none |
| License | existing `x/sys` three-clause BSD-style license |
| CGO | not required |
| Linux boundary | `linux/amd64` build-tagged raw UAPI probe |
| Other platforms | explicit unsupported stub; Windows amd64 cross-build gate |
| Product API exposure | none |
| Wrapper comparison | deferred; no wrapper is necessary to prove D1 |

A wrapper is not selected during D1 because the executable raw spike is
bounded and passes without adding a module or CGO boundary. D2 must move only
the proven minimum into an internal execution seam; it must not make the
probe package a public storage abstraction.

## Developer Evidence

Exact source commit: `ea1a44c`

Environment:

```text
host=m02
kernel_release=6.17.0-23-generic
io_uring_disabled=0
seccomp=0
```

Gate:

```text
phase168_io_uring_capability_status=ok
product_selector_added=false
parallelwal_integration_added=false
linux_probe_test=pass
linux_write_fsync_reopen=pass
required_opcodes=write,fsync
dependency_added=false
cgo_required=false
windows_cross_compile=pass
unsupported_platform_boundary=explicit
```

Probe:

```text
io_uring_probe_status=ok
platform=linux/amd64
kernel_release=6.17.0-23-generic
io_uring_supported=true
refusal_reason=-
queue_depth=8
write_opcode_supported=true
fsync_opcode_supported=true
submitted_ops=4
submit_syscalls=4
write_completions=3
fsync_completions=1
completion_count=4
verified_bytes=12288
implementation=raw_linux_uapi
dependency=golang.org/x/sys/unix
cgo_required=false
```

The three writes use offsets `0`, `12288`, and `4096`, so they are deliberately
non-contiguous in submission order. The barrier is an `IORING_OP_FSYNC`, not an
`os.File.Sync` hidden outside the ring. Every CQE is matched by `user_data`,
write lengths are checked, and all payloads are verified after reopening the
file. Interrupted waits are retried, accepted submissions are counted from the
kernel-owned SQ head, and every accepted request buffer remains live through a
terminal CQE.

Independent validation:

```text
adversarial_review=ACCEPT
independent_qa=PASS
linux_race_count_20=PASS
go_vet=PASS
probe_temp_dir_count=0
remote_cleanup_status=ok
local_cleanup_status=ok
```

The probe's accepted-request drain intentionally relies on the gate's outer
timeout if the kernel stops making progress. That is acceptable for this
process-scoped capability test only. D2 product code must define bounded
shutdown and terminal completion semantics before it can own application
buffers.

## D1 Verdict

D1 is closed. It permits D2 implementation work; it does not permit a selector,
default change, performance claim, or mounted/RF3 claim.
