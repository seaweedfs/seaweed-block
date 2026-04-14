package main

// ScopeStatement is the authoritative Phase 05 honest capability
// statement. Shown by `sparrow --help` and echoed by the optional
// HTTP /status endpoint so no interface ever overclaims what the
// semantic core actually owns.
const ScopeStatement = `seaweed-block sparrow, runnable slice with Phase 05 ops and Phase 06 calibration.

Supported:
  - run the three canonical demos (healthy, catch-up, rebuild)
  - repeatable validation via --runs N
  - machine-readable output via --json
  - optional read-only HTTP inspection surface via --http ADDR
  - Phase 06 calibration pass via --calibrate (C1 through C5)

Not supported (errors surface explicitly):
  - persistence or crash recovery      (storage is in-memory)
  - master service / dynamic assignment
  - any mutation via the HTTP ops surface
  - RF3 and multi-replica flows
  - frontend protocols (iSCSI, NVMe-oF)
  - concurrent writer during replication
  - a real operator CLI surface        (weed shell is the future path)
`

// Version is the current binary version. Zero-dot to make the
// pre-production status explicit — not a 1.x contract.
const Version = "0.5.0-phase05"
