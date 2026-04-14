package main

// ScopeStatement is the authoritative honest capability statement.
// Shown by `sparrow --help` and echoed by the optional HTTP /status
// endpoint so no interface ever overclaims what the semantic core
// actually owns.
const ScopeStatement = `seaweed-block sparrow, runnable slice with bootstrap, calibration, and single-node persistence.

Supported:
  - run the three canonical demos (healthy, catch-up, rebuild)
  - repeatable validation via --runs N
  - machine-readable output via --json
  - optional read-only HTTP inspection surface via --http ADDR
  - calibration pass via --calibrate (5 scenarios C1-C5)
  - single-node persistence demo via --persist-demo --persist-dir DIR
    (WAL+extent backend; write -> Sync -> Close -> reopen -> Recover -> read)
  - crash consistency for acked writes (writes covered by a successful Sync
    survive process kill; verified by simulated-crash test in the
    storage package, not by this CLI demo)

Not supported (errors surface explicitly):
  - distributed durability across nodes
  - master service / dynamic assignment
  - any mutation via the HTTP ops surface
  - RF3 and multi-replica flows
  - frontend protocols (iSCSI, NVMe-oF)
  - concurrent writer during replication
  - a real operator CLI surface        (weed shell is the future path)
`

// Version is the current binary version. Zero-dot to make the
// pre-production status explicit — not a 1.x contract.
const Version = "0.7.0-phase07"
