// Package ublk exposes a frontend.Backend as a local Linux block device
// through the kernel ublk driver (/dev/ublkbN): io_uring command queues,
// one server thread per hardware queue. It is a same-host alternative to
// the NVMe/TCP loopback target.
//
// Request semantics match the NVMe frontend:
//   - READ and WRITE map to Backend.Read and Backend.Write;
//   - FLUSH maps to Backend.Sync;
//   - a WRITE carrying FUA is written and then synced before completing;
//   - any backend error completes the request with -EIO (fail-closed);
//     a range outside the device completes with -EINVAL without touching
//     the backend; other operations complete with -EOPNOTSUPP (the device
//     advertises no discard or write-zeroes support).
//
// Server crash or restart is fail-closed: the device is created without
// UBLK_F_USER_RECOVERY, so when the server process dies the kernel aborts
// outstanding and new I/O on /dev/ublkbN with errors; nothing is
// acknowledged that the backend did not complete. A restarted server
// deletes a stale device left at the same id and creates it again; the
// consumer must reopen /dev/ublkbN.
package ublk
