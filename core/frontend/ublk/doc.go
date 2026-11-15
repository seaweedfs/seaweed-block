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
// acknowledged that the backend did not complete.
//
// A restarted server never reclaims a device left at its id: Start
// refuses any pre-existing /dev/ublkcN with an error naming the id and
// sends nothing to the kernel. Removing a leftover device is a separate
// operator step, taken only after proving no server uses it: delete it
// through the ublk control device (for example the ublksrv "ublk del -n N"
// tool), or unload ublk_drv once no device is in use. Then start again;
// the consumer must reopen /dev/ublkbN.
package ublk
