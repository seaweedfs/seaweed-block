# Local disk headroom at volume create

Creating a volume now requires local free space for its full size plus the WAL, checked per volume at create time.

- **Create:** blockvolume refuses to create a volume when the durable root's file system has less free space than
  the volume size plus its 64 MiB WAL. The refusal names both numbers. There is no override.
- **Start:** an existing volume still opens, so its acknowledged data stays served. It logs a `WARNING` when the free
  space is below its remaining growth plus the WAL.
- **Why:** on a full local disk a Linux guest's writes fail after about 75 s with an I/O error (EREMOTEIO).
  See [Size a host](../operator-guide.md#size-a-host).
