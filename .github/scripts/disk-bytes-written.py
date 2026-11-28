"""Runs a command and reports the bytes it and its children wrote to disk.

Usage: python3 -I disk-bytes-written.py CMD [ARG...]

The count is the kernel's write accounting for the command's process tree
(getrusage RUSAGE_CHILDREN ru_oublock, 512-byte units). It counts writes to
block devices only: files on a tmpfs are not in it. The command's exit status
is kept.
"""
import resource
import subprocess
import sys

rc = subprocess.call(sys.argv[1:])
blocks = resource.getrusage(resource.RUSAGE_CHILDREN).ru_oublock
print(f"disk bytes written by the command: {blocks * 512} ({blocks * 512 / 2**30:.2f} GiB)", flush=True)
sys.exit(rc)
