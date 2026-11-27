"""Tests shard-named-tests.py: every name in exactly one shard, longest first
into the least-loaded shard, the same split on every run, unknown names
counted, and a duplicate or empty list refused."""
import importlib.util
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "shard-named-tests.py")
spec = importlib.util.spec_from_file_location("shard", SCRIPT)
shard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shard)

fail = 0


def check(name, ok):
    global fail
    print(("ok   " if ok else "FAIL ") + name)
    if not ok:
        fail = 1


secs = {"A": 500, "B": 400, "C": 300, "D": 100, "E": 100}
parts, load = shard.shard(list(secs), secs, 2)
check("every name once", sorted(sum(parts, [])) == sorted(secs))
check("longest first, least loaded", parts == [["A", "D", "E"], ["B", "C"]] and load == [700, 700])
check("same split on every run", shard.shard(list(secs), secs, 2) == (parts, load))
parts, load = shard.shard(["A", "Z"], {"A": 10}, 2)
check("an unknown name counts as the default", load == [shard.DEFAULT, 10.0])
parts, _ = shard.shard(["A", "B"], {}, 4)
check("more shards than names leaves some empty", sum(len(p) for p in parts) == 2 and [] in parts)

with tempfile.TemporaryDirectory() as d:
    names = os.path.join(d, "names.txt")
    seconds = os.path.join(d, "seconds.txt")
    with open(seconds, "w") as f:
        f.write("# times\nTestA 30\nTestB 20\n")
    with open(names, "w") as f:
        f.write("# list\nTestA\nTestB  # note\n\nTestC\n")
    rc = subprocess.run([sys.executable, "-I", SCRIPT, names, seconds, "2", os.path.join(d, "out")], capture_output=True, text=True).returncode
    got = sorted(open(os.path.join(d, "out", f)).read().split() for f in ("1.txt", "2.txt"))
    check("the files hold every name once", rc == 0 and sorted(sum(got, [])) == ["TestA", "TestB", "TestC"])
    with open(names, "w") as f:
        f.write("TestA\nTestA\n")
    rc = subprocess.run([sys.executable, "-I", SCRIPT, names, seconds, "2", os.path.join(d, "o2")], capture_output=True, text=True).returncode
    check("a name listed twice is refused", rc == 2)
    with open(names, "w") as f:
        f.write("# nothing\n")
    rc = subprocess.run([sys.executable, "-I", SCRIPT, names, seconds, "2", os.path.join(d, "o3")], capture_output=True, text=True).returncode
    check("an empty list is refused", rc == 2)

sys.exit(fail)
