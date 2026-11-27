"""Split the named process-level tests into N balanced shards.

Usage: shard-named-tests.py NAMES_FILE SECONDS_FILE N OUT_DIR

Writes OUT_DIR/1.txt .. OUT_DIR/N.txt, one test name per line. Every name in
NAMES_FILE (# starts a comment) lands in exactly one shard. SECONDS_FILE holds
"Name seconds" lines from an earlier run; a name missing there counts as
DEFAULT seconds. Longest first, each into the shard with the least time so
far (ties to the lowest shard), so the result is the same on every run.
Prints each shard's names count and its estimated seconds.

Exit 0, or 2 on a usage error, a duplicate name or an empty names list.
"""
import os
import sys

DEFAULT = 60.0


def names_of(path):
    out = []
    for line in open(path, encoding="utf-8"):
        name = line.split("#", 1)[0].strip()
        if name:
            out.append(name)
    return out


def seconds_of(path):
    out = {}
    for line in open(path, encoding="utf-8"):
        fields = line.split("#", 1)[0].split()
        if len(fields) == 2:
            out[fields[0]] = float(fields[1])
    return out


def shard(names, seconds, n):
    load = [0.0] * n
    shards = [[] for _ in range(n)]
    for name in sorted(names, key=lambda x: (-seconds.get(x, DEFAULT), x)):
        k = min(range(n), key=lambda i: (load[i], i))
        shards[k].append(name)
        load[k] += seconds.get(name, DEFAULT)
    return shards, load


def main(argv):
    if len(argv) != 5:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    names = names_of(argv[1])
    if not names or len(set(names)) != len(names):
        print("shard-named-tests: empty names list or a name listed twice", file=sys.stderr)
        return 2
    n = int(argv[3])
    if n < 1:
        return 2
    shards, load = shard(names, seconds_of(argv[2]), n)
    os.makedirs(argv[4], exist_ok=True)
    for i, part in enumerate(shards, 1):
        with open(os.path.join(argv[4], "%d.txt" % i), "w", encoding="utf-8") as f:
            f.write("".join(x + "\n" for x in part))
        print("shard %d: %d tests, about %d s" % (i, len(part), load[i - 1]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
