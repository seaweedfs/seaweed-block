#!/usr/bin/env python3
"""Check Model citations against a base-owned, exact-path TSV mapping."""
import re
import sys
from pathlib import Path, PurePosixPath

IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
ACTION = re.compile(rf"{IDENT}\.{IDENT}")
PURE = re.compile(rf"NONE \(pure function of ({IDENT})\)")


def read_mapping(path):
    mapping = {}
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        columns = line.split("\t")
        if len(columns) != 2:
            raise ValueError(f"mapping line {number}: expected PATH<TAB>Module.Action")
        file, action = columns
        parts = PurePosixPath(file).parts
        if (not file or file.startswith("/") or "\\" in file or ":" in file
                or any(part in (".", "..") for part in file.split("/"))
                or str(PurePosixPath(file)) != file or not parts):
            raise ValueError(f"mapping line {number}: expected a repository-relative exact path")
        if not ACTION.fullmatch(action):
            raise ValueError(f"mapping line {number}: invalid Module.Action")
        mapping.setdefault(file, set()).add(action)
    if not mapping:
        raise ValueError("model mapping is empty; populate it before enabling this gate")
    return mapping


def check(model, paths, mapping):
    # A list permits one citation for each mapped file in a multi-module PR.
    # Full matches prevent prose, quoted examples, or a prefixed NONE excuse
    # from accidentally satisfying a mapped-file obligation.
    citations = {part.strip() for part in model.split(";")}
    actions = {part for part in citations if ACTION.fullmatch(part)}
    pure = {match.group(1) for part in citations if (match := PURE.fullmatch(part))}
    problems = []
    for file in sorted(set(paths)):
        expected = mapping.get(file)
        if expected and not (actions & expected or any(a.split(".")[0] in pure for a in expected)):
            names = "; ".join(sorted(expected))
            problems.append(f"{file}: Model must cite {names} or NONE (pure function of its module)")
    return problems


def main():
    if len(sys.argv) != 3:
        print("usage: model-trace-check.py MAPPING MODEL < nul-separated-paths", file=sys.stderr)
        return 2
    try:
        mapping = read_mapping(sys.argv[1])
        raw = sys.stdin.buffer.read()
        if raw and not raw.endswith(b"\0"):
            raise ValueError("unterminated changed path")
        paths = [p.decode("utf-8") for p in raw.split(b"\0")[:-1]]
        if any(not p for p in paths):
            raise ValueError("empty changed path")
        problems = check(sys.argv[2], paths, mapping)
    except (OSError, UnicodeError, ValueError) as error:
        print(f"spec-trace: {error}", file=sys.stderr)
        return 2
    for problem in problems:
        print(f"spec-trace: {problem}")
    return int(bool(problems))


if __name__ == "__main__":
    sys.exit(main())
