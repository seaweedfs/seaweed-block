#!/usr/bin/env python3
"""Validate a protocol release gate suite artifact bundle.

This is intentionally offline-only: it inspects the suite result/status files
and child runner bundles without touching the lab. Use it after a long hardware
run to catch malformed suite metadata, missing child run pointers, or partial
child phases before doing manual artifact review.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any


EXPECTED_CHILDREN = [
    "iscsi-p6-alua-failover",
    "nvme-p4-multipath-failover",
    "nvme-p5-csi-protocol",
    "iscsi-p8-compat-soak",
]


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"missing {path}") from None
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid JSON {path}: {e}") from None
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def phase_map(doc: dict[str, Any], field: str) -> dict[str, dict[str, Any]]:
    phases = doc.get(field)
    if not isinstance(phases, list):
        raise ValueError(f"{field} must be a list")
    out: dict[str, dict[str, Any]] = {}
    for item in phases:
        if not isinstance(item, dict):
            raise ValueError(f"{field} entries must be objects")
        name = item.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"{field} entry missing name")
        out[name] = item
    return out


def require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def validate(root: pathlib.Path, expect_product_commit: str | None, require_pass: bool) -> list[str]:
    errors: list[str] = []
    result = load_json(root / "result.json")
    status = load_json(root / "status.json")

    require(result.get("scenario") == "protocol-release-gate-suite", "result.json scenario mismatch", errors)
    require(status.get("scenario") == "protocol-release-gate-suite", "status.json scenario mismatch", errors)
    require(result.get("run_id") == status.get("run_id"), "run_id differs between result.json and status.json", errors)
    require(result.get("product_commit") == status.get("product_commit"), "product_commit differs between result/status", errors)
    require(result.get("runner_commit") == status.get("runner_commit"), "runner_commit differs between result/status", errors)

    if expect_product_commit:
        require(
            str(result.get("product_commit", "")).startswith(expect_product_commit),
            f"product_commit {result.get('product_commit')} does not match expected {expect_product_commit}",
            errors,
        )

    for key in ("started_at", "ended_at", "wall_clock_s", "artifact_dir"):
        require(key in result, f"result.json missing {key}", errors)
        require(key in status, f"status.json missing {key}", errors)

    if require_pass:
        require(result.get("status") == "pass", f"result status is {result.get('status')}, expected pass", errors)
        require(status.get("state") == "pass", f"status state is {status.get('state')}, expected pass", errors)

    result_phases = phase_map(result, "phase_results")
    status_phases = phase_map(status, "phases")
    require(list(result_phases) == EXPECTED_CHILDREN, f"result child order mismatch: {list(result_phases)}", errors)
    require(list(status_phases) == EXPECTED_CHILDREN, f"status child order mismatch: {list(status_phases)}", errors)

    for child in EXPECTED_CHILDREN:
        r_phase = result_phases.get(child, {})
        s_phase = status_phases.get(child, {})
        require(r_phase.get("status") == s_phase.get("status"), f"{child}: status differs between result/status", errors)
        require(r_phase.get("run_id") == s_phase.get("run_id"), f"{child}: run_id differs between result/status", errors)
        require(r_phase.get("run_dir") == s_phase.get("run_dir"), f"{child}: run_dir differs between result/status", errors)
        require(r_phase.get("phases_done") == s_phase.get("phases_done"), f"{child}: phases_done differs", errors)
        require(r_phase.get("phases_total") == s_phase.get("phases_total"), f"{child}: phases_total differs", errors)
        if require_pass:
            require(r_phase.get("status") == "pass", f"{child}: status is {r_phase.get('status')}, expected pass", errors)
            require(r_phase.get("phases_done") == r_phase.get("phases_total"), f"{child}: incomplete phases", errors)

        run_dir_value = r_phase.get("run_dir")
        require(isinstance(run_dir_value, str) and run_dir_value, f"{child}: missing run_dir", errors)
        if isinstance(run_dir_value, str) and run_dir_value:
            run_dir = pathlib.Path(run_dir_value)
            if not run_dir.is_absolute():
                run_dir = root / run_dir
            require((run_dir / "status.json").exists(), f"{child}: missing child status.json at {run_dir}", errors)
            require((run_dir / "result.json").exists(), f"{child}: missing child result.json at {run_dir}", errors)

    return errors


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("artifact_dir", type=pathlib.Path, help="Protocol release gate suite artifact directory")
    ap.add_argument("--expect-product-commit", help="Expected product commit prefix")
    ap.add_argument("--allow-fail", action="store_true", help="Validate schema without requiring PASS")
    args = ap.parse_args()

    root = args.artifact_dir.resolve()
    try:
        errors = validate(root, args.expect_product_commit, require_pass=not args.allow_fail)
    except ValueError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 1

    if errors:
        print("FAIL: protocol release gate bundle validation failed", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    result = load_json(root / "result.json")
    print(
        "PASS: protocol release gate bundle "
        f"run_id={result.get('run_id')} product_commit={result.get('product_commit')} "
        f"wall_clock_s={result.get('wall_clock_s')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
