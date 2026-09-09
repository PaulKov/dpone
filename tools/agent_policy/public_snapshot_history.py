"""Separate public snapshot continuity from retrospective CI-shadow proof.

The public root is an immutable observation anchor, never a replacement for an
original integration commit. Ordinary regression tests call the continuity
assertion. Run ``python -m tools.agent_policy.public_snapshot_history`` to execute
the preserved original assertions when their Git objects are available. Its JSON
report and exit status distinguish missing evidence (UNVERIFIED, exit 2) from a
contradiction (FAIL, exit 1) and successful historical verification (PASS, exit 0).
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PUBLIC_ROOT = "f8c6a4a5e75d167829c05f65d5d3033acb193878"
PUBLIC_TREE = "1c23d4021b6f2e8c8d6519f41b86cf4ec14b29df"


def _git(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        env={**os.environ, "GIT_NO_REPLACE_OBJECTS": "1"},
    ).stdout


def assert_public_snapshot_continuity(root: Path, paths: Sequence[str]) -> None:
    """Require the fixed public root/tree, ancestry and unchanged imported bytes.

    This proves current continuity only. It does not reconstruct the unavailable
    review, approval, provider receipt or integration history of imported files.
    Committed HEAD, intervening history and working bytes must agree with the
    frozen anchor; editing and then restoring a contract cannot erase drift.
    """
    if not __debug__:
        raise ValueError("historical assertion verification requires Python assertions enabled")
    assert _git(root, "rev-parse", f"{PUBLIC_ROOT}^{{commit}}").decode().strip() == PUBLIC_ROOT
    assert _git(root, "rev-parse", f"{PUBLIC_ROOT}^{{tree}}").decode().strip() == PUBLIC_TREE
    assert _git(root, "show", "-s", "--format=%P", PUBLIC_ROOT).strip() == b""
    _git(root, "merge-base", "--is-ancestor", PUBLIC_ROOT, "HEAD")
    for relative in paths:
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("continuity paths must be confined repository files")
        current = root
        for part in path.parts:
            current /= part
            assert not current.is_symlink(), f"symlink in frozen contract path: {relative}"
        frozen = _git(root, "show", f"{PUBLIC_ROOT}:{relative}")
        assert _git(root, "show", f"HEAD:{relative}") == frozen, f"committed contract drift: {relative}"
        assert current.read_bytes() == frozen, f"working contract drift: {relative}"
        commits = _git(root, "log", "--full-history", "--format=%H", f"{PUBLIC_ROOT}..HEAD", "--", relative)
        for commit in commits.decode().splitlines():
            assert _git(root, "show", f"{commit}:{relative}") == frozen, f"historical contract drift: {relative}"


@dataclass(frozen=True)
class HistoricalCheck:
    """Original assertion plus the original commits necessary to execute it."""

    name: str
    required_commits: tuple[str, ...]
    verify: Callable[[], None]


def evaluate_history(root: Path, checks: Sequence[HistoricalCheck]) -> dict[str, Any]:
    """Evaluate available original evidence; absence never becomes success."""
    results: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "public_root": PUBLIC_ROOT,
        "public_tree": PUBLIC_TREE,
        "scope": "retrospective CI-shadow history; public continuity is not historical proof",
        "checks": results,
    }
    try:
        assert_public_snapshot_continuity(root, ())
    except (AssertionError, ValueError, OSError, subprocess.CalledProcessError):
        return {**report, "status": "FAIL", "reason": "public snapshot anchor cannot be verified"}
    for check in checks:
        missing = []
        for commit in check.required_commits:
            try:
                _git(root, "cat-file", "-e", f"{commit}^{{commit}}")
            except subprocess.CalledProcessError:
                missing.append(commit)
        if missing:
            results.append({"name": check.name, "status": "UNVERIFIED", "missing_commits": missing})
            continue
        try:
            check.verify()
        except (AssertionError, ValueError, OSError, subprocess.CalledProcessError):
            results.append({"name": check.name, "status": "FAIL", "reason": "original historical assertion failed"})
        else:
            results.append({"name": check.name, "status": "PASS"})
    if not results:
        return {**report, "status": "UNVERIFIED", "reason": "no historical assertions selected"}
    statuses = {item["status"] for item in results}
    report["status"] = "FAIL" if "FAIL" in statuses else "UNVERIFIED" if "UNVERIFIED" in statuses else "PASS"
    return report


def _load_checks(root: Path) -> tuple[HistoricalCheck, ...]:
    """Load preserved assertion callables without running ordinary pytest tests."""
    modules = (
        "tests/test_ci_shadow_pr3b_scope_contracts.py",
        "tests/test_ci_shadow_pr3b_spec_contracts.py",
        "tests/test_ci_shadow_pr3b_output_amendment_contracts.py",
        "tests/agent_policy/test_ci_shadow_pr3a_implementation_contract.py",
        "tests/agent_policy/test_ci_shadow_pr3b_implementation_contract.py",
    )
    checks: list[HistoricalCheck] = []
    for index, relative in enumerate(modules):
        name = f"_dpone_retrospective_{index}"
        spec = importlib.util.spec_from_file_location(name, root / relative)
        if spec is None or spec.loader is None:
            raise ValueError("historical assertion module cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        checks.extend(module.HISTORICAL_CHECKS)
    return tuple(checks)


def main() -> int:
    """Emit the standalone historical report, retaining nonzero unknown status."""
    root = Path(__file__).resolve().parents[2]
    report = evaluate_history(root, _load_checks(root))
    print(json.dumps(report, indent=2, sort_keys=True))
    return {"PASS": 0, "FAIL": 1, "UNVERIFIED": 2}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
