"""Tests for bounded A/B/C governance snapshot stability."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


snapshot = _load("dpone_agent_governance_snapshot_tests", "tools/agent_policy/governance_snapshot.py")


def test_stable_projection_requires_identical_reads_and_gap() -> None:
    reads = [{"version_id": 1}, {"version_id": 1}]
    sleeps: list[float] = []
    clock_values = iter([0.0, 5.1])

    def reader() -> dict[str, int]:
        return reads.pop(0)

    result, blockers = snapshot.capture_stable_projection(
        label="A",
        reader=reader,
        sleeper=sleeps.append,
        clock=lambda: next(clock_values),
        min_gap_seconds=5.0,
    )
    assert blockers == ()
    assert result is not None
    assert result.read_count == 2
    assert sleeps == [5.0]


def test_unstable_projection_fails_closed() -> None:
    reads = [{"version_id": 1}, {"version_id": 2}]
    result, blockers = snapshot.capture_stable_projection(
        label="B",
        reader=lambda: reads.pop(0),
        sleeper=lambda _: None,
        clock=lambda: 10.0,
    )
    assert result is None
    assert {item.code for item in blockers} == {"GOVERNANCE_SNAPSHOT_UNSTABLE"}


def test_protected_base_ancestry_rule() -> None:
    def is_ancestor(older: str, newer: str) -> bool:
        order = {"a": 1, "b": 2, "c": 3}
        return order[older] <= order[newer]

    assert snapshot.protected_base_ancestry_ok("a", "b", "c", is_ancestor=is_ancestor)
    assert not snapshot.protected_base_ancestry_ok("b", "a", "c", is_ancestor=is_ancestor)
