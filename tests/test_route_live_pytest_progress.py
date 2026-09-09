"""Contracts for crash-durable, non-authoritative route-live progress."""

from __future__ import annotations

import json
from types import SimpleNamespace

from tools.route_live_certification import pytest_plugin


def test_route_live_progress_survives_before_session_finish(tmp_path, monkeypatch) -> None:
    progress = tmp_path / "diagnostics" / "pytest-progress.jsonl"
    monkeypatch.setenv("DPONE_ROUTE_LIVE_PROGRESS_JSONL", str(progress))
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")

    pytest_plugin.pytest_sessionstart(SimpleNamespace(config=SimpleNamespace()))
    pytest_plugin.pytest_runtest_logstart("tests/test_route.py::test_case[value]", ("test_route.py", 1, "test_case"))
    pytest_plugin.pytest_runtest_logreport(
        SimpleNamespace(
            nodeid="tests/test_route.py::test_case[value]",
            when="setup",
            outcome="passed",
            duration=0.125,
        )
    )
    pytest_plugin.record_route_live_subcase_started(
        suite_id="explicit_types",
        case_id="array_catalog__string_contract__nominal",
        ordinal=5,
    )
    pytest_plugin.record_route_live_subcase_finished(
        suite_id="explicit_types",
        case_id="array_catalog__string_contract__nominal",
        ordinal=5,
    )

    records = [json.loads(line) for line in progress.read_text(encoding="utf-8").splitlines()]
    assert [record["event"] for record in records] == [
        "session_started",
        "started",
        "phase",
        "subcase_started",
        "subcase_finished",
    ]
    assert records[2]["phase"] == "setup"
    assert records[2]["duration_seconds"] == 0.125
    assert records[-2]["case_id"] == "array_catalog__string_contract__nominal"
    assert records[-2]["ordinal"] == 5
    assert records[-1]["suite_id"] == "explicit_types"
    assert all(record["diagnostic_only"] is True for record in records)
    assert all(record["release_ready"] is False for record in records)
    assert all(record["commit_sha"] == "a" * 40 for record in records)

    pytest_plugin.pytest_sessionfinish(SimpleNamespace(), 1)
    terminal = json.loads(progress.read_text(encoding="utf-8").splitlines()[-1])
    assert terminal["event"] == "session_finished"
    assert terminal["exit_status"] == 1


def test_route_live_progress_is_inert_without_explicit_path(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DPONE_ROUTE_LIVE_PROGRESS_JSONL", raising=False)

    pytest_plugin.pytest_sessionstart(SimpleNamespace(config=SimpleNamespace()))
    pytest_plugin.pytest_runtest_logstart("tests/test_route.py::test_case", ("test_route.py", 1, "test_case"))
    pytest_plugin.pytest_sessionfinish(SimpleNamespace(), 0)

    assert list(tmp_path.iterdir()) == []


def test_route_live_progress_ignores_xdist_workers(tmp_path, monkeypatch) -> None:
    progress = tmp_path / "pytest-progress.jsonl"
    monkeypatch.setenv("DPONE_ROUTE_LIVE_PROGRESS_JSONL", str(progress))

    pytest_plugin.pytest_sessionstart(SimpleNamespace(config=SimpleNamespace(workerinput={})))
    pytest_plugin.pytest_runtest_logstart("tests/test_route.py::test_case", ("test_route.py", 1, "test_case"))
    pytest_plugin.pytest_sessionfinish(SimpleNamespace(), 0)

    assert not progress.exists()
