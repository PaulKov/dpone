"""Tests for workload init interactive wizard."""

from __future__ import annotations

import argparse

import pytest

from dpone.readiness.workload_init_wizard import resolve_workload_init_inputs


def test_resolve_workload_init_inputs_requires_flags_without_wizard() -> None:
    args = argparse.Namespace(
        source=None,
        sink=None,
        strategy="full_refresh",
        wizard=False,
        apply=False,
    )
    with pytest.raises(SystemExit, match="requires --source and --sink"):
        resolve_workload_init_inputs(args)


def test_resolve_workload_init_inputs_uses_explicit_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    args = argparse.Namespace(
        source="clickhouse",
        sink="mssql",
        strategy="full_refresh",
        wizard=False,
        apply=False,
    )
    result = resolve_workload_init_inputs(args)
    assert result.source == "clickhouse"
    assert result.sink == "mssql"
    assert result.apply is False


def test_resolve_workload_init_inputs_wizard_prompts_and_confirms_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(
        "dpone.readiness.workload_init_wizard._prompt_choice",
        lambda title, choices: "clickhouse" if "Source" in title else "mssql",
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "y" if "Apply" in prompt else "")
    args = argparse.Namespace(
        source=None,
        sink=None,
        strategy="full_refresh",
        schedule="0 6 * * *",
        wizard=True,
        apply=False,
    )
    result = resolve_workload_init_inputs(args)
    assert result.source == "clickhouse"
    assert result.sink == "mssql"
    assert result.schedule == "0 6 * * *"
    assert result.apply is True


def test_resolve_workload_init_inputs_wizard_accepts_custom_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(
        "dpone.readiness.workload_init_wizard._prompt_choice",
        lambda title, choices: "clickhouse" if "Source" in title else "mssql",
    )

    def fake_input(prompt: str) -> str:
        if "Schedule" in prompt:
            return "0 8 * * *"
        return "n"

    monkeypatch.setattr("builtins.input", fake_input)
    args = argparse.Namespace(
        source=None,
        sink=None,
        strategy="full_refresh",
        schedule="0 6 * * *",
        wizard=True,
        apply=False,
    )
    result = resolve_workload_init_inputs(args)
    assert result.schedule == "0 8 * * *"
    assert result.apply is False
