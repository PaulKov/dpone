from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pytest

from dpone.commands import plan_cmd


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        path=tmp_path / "manifest.yaml",
        selector=None,
        apply_safe_schema=False,
        explain_strategy=False,
        format="json",
    )


def _renderable_payload() -> dict[str, object]:
    return {
        "process": "external-publication",
        "source": {"type": "mssql", "table": "dbo.source"},
        "sink": {"type": "clickhouse", "table": "analytics.target"},
        "strategy": {"mode": "full_refresh"},
        "bulk_path": "native",
        "staging": {"staging_first": True},
        "schema_evolution": {"enabled": False},
        "type_inference": {"options": {"enabled": False}},
        "physical_design": {"options": {"enabled": True}},
        "type_matrix": {},
        "publication": {
            "requested": True,
            "selected": False,
            "mode": "blocked",
            "replication_mode": "external",
            "runtime_admission_required": False,
            "no_fallback": True,
            "blockers": ["clickhouse_cluster_publication.external_engine_must_be_non_replicated_merge_tree"],
        },
    }


def test_plan_returns_validation_exit_for_requested_publication_blockers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "publication": {
            "requested": True,
            "selected": False,
            "mode": "cluster_external",
            "blockers": ["clickhouse_cluster_publication.external_engine_must_be_non_replicated_merge_tree"],
        }
    }
    monkeypatch.setattr(plan_cmd.ExecutionPlanService, "plan_manifest", lambda *_args, **_kwargs: payload)

    exit_code = plan_cmd.cmd_plan(_args(tmp_path), ctx=object(), logger=logging.getLogger("test"))

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out) == payload


def test_plan_keeps_zero_exit_when_requested_publication_is_selected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "publication": {
            "requested": True,
            "selected": True,
            "mode": "cluster_external",
            "blockers": [],
        }
    }
    monkeypatch.setattr(plan_cmd.ExecutionPlanService, "plan_manifest", lambda *_args, **_kwargs: payload)

    exit_code = plan_cmd.cmd_plan(_args(tmp_path), ctx=object(), logger=logging.getLogger("test"))

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == payload


@pytest.mark.parametrize("output_format", ["text", "md"])
def test_blocked_plan_explains_publication_decision_in_human_formats(
    output_format: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _renderable_payload()
    monkeypatch.setattr(plan_cmd.ExecutionPlanService, "plan_manifest", lambda *_args, **_kwargs: payload)
    args = _args(tmp_path)
    args.format = output_format

    exit_code = plan_cmd.cmd_plan(args, ctx=object(), logger=logging.getLogger("test"))

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "publication" in output.lower()
    assert "blocked" in output
    assert "external_engine_must_be_non_replicated_merge_tree" in output
    assert "no_fallback" in output
