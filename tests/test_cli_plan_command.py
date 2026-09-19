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
