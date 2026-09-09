from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.commands.runtime_storage_cmd import cmd_runtime_storage_gc


def _args(**overrides: object) -> Namespace:
    data = {
        "work_dir": "",
        "older_than_seconds": 0,
        "dry_run": True,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def test_runtime_storage_gc_dry_run_reports_candidates_without_deleting(tmp_path: Path, capsys) -> None:
    stale = tmp_path / "orders" / "run-1" / "transfer" / "slice.tsv"
    stale.parent.mkdir(parents=True)
    stale.write_text("1\talpha\n", encoding="utf-8")

    code = cmd_runtime_storage_gc(_args(work_dir=str(tmp_path)), ctx=object(), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["deleted_count"] == 0
    assert payload["candidate_count"] == 1
    assert stale.exists()


def test_runtime_storage_gc_apply_deletes_candidates(tmp_path: Path, capsys) -> None:
    stale = tmp_path / "orders" / "run-1" / "transfer" / "slice.tsv"
    stale.parent.mkdir(parents=True)
    stale.write_text("1\talpha\n", encoding="utf-8")

    code = cmd_runtime_storage_gc(
        _args(work_dir=str(tmp_path), dry_run=False),
        ctx=object(),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["deleted_count"] == 1
    assert not stale.exists()
