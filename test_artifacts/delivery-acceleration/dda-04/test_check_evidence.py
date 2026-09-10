"""Regression checks for source identity in the DDA-04 evidence producer."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def producer():
    spec = importlib.util.spec_from_file_location("dda04_checks", Path(__file__).with_name("run_checks.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "changed,exit_code,expected",
    [(False, 0, "PASS"), (False, 1, "FAIL"), (True, 0, "UNVERIFIED"), (True, 1, "UNVERIFIED")],
)
def test_source_identity_changes_cannot_be_reported_as_authoritative(
    tmp_path, monkeypatch, changed, exit_code, expected
):
    module = producer()
    first = {"head": "before", "tree": "tree", "source_sha256": "source", "producer_sha256": "producer"}
    last = dict(first, head="after") if changed else dict(first)
    snapshots = iter((first, last))
    monkeypatch.setattr(module, "OUTPUT", tmp_path)
    monkeypatch.setattr(module, "source_identity", lambda: next(snapshots))
    monkeypatch.setattr(module, "git", lambda *args: "")
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=exit_code))
    module.run("ruff")
    record = json.loads((tmp_path / "ruff.json").read_text())
    assert record["status"] == expected
    assert record["source_before"] == first and record["source_after"] == last
    assert record["source_unchanged"] is not changed
    assert record["exit_code"] == exit_code
