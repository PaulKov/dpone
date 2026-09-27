"""Producer contract for the target-local layout certification receipt."""

from __future__ import annotations

import json

import pytest
from tools.mssql_target_local_layout_artifact import write_layout_artifact


def test_layout_artifact_is_exact_commit_bound_and_deterministic(tmp_path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    write_layout_artifact(source_commit="a" * 40, output=first)
    write_layout_artifact(source_commit="a" * 40, output=second)

    payload = json.loads(first.read_text(encoding="utf-8"))
    assert first.read_bytes() == second.read_bytes()
    assert payload["source_commit"] == "a" * 40
    assert payload["kind"] == "dpone.mssql-target-local-layout-matrix"
    assert len(payload["capability_digest"]) == 64


def test_layout_artifact_rejects_nonexact_commit(tmp_path) -> None:
    with pytest.raises(ValueError, match="layout_matrix_source_commit"):
        write_layout_artifact(source_commit="main", output=tmp_path / "invalid.json")
