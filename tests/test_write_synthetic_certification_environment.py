"""Privacy contract for the synthetic certification environment producer."""

from __future__ import annotations

import json

import pytest
from tools.write_synthetic_certification_environment import write_environment_receipt


def _values(tmp_path):
    return {
        "output": tmp_path / "environment.json",
        "source_commit": "a" * 40,
        "platform": "linux-arm64",
        "python_version": "3.12.14",
        "pyodbc_version": "5.3.0",
        "odbc_driver": "ODBC Driver 18 for SQL Server",
        "mssql_version": "16.0.1000.6",
        "clickhouse_version": "24.8.14.39",
        "runner_image": "sha256:" + "b" * 64,
        "mssql_image": "sha256:" + "c" * 64,
        "clickhouse_image": "sha256:" + "d" * 64,
    }


def test_environment_receipt_contains_only_allowlisted_reproducibility_fields(tmp_path) -> None:
    values = _values(tmp_path)
    write_environment_receipt(**values)

    payload = json.loads(values["output"].read_text(encoding="utf-8"))
    assert payload["source_commit"] == "a" * 40
    assert payload["runtime"]["platform"] == "linux-arm64"
    assert set(payload["services"]) == {"mssql", "clickhouse"}
    assert "host" not in values["output"].read_text(encoding="utf-8").lower()


@pytest.mark.parametrize(
    ("field", "value", "diagnostic"),
    [
        ("platform", "private.example.internal", "version"),
        ("runner_image", "latest", "image"),
        ("source_commit", "main", "source_commit"),
    ],
)
def test_environment_receipt_rejects_unbounded_or_nonexact_values(tmp_path, field, value, diagnostic) -> None:
    values = _values(tmp_path)
    values[field] = value

    with pytest.raises(ValueError, match=diagnostic):
        write_environment_receipt(**values)
