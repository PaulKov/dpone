from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "dpone"


def _loc(path: str) -> int:
    return len((SRC / path).read_text(encoding="utf-8").splitlines())


def test_phase3_module_size_debt_never_waives_hard_limits() -> None:
    payload = json.loads((ROOT / "docs" / "module_size_baseline.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert all(entry["max_lines"] <= 600 for entry in payload["debt"].values())
    assert all(entry["max_sloc"] <= 400 for entry in payload["debt"].values())


def test_phase3_deprecated_implementation_shims_stay_tiny() -> None:
    for path in [
        "runtime/cdc/postgres_impl.py",
        "runtime/connectors/api/appsflyer_impl.py",
        "runtime/connectors/api/google_sheets_impl.py",
        "runtime/connectors/bigquery_impl.py",
        "runtime/sinks/clickhouse_impl.py",
        "runtime/sinks/strategies/mssql/mssql_strategies_impl.py",
        "runtime/sources/strategies/api/yandex_webmaster/common_impl.py",
        "runtime/sources/strategies/postgres/postgres_base_impl.py",
    ]:
        assert _loc(path) <= 90, path
