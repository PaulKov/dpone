from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "dpone"


def _loc(path: str) -> int:
    return len((SRC / path).read_text(encoding="utf-8").splitlines())


def _imports(path: str) -> set[str]:
    tree = ast.parse((SRC / path).read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_manifest_public_modules_are_thin_facades() -> None:
    assert _loc("manifest/validation.py") <= 120
    assert _loc("manifest/migrate.py") <= 140
    assert _loc("manifest/batch_compiler.py") <= 120


def test_runtime_artifacts_core_is_vendor_neutral_facade() -> None:
    assert _loc("runtime/artifacts.py") <= 250
    imports = _imports("runtime/artifacts.py")
    forbidden = {"dpone.runtime.connectors.bigquery", "dpone.runtime.sinks.clickhouse", "dpone.runtime.sinks.mssql"}
    assert not (imports & forbidden)


def test_command_registry_is_split_into_focused_sections() -> None:
    assert _loc("commands/registry.py") <= 80
    for path in [
        "commands/registry_core.py",
        "commands/registry_ops.py",
        "commands/registry_schema.py",
        "commands/registry_manifest.py",
        "commands/registry_dag.py",
        "commands/registry_docs.py",
    ]:
        assert _loc(path) <= 450, path


def test_remaining_module_size_debt_uses_closed_v2_exact_caps() -> None:
    payload = json.loads((ROOT / "docs" / "module_size_baseline.json").read_text(encoding="utf-8"))
    assert set(payload) == {"debt", "schema_version"}
    assert payload["schema_version"] == 2
    assert isinstance(payload["debt"], dict)
    assert payload["debt"]
    required = {
        "accepted_adr",
        "baseline_commit",
        "max_lines",
        "max_sloc",
        "owner",
        "reason",
        "target_date",
        "target_sloc",
    }
    assert all(set(entry) == required for entry in payload["debt"].values())


def test_connector_and_strategy_facades_are_below_hard_limit() -> None:
    for path in [
        "runtime/connectors/api/omnidesk.py",
        "runtime/connectors/api/google_sheets.py",
        "runtime/connectors/api/appsflyer.py",
        "runtime/connectors/bigquery.py",
        "runtime/sinks/clickhouse.py",
        "runtime/sinks/strategies/mssql/mssql_strategies.py",
        "runtime/sources/strategies/postgres/postgres_base.py",
        "runtime/cdc/postgres.py",
    ]:
        assert _loc(path) <= 450, path
