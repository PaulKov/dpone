from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LIVE_EXECUTOR_PRODUCERS = (
    ROOT / "tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py",
    ROOT / "tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py",
)


@pytest.mark.parametrize("producer_path", LIVE_EXECUTOR_PRODUCERS, ids=lambda path: path.stem)
def test_retained_live_evidence_does_not_own_secret_bearing_executor_configs(
    producer_path: Path,
) -> None:
    source = producer_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    retained_config_writes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword)
        and node.arg == "path"
        and isinstance(node.value, ast.BinOp)
        and isinstance(node.value.left, ast.Name)
        and node.value.left.id == "artifact_root"
        and "executor-config" in ast.unparse(node.value)
    ]

    assert retained_config_writes == []
    assert 'path=runtime_config_root / "executor-config.json"' in source
    assert 'path=runtime_config_root / "executor-config-evolved.json"' in source
    assert "runtime_config_dir.cleanup()" in source
