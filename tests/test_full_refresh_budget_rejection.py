"""Public admission must not silently discard a requested source-byte limit."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from dpone.config.load_strategy import LoadStrategy
from dpone.dag.config import ETLProcessConfig, LoadConfigBuilder
from dpone.dag.errors import DagConfigurationError
from dpone.runtime.clickhouse_file_stage_contract import ClickHouseValidatedFilePolicy
from dpone.strategy_intelligence.compiler import StrategyAutoCompiler


def _process(strategy: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": "synthetic_source_budget",
        "source": {
            "type": "mssql",
            "connection_id": "synthetic_source",
            "table": {"database": "synthetic", "schema": "dbo", "name": "source"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "synthetic_target",
            "table": {"schema": "synthetic", "name": "target"},
            "strategy": strategy,
        },
    }


@pytest.mark.parametrize("value", [100, None, 0, -1, True, "100"])
@pytest.mark.parametrize("mode", [None, "full_refresh", "auto", "incremental_append"])
def test_public_builder_rejects_present_budget_without_changing_input(mode: str | None, value: object) -> None:
    strategy = {"max_source_bytes": value}
    if mode is not None:
        strategy["mode"] = mode
    process = _process(strategy)
    original = deepcopy(process)

    with pytest.raises(DagConfigurationError, match=r"sink\.strategy\.max_source_bytes cannot be enforced"):
        LoadConfigBuilder().build(process)

    assert process == original


def test_budget_rejection_precedes_strategy_compilation() -> None:
    compiler = Mock(spec=StrategyAutoCompiler)

    with pytest.raises(DagConfigurationError, match=r"sink\.strategy\.max_source_bytes"):
        LoadConfigBuilder(strategy_compiler=compiler).build(_process({"mode": "full_refresh", "max_source_bytes": 100}))

    compiler.compile.assert_not_called()


@pytest.mark.parametrize("metadata_only", [False, True])
def test_public_process_parser_rejects_budget_before_process_construction(metadata_only: bool) -> None:
    process = _process({"mode": "full_refresh", "max_source_bytes": 100})
    original = deepcopy(process)

    with pytest.raises(DagConfigurationError, match=r"sink\.strategy\.max_source_bytes"):
        ETLProcessConfig.from_dict(process, metadata_only=metadata_only)

    assert process == original


@pytest.mark.parametrize(
    ("strategy", "expected"),
    [
        ({}, LoadStrategy.FULL_REFRESH),
        ({"mode": "full_refresh"}, LoadStrategy.FULL_REFRESH),
        ({"mode": "auto"}, LoadStrategy.FULL_REFRESH),
        ({"mode": "incremental_append"}, LoadStrategy.INCREMENTAL_APPEND),
    ],
)
def test_no_budget_legacy_strategy_and_public_parser_remain_usable(
    strategy: dict[str, Any], expected: LoadStrategy
) -> None:
    process = _process(strategy)
    original = deepcopy(process)
    built = LoadConfigBuilder().build(process)
    parsed = ETLProcessConfig.from_dict(process, metadata_only=True)

    assert built.load_strategy is expected
    assert asdict(parsed.load_config) == asdict(built)
    assert process == original


def test_unrelated_validated_file_policy_retains_its_own_budget(tmp_path: Path) -> None:
    policy = ClickHouseValidatedFilePolicy(
        work_directory=tmp_path / "not-created", max_spool_bytes=1000, max_source_bytes=100
    )

    assert policy.max_source_bytes == 100
    assert not policy.work_directory.exists()
    with pytest.raises(ValueError, match="max_source_bytes must be a positive finite integer"):
        ClickHouseValidatedFilePolicy(work_directory=tmp_path, max_spool_bytes=1000, max_source_bytes=0)
