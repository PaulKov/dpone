"""Public admission preserves an enforceable source-byte limit."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.dag.config import ETLProcessConfig, LoadConfigBuilder
from dpone.runtime.clickhouse_file_stage_contract import ClickHouseValidatedFilePolicy


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


def test_public_builder_preserves_valid_full_refresh_budget_without_changing_input() -> None:
    process = _process({"mode": "full_refresh", "max_source_bytes": 100})
    original = deepcopy(process)

    built = LoadConfigBuilder().build(process)

    assert built.load_strategy is LoadStrategy.FULL_REFRESH
    assert built.options[SOURCE_BYTE_BUDGET_OPTION] == 100
    assert process == original


def test_public_metadata_parser_preserves_budget() -> None:
    process = _process({"mode": "full_refresh", "max_source_bytes": 100})
    original = deepcopy(process)

    parsed = ETLProcessConfig.from_dict(process, metadata_only=True)

    assert parsed.load_config.options[SOURCE_BYTE_BUDGET_OPTION] == 100
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
