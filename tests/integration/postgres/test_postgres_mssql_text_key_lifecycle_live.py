"""Vendor-live exact text-key lifecycle across generic MSSQL strategies."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from tests.integration.postgres.postgres_live_support import (
    ensure_mssql_database_and_schemas,
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    QuietIntegrationLogger,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

_SOURCE_SCHEMA = "dpone_src"
_SOURCE_TABLE = "text_key_lifecycle_source"
_TARGET_SCHEMA = "dpone_it"
_STAGING_SCHEMA = "staging"
_EVIDENCE = Path("test_artifacts/live_certification/postgres_mssql_text_key_lifecycle.json")
_TEXT_KEYS = (
    "",
    "tab\tline\n",
    "\x1dE",
    "\x1dP-marker-prefix",
    "A",
    "a",
    "é",
    "e\u0301",
    "trail",
    "Ａ",
    "ｱ",
    "ア",
)


def _config(case_id: str, tmp_path: Path, strategy: LoadStrategy, **values: Any) -> LoadConfig:
    options = {
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "required",
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
        "physical_design": {
            "columns": {
                "key_text": {"target_type": {"mssql": "nvarchar(128)"}},
                "value_text": {"target_type": {"mssql": "nvarchar(256)"}},
            }
        },
    }
    options.update(values.pop("options", {}))
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=_SOURCE_SCHEMA,
        source_table=_SOURCE_TABLE,
        target_schema=_TARGET_SCHEMA,
        target_table=f"text_key_{case_id}",
        staging_schema=_STAGING_SCHEMA,
        load_strategy=strategy,
        export_format="csv",
        compress_export=False,
        options=options,
        **values,
    )


def _set_rows(postgres: Any, values: dict[str, str]) -> None:
    postgres.execute_query(f'TRUNCATE TABLE "{_SOURCE_SCHEMA}"."{_SOURCE_TABLE}"')
    for key, value in values.items():
        postgres.execute_query(
            f'INSERT INTO "{_SOURCE_SCHEMA}"."{_SOURCE_TABLE}" (key_text, value_text) VALUES (%s, %s)',
            (key, value),
        )


def _load(runner: GovernedStandardEtlRunner, config: LoadConfig):
    return runner.run(config, label=f"text_key_{config.target_table}")


def _rows(mssql: Any, config: LoadConfig) -> list[dict[str, Any]]:
    return mssql.get_records(
        f"SELECT * FROM [{_TARGET_SCHEMA}].[{config.target_table}]",
        as_dict=True,
    )


def _active_values(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {
        str(row["key_text"]): str(row["value_text"])
        for row in rows
        if bool(row.get("__dpone__is_current", True)) and row.get("__dpone__deleted_at") is None
    }


def _sha(rows: list[dict[str, Any]]) -> str:
    serializable = sorted(
        ({key: str(value) if value is not None else None for key, value in row.items()} for row in rows),
        key=lambda row: (str(row.get("key_text")), str(row.get("__dpone__valid_from_at", ""))),
    )
    payload = json.dumps(serializable, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _staging_count(mssql: Any, config: LoadConfig) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        (_STAGING_SCHEMA, f"stg_{config.target_table}_%"),
    )
    return int(rows[0][0])


def _target_exists(mssql: Any, config: LoadConfig) -> bool:
    return bool(
        mssql.get_records(
            "SELECT 1 WHERE OBJECT_ID(?, 'U') IS NOT NULL",
            (f"{_TARGET_SCHEMA}.{config.target_table}",),
        )
    )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_text_key_lifecycle_is_byte_exact_for_every_keyed_generic_strategy(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Control markers and BIN2 distinctions survive; trailing spaces fail closed."""

    postgres = postgres_connector()
    mssql = governed_mssql_live_campaign.target
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_mssql_database_and_schemas()
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{_SOURCE_SCHEMA}"')
    postgres.execute_query(f'DROP TABLE IF EXISTS "{_SOURCE_SCHEMA}"."{_SOURCE_TABLE}"')
    postgres.execute_query(
        f'CREATE TABLE "{_SOURCE_SCHEMA}"."{_SOURCE_TABLE}" (key_text text NOT NULL, value_text text NULL)'
    )
    initial = {key: f"initial-{index}" for index, key in enumerate(_TEXT_KEYS)}
    changed = {key: f"changed-{index}" for index, key in enumerate(_TEXT_KEYS)}
    cases = [
        (
            "append_only_new",
            _config(
                "append_only_new",
                tmp_path,
                LoadStrategy.INCREMENTAL_APPEND,
                only_new_rows=True,
                unique_key=["key_text"],
            ),
        ),
        *[
            (
                f"merge_{policy}",
                _config(
                    f"merge_{policy}",
                    tmp_path,
                    LoadStrategy.INCREMENTAL_MERGE,
                    unique_key=["key_text"],
                    merge_policy=policy,
                ),
            )
            for policy in ("update_insert", "delete_insert")
        ],
        (
            "snapshot_soft_delete",
            _config(
                "snapshot_soft_delete",
                tmp_path,
                LoadStrategy.SNAPSHOT_DIFF,
                unique_key=["key_text"],
                options={"diff": {"compare": "all_columns", "delete_policy": "soft_delete"}},
            ),
        ),
        (
            "scd2_expire",
            _config(
                "scd2_expire",
                tmp_path,
                LoadStrategy.SCD2,
                unique_key=["key_text"],
                options={"scd2": {"delete_policy": "expire"}},
            ),
        ),
    ]
    evidence: list[dict[str, Any]] = []
    for case_id, config in cases:
        config.target_database = governed_mssql_live_campaign.target_database
        runner = GovernedStandardEtlRunner(
            governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            ),
            postgres,
            logger=QuietIntegrationLogger(),
            source_type=GovernedPostgresSnapshotSource,
        )
        mssql.execute_query(f"DROP TABLE IF EXISTS [{_TARGET_SCHEMA}].[{config.target_table}]")
        assert not _target_exists(mssql, config)
        before_image = {"target_exists": False, "rows": []}
        _set_rows(postgres, initial)
        baseline = _load(runner, config)
        assert _active_values(_rows(mssql, config)) == initial
        actions = [{"name": "baseline", "inserted": baseline.inserted_rows}]
        if case_id == "append_only_new":
            _set_rows(postgres, changed)
            rerun = _load(runner, config)
            assert rerun.inserted_rows == 0
            assert _active_values(_rows(mssql, config)) == initial
            actions.append({"name": "identical_keys", "inserted": rerun.inserted_rows})
        elif case_id == "snapshot_soft_delete":
            absent_key = "A"
            without_a = {key: value for key, value in changed.items() if key != absent_key}
            _set_rows(postgres, without_a)
            deleted = _load(runner, config)
            deleted_row = next(row for row in _rows(mssql, config) if row["key_text"] == absent_key)
            first_deleted_at = deleted_row["__dpone__deleted_at"]
            assert first_deleted_at is not None
            assert "a" in _active_values(_rows(mssql, config))
            repeated = _load(runner, config)
            repeated_row = next(row for row in _rows(mssql, config) if row["key_text"] == absent_key)
            assert repeated_row["__dpone__deleted_at"] == first_deleted_at
            _set_rows(postgres, changed)
            reactivated = _load(runner, config)
            assert _active_values(_rows(mssql, config)) == changed
            actions.extend(
                [
                    {"name": "delete_A_not_a", "soft_deleted": deleted.soft_deleted_rows},
                    {"name": "repeat_absence", "soft_deleted": repeated.soft_deleted_rows},
                    {"name": "reactivate_A", "reactivated": reactivated.reactivated_rows},
                ]
            )
        else:
            _set_rows(postgres, changed)
            rerun = _load(runner, config)
            assert _active_values(_rows(mssql, config)) == changed
            if case_id == "scd2_expire":
                history = _rows(mssql, config)
                assert len(history) == len(_TEXT_KEYS) * 2
                assert sum(row["key_text"] == "A" for row in history) == 2
                assert sum(row["key_text"] == "a" for row in history) == 2
            actions.append({"name": "update_exact_keys", "updated": rerun.updated_rows})
        rows = _rows(mssql, config)
        assert _staging_count(mssql, config) == 0
        route_live_recorder.observe_case(
            "text_key_lifecycle",
            case_id,
            before_image=before_image,
            after_image={"target_exists": True, "rows": rows},
            observations={
                "key_count": len(_TEXT_KEYS),
                "actions": actions,
                "staging_objects_after": 0,
            },
        )
        evidence.append(
            {
                "case_id": case_id,
                "passed": True,
                "key_count": len(_TEXT_KEYS),
                "target_sha256": _sha(rows),
                "actions": actions,
                "staging_objects_after": 0,
            }
        )
    rejection_cases = (
        (
            "reject_trailing_space_sql_equivalence",
            "update_insert",
            {"trail": "one", "trail ": "two"},
            SnapshotReconciliationError,
            "code",
            "mssql_native_projection.text_key_trailing_space_unsupported",
        ),
        (
            "reject_shadow_swap_physical_preservation",
            "shadow_swap",
            initial,
            MSSQLStrategyContractError,
            "blocker",
            "mssql.strategy.incremental_merge.shadow_swap_physical_preservation",
        ),
    )
    absent_image = {"target_exists": False, "rows": []}
    for rejection_id, policy, source_rows, error_type, error_attribute, expected_error in rejection_cases:
        rejection = _config(
            rejection_id,
            tmp_path,
            LoadStrategy.INCREMENTAL_MERGE,
            unique_key=["key_text"],
            merge_policy=policy,
        )
        rejection.target_database = governed_mssql_live_campaign.target_database
        rejection_runner = GovernedStandardEtlRunner(
            governed_mssql_live_campaign.route(
                target_schema=rejection.target_schema,
                target_table=rejection.target_table,
            ),
            postgres,
            logger=QuietIntegrationLogger(),
            source_type=GovernedPostgresSnapshotSource,
        )
        mssql.execute_query(f"DROP TABLE IF EXISTS [{_TARGET_SCHEMA}].[{rejection.target_table}]")
        _set_rows(postgres, source_rows)
        with pytest.raises(error_type) as raised:
            _load(rejection_runner, rejection)
        error_code = str(getattr(raised.value, error_attribute))
        assert error_code == expected_error
        assert _staging_count(mssql, rejection) == 0
        assert not _target_exists(mssql, rejection)
        route_live_recorder.observe_case(
            "text_key_lifecycle",
            rejection_id,
            before_image=absent_image,
            after_image=absent_image,
            observations={"error_code": error_code, "staging_objects_after": 0},
        )
        evidence.append(
            {
                "case_id": rejection_id,
                "passed": True,
                "result": "rejected_before_target_transaction",
                "error_code": error_code,
                "target_before_image": "absent",
                "target_after_image": "absent",
                "staging_objects_after": 0,
            }
        )
    expected = [case_id for case_id, _config_value in cases] + [case_id for case_id, *_rest in rejection_cases]
    assert [item["case_id"] for item in evidence] == expected
    _EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    _EVIDENCE.write_text(
        json.dumps(
            {
                "schema_version": "dpone.postgres_mssql.text_key_lifecycle.v1",
                "status": "local_certification_passed",
                "release_ready": False,
                "connector_doubles": False,
                "identity_policy": (
                    "BIN2 UNIQUE authority; case/accent/width/kana/code-unit exact; "
                    "trailing U+0020 rejected before target transaction because SQL equality collapses it"
                ),
                "expected_case_ids": expected,
                "cases": evidence,
            },
            indent=2,
            ensure_ascii=True,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
