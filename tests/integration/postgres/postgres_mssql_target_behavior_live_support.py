"""Disposable real-vendor helpers for the target-behaviour matrix."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.config import LoadConfig, LoadStrategy
from tests.integration.postgres.postgres_mssql_governance_live_support import GovernedPostgresSnapshotSource

SOURCE_SCHEMA = "dpone_src"
SOURCE_TABLE = "target_behavior_source"
TARGET_SCHEMA = "dpone_it"
STAGING_SCHEMA = "staging"
_SAFE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_GOVERNED_COLUMNS_SQL = (
    "[__dpone__load_id] varchar(26) NOT NULL, "
    "[__dpone__row_id] varchar(64) NULL, "
    "[__dpone__extracted_at] datetime2(7) NOT NULL, "
    "[__dpone__loaded_at] datetime2(7) NOT NULL"
)


class ObservedSnapshotSource(GovernedPostgresSnapshotSource):
    """Complete-snapshot test adapter exposing its physical COPY count."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.extract_calls = 0

    def extract(self, load_config: LoadConfig, last_state: Any | None):
        self.extract_calls += 1
        return super().extract(load_config, last_state)


def ensure_source(postgres: Any) -> None:
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    postgres.execute_query(
        f'CREATE TABLE IF NOT EXISTS "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" ('
        "id integer NOT NULL, partition_id integer NULL, value text NULL)"
    )
    postgres.execute_query(f'TRUNCATE TABLE "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"')
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" '
        "(id, partition_id, value) VALUES (1, 1, 'source-one'), (2, 1, 'source-two')"
    )


def load_config(
    case_id: str,
    *,
    strategy_group: str,
    target_database: str,
    tmp_path: Path,
) -> LoadConfig:
    strategy, unique_key = {
        "append_key_preserving": (LoadStrategy.INCREMENTAL_APPEND, []),
        "merge_key_preserving": (LoadStrategy.INCREMENTAL_MERGE, ["id"]),
        "destructive_replace": (LoadStrategy.FULL_REFRESH, []),
    }[strategy_group]
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=SOURCE_TABLE,
        target_database=target_database,
        target_schema=TARGET_SCHEMA,
        target_table=f"behavior_{safe_name(case_id)}"[:120],
        staging_schema=STAGING_SCHEMA,
        load_strategy=strategy,
        unique_key=unique_key,
        merge_policy="update_insert" if strategy is LoadStrategy.INCREMENTAL_MERGE else "auto",
        export_format="csv",
        compress_export=False,
        options={
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "partition_tmp_dir": str(tmp_path),
            "technical_columns": "required",
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
            "physical_design": {"indexes": {"primary_key": ["id"]}},
        },
    )


def provision(mssql: Any, config: LoadConfig, behavior: str) -> None:
    table = _table(config)
    if behavior == "temporal":
        mssql.execute_query(
            f"CREATE TABLE {table} ("
            "[id] int NOT NULL PRIMARY KEY, [partition_id] int NULL, [value] nvarchar(max) NULL, "
            f"{_GOVERNED_COLUMNS_SQL}, "
            "[valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL, "
            "[valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL, "
            "PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH (SYSTEM_VERSIONING = ON)"
        )
        return
    if behavior == "ledger":
        mssql.execute_query(
            f"CREATE TABLE {table} ([id] int NOT NULL PRIMARY KEY, [partition_id] int NULL, "
            f"[value] nvarchar(max) NULL, {_GOVERNED_COLUMNS_SQL}) "
            "WITH (LEDGER = ON (APPEND_ONLY = ON))"
        )
        return
    if behavior == "memory_optimized":
        mssql.execute_query(
            f"CREATE TABLE {table} ([id] int NOT NULL PRIMARY KEY NONCLUSTERED, "
            f"[partition_id] int NULL, [value] nvarchar(200) NULL, {_GOVERNED_COLUMNS_SQL}) "
            "WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA)"
        )
        return
    if behavior in {"graph_node", "graph_edge"}:
        kind = "NODE" if behavior == "graph_node" else "EDGE"
        mssql.execute_query(
            f"CREATE TABLE {table} ([id] int NOT NULL, [partition_id] int NULL, "
            f"[value] nvarchar(max) NULL, {_GOVERNED_COLUMNS_SQL}) AS {kind}"
        )
        return
    mssql.execute_query(
        f"CREATE TABLE {table} ([id] int NOT NULL PRIMARY KEY, [partition_id] int NULL, "
        f"[value] nvarchar(max) NULL, {_GOVERNED_COLUMNS_SQL})"
    )
    if behavior == "dml_trigger":
        mssql.execute_query(
            f"CREATE TRIGGER [{TARGET_SCHEMA}].[tr_{safe_name(config.target_table)}] ON {table} "
            "AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; RETURN; END"
        )
    elif behavior.startswith("inbound_fk_"):
        action = {
            "inbound_fk_no_action": "NO ACTION",
            "inbound_fk_cascade": "CASCADE",
            "inbound_fk_set_null": "SET NULL",
            "inbound_fk_set_default": "SET DEFAULT",
        }[behavior]
        child = f"child_{safe_name(config.target_table)}"[:120]
        mssql.execute_query(
            f"CREATE TABLE [{TARGET_SCHEMA}].[{child}] ([child_id] int NOT NULL PRIMARY KEY, "
            f"[parent_id] int NULL DEFAULT (1), CONSTRAINT [fk_{child}] FOREIGN KEY ([parent_id]) "
            f"REFERENCES {table} ([id]) ON UPDATE {action} ON DELETE {action})"
        )
    elif behavior != "ordinary_table":
        raise AssertionError(f"unsupported behavior: {behavior}")


def seed_safe_before(mssql: Any, config: LoadConfig, strategy_group: str) -> None:
    row = (1, 1, "before-one") if strategy_group == "merge_key_preserving" else (99, 9, "before-only")
    mssql.execute_query(
        f"INSERT INTO {_table(config)} ("
        "[id], [partition_id], [value], [__dpone__load_id], [__dpone__row_id], "
        "[__dpone__extracted_at], [__dpone__loaded_at]) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (*row, "01HZZZZZZZZZZZZZZZZZZZZZZZ", None, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
    )


def target_rows(mssql: Any, config: LoadConfig) -> list[dict[str, Any]]:
    # Governed attempts use SERIALIZABLE; SQL Server retains that session
    # isolation after COMMIT, while ordinary diagnostic reads of a
    # memory-optimized table require a lower isolation boundary.
    mssql.execute_query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    return mssql.get_records(
        f"SELECT [id], [partition_id], [value] FROM {_table(config)} ORDER BY [id]",
        as_dict=True,
    )


def catalog(mssql: Any, config: LoadConfig) -> dict[str, Any]:
    row = mssql.get_records(
        "SELECT temporal_type, ledger_type, is_memory_optimized, is_filetable, is_node, is_edge "
        "FROM sys.tables WHERE object_id = OBJECT_ID(?)",
        (f"{TARGET_SCHEMA}.{config.target_table}",),
        as_dict=True,
    )[0]
    triggers = mssql.get_records(
        "SELECT COUNT_BIG(*) FROM sys.triggers WHERE parent_id=OBJECT_ID(?) AND is_disabled=0",
        (f"{TARGET_SCHEMA}.{config.target_table}",),
    )
    inbound = mssql.get_records(
        "SELECT delete_referential_action_desc FROM sys.foreign_keys "
        "WHERE referenced_object_id=OBJECT_ID(?) ORDER BY name",
        (f"{TARGET_SCHEMA}.{config.target_table}",),
    )
    return {
        **dict(row),
        "enabled_trigger_count": int(triggers[0][0]),
        "inbound_fk_actions": [str(value[0]) for value in inbound],
    }


def require_catalog_behavior(values: dict[str, Any], behavior: str) -> None:
    flags = {
        "temporal": bool(values["temporal_type"]),
        "ledger": bool(values["ledger_type"]),
        "memory_optimized": bool(values["is_memory_optimized"]),
        "filetable": bool(values["is_filetable"]),
        "graph_node": bool(values["is_node"]),
        "graph_edge": bool(values["is_edge"]),
    }
    expected_flag = behavior if behavior in flags else None
    assert [name for name, value in flags.items() if value] == ([expected_flag] if expected_flag is not None else [])
    assert values["enabled_trigger_count"] == (1 if behavior == "dml_trigger" else 0)
    expected_action = {
        "inbound_fk_no_action": "NO_ACTION",
        "inbound_fk_cascade": "CASCADE",
        "inbound_fk_set_null": "SET_NULL",
        "inbound_fk_set_default": "SET_DEFAULT",
    }.get(behavior)
    assert values["inbound_fk_actions"] == ([expected_action] if expected_action is not None else [])


def staging_count(mssql: Any, config: LoadConfig) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) FROM sys.tables AS t JOIN sys.schemas AS s ON s.schema_id=t.schema_id "
        "WHERE s.name=? AND t.name LIKE ?",
        (STAGING_SCHEMA, f"stg_{config.target_table}_%"),
    )
    return int(rows[0][0])


def observe_filetable_platform_boundary(
    mssql: Any,
    case_id: str,
    strategy_group: str,
    recorder: RouteLiveObservationRecorder,
) -> None:
    capabilities = mssql.get_records(
        "SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version, "
        "CAST(SERVERPROPERTY('FilestreamEffectiveLevel') AS int) AS filestream_level, @@VERSION AS version_text",
        as_dict=True,
    )[0]
    assert int(capabilities["filestream_level"]) == 0
    assert "Linux" in str(capabilities["version_text"])
    probe = f"filetable_{safe_name(case_id)}"[:120]
    ddl_error: Exception | None = None
    try:
        mssql.execute_query(
            f"CREATE TABLE [{TARGET_SCHEMA}].[{probe}] AS FILETABLE WITH (FILETABLE_DIRECTORY = N'{probe}')"
        )
    except Exception as exc:  # noqa: BLE001 - vendor capability is asserted below.
        ddl_error = exc
    assert ddl_error is not None
    assert mssql.get_records(
        "SELECT COUNT_BIG(*) FROM sys.tables WHERE object_id = OBJECT_ID(?)",
        (f"{TARGET_SCHEMA}.{probe}",),
    ) == [(0,)]
    recorder.observe_case(
        "target_behavior",
        case_id,
        before_image=[],
        after_image=[],
        observations={
            "strategy_group": strategy_group,
            "vendor_platform": "sqlserver_linux",
            "filestream_effective_level": 0,
            "filetable_catalog_object_provisionable": False,
            "filetable_create_error": type(ddl_error).__name__,
            "production_static_policy": "typed_reject_if_catalog_object_exists",
        },
    )


def exception_text(error: BaseException) -> str:
    values: list[str] = []
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        values.append(str(current))
        current = current.__cause__ or current.__context__
    return " | ".join(values)


def safe_name(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    if _SAFE.fullmatch(token) is None:
        raise ValueError("target_behavior.identifier_invalid")
    return token


def _table(config: LoadConfig) -> str:
    return f"[{TARGET_SCHEMA}].[{safe_name(config.target_table)}]"


__all__ = [
    "ObservedSnapshotSource",
    "STAGING_SCHEMA",
    "TARGET_SCHEMA",
    "catalog",
    "ensure_source",
    "exception_text",
    "load_config",
    "observe_filetable_platform_boundary",
    "provision",
    "require_catalog_behavior",
    "safe_name",
    "seed_safe_before",
    "staging_count",
    "target_rows",
]
