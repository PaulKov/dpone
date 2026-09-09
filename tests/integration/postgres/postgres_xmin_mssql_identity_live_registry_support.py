"""Reusable fixture mechanics for MSSQL target-identity vendor-live cases."""

from __future__ import annotations

import copy
import re
import uuid
from contextlib import contextmanager, suppress
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.contracts.incremental_snapshot import snapshot_token_digest
from dpone.ports.source_state_storage import SourceStateKey
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_options import (
    acquire_target_lock,
    target_lock_resource,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_extract import (
    PostgresXMinExtractStrategy,
)
from dpone.runtime.state.mssql import MSSQLXMinStateStorage
from dpone.runtime.state.xmin_storage import XMinState
from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    bind_factual_mssql_database_authority,
    bind_factual_postgres_source_authority,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    STAGING_SCHEMA,
    STATE_SCHEMA,
    TARGET_SCHEMA,
    TARGET_TABLE,
    QuietIntegrationLogger,
    SnapshotRouteEnvironment,
)

CASE_SENSITIVE_COLLATION = "Latin1_General_100_CS_AS"
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_UTC = timezone.utc  # noqa: UP017 - keep compatibility with the supported Python baseline.


def strategy(
    route: SnapshotRouteEnvironment,
    *,
    target: Any,
    database: str,
    table: str,
    process: str,
) -> tuple[PostgresXMinExtractStrategy, MSSQLXMinStateStorage, LoadConfig]:
    state = state_storage(route, target_database=database)
    xmin = PostgresXMinExtractStrategy(
        route.postgres,
        state,
        QuietIntegrationLogger(),
        sink_connector=target,
    )
    load_config = config(
        route.load_config,
        database=database,
        schema=TARGET_SCHEMA,
        table=table,
        process=process,
    )
    bind_factual_postgres_source_authority(xmin, route.postgres, load_config=load_config)
    return xmin, state, load_config


def state_storage(route: SnapshotRouteEnvironment, *, target_database: str) -> MSSQLXMinStateStorage:
    storage = MSSQLXMinStateStorage(
        route.state,
        database=route.state_database,
        schema=STATE_SCHEMA,
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        run_table="dpone_run_state",
        audit_table="dpone_load_audit",
        atomicity="target_atomic",
        provisioning="external",
    )
    bind_factual_mssql_database_authority(
        storage,
        route.master,
        target_database=target_database,
        staging_database=target_database,
        state_database=route.state_database,
    )
    return storage


def config(template: LoadConfig, *, database: str, schema: str, table: str, process: str) -> LoadConfig:
    options = copy.deepcopy(template.options)
    identity = dict(options.get("state_identity") or {})
    identity["process"] = process
    options["state_identity"] = identity
    return replace(
        template,
        target_database=database,
        target_schema=schema,
        target_table=table,
        staging_database=database,
        options=options,
    )


def set_process(config: LoadConfig, process: str) -> None:
    options = copy.deepcopy(config.options)
    identity = dict(options.get("state_identity") or {})
    identity["process"] = process
    options["state_identity"] = identity
    config.options = options


def state_key(strategy: PostgresXMinExtractStrategy, config: LoadConfig) -> SourceStateKey:
    schema = list(strategy.fetch_schema(config))
    return strategy._snapshot_extractor.state_key(config, schema)


def commit_owner(
    executor: Any,
    state: MSSQLXMinStateStorage,
    key: SourceStateKey,
    xmin: int,
    label: str,
) -> dict[str, Any]:
    load_id = f"identity-live-{label}-{uuid.uuid4().hex}"
    started = False
    try:
        executor.begin()
        started = True
        executor.execute_query("SET XACT_ABORT ON")
        executor.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        acquire_target_lock(
            executor,
            target_lock_resource(key.target_identity),
            database=key.target_database,
            timeout_ms=60_000,
        )
        state.assert_physical_target_identity(executor=executor, key=key)
        state.assert_or_transfer_target_authority(executor=executor, key=key, authority=None)
        outcome = state.compare_and_set_with_receipt(
            executor=executor,
            key=key,
            expected=None,
            candidate=XMinState(xmin, datetime.now(_UTC), is_initial=True),
            load_id=load_id,
            snapshot_token=snapshot_token_digest(f"identity-live:{label}:{xmin}"),
        )
        executor.commit_transaction()
        started = False
        return {
            "load_id": load_id,
            "receipt_id": outcome.receipt_id,
            "candidate_revision": outcome.candidate_revision,
        }
    except Exception:
        if started:
            executor.rollback()
        raise


def pre_source_failure(
    route: SnapshotRouteEnvironment,
    *,
    target: Any,
    database: str,
    table: str,
    expected: str,
) -> dict[str, Any]:
    xmin, _state, load_config = strategy(
        route,
        target=target,
        database=database,
        table=table,
        process=f"integration.target_identity.failure.{table}",
    )
    fetch_schema_calls = 0

    def forbidden_fetch_schema(_config: Any) -> list[tuple[str, str]]:
        nonlocal fetch_schema_calls
        fetch_schema_calls += 1
        raise AssertionError("source_schema_io_observed_before_registry_failure")

    xmin.fetch_schema = forbidden_fetch_schema
    try:
        xmin.get_state(load_config)
    except Exception as exc:  # noqa: BLE001 - stable production diagnostic is asserted.
        if expected not in str(exc):
            raise
    else:  # pragma: no cover - a malformed registry must never be admitted.
        raise AssertionError(f"expected pre-source registry failure: {expected}")
    assert fetch_schema_calls == 0
    return {
        "failed_closed": True,
        "diagnostic_contains": expected,
        "fetch_schema_calls": fetch_schema_calls,
    }


@contextmanager
def case_sensitive_target(
    route: SnapshotRouteEnvironment,
) -> Any:
    database = safe(f"dpone_it_xmin_cs_{uuid.uuid4().hex[:10]}")
    target = None
    route.master.execute_query(f"CREATE DATABASE [{database}] COLLATE {CASE_SENSITIVE_COLLATION}")
    try:
        target = mssql_connector(database=database)
        wait_until_ready("mssql case-sensitive target", lambda: target.get_records("SELECT 1"))
        target.execute_query(f"CREATE SCHEMA [{TARGET_SCHEMA}] AUTHORIZATION [dbo]")
        target.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        execute_target_fixture(target, database)
        bindings = {"Foo": uuid.uuid4(), "foo": uuid.uuid4()}
        for table, binding in bindings.items():
            target.execute_query(
                f"INSERT INTO [{database}].[dbo].[dpone_target_identity] "
                "([binding_id], [schema_name], [table_name]) VALUES (?, ?, ?)",
                (str(binding), TARGET_SCHEMA, table),
            )
            target.execute_query(
                f"SELECT TOP (0) * INTO [{database}].[{TARGET_SCHEMA}].[{table}] "
                f"FROM [{database}].[{TARGET_SCHEMA}].[{TARGET_TABLE}]"
            )
            target.execute_query(
                f"CREATE UNIQUE NONCLUSTERED INDEX [ux_{table}_metric_code] "
                f"ON [{database}].[{TARGET_SCHEMA}].[{table}] ([metric_code]) "
                "WITH (DATA_COMPRESSION = NONE)"
            )
        yield target, database, bindings
    finally:
        if target is not None:
            with suppress(Exception):
                target.close()
        with suppress(Exception):
            route.master.execute_query(f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        with suppress(Exception):
            route.master.execute_query(f"DROP DATABASE [{database}]")


def execute_target_fixture(target: Any, database: str) -> None:
    fixture = Path(__file__).with_name("sql") / "postgres_xmin_mssql_target.sql"
    rendered = fixture.read_text(encoding="utf-8").replace("__DATABASE__", safe(database))
    for statement in rendered.split("-- dpone:statement"):
        if statement := statement.strip():
            target.execute_query(statement)


def active_owners(route: SnapshotRouteEnvironment) -> list[dict[str, Any]]:
    return route.state.get_records(
        f"SELECT state_key, target_identity, target_table "
        f"FROM [{route.state_database}].[{STATE_SCHEMA}].[dpone_source_state] "
        "WHERE superseded_at_utc IS NULL",
        as_dict=True,
    )


def receipt_count(route: SnapshotRouteEnvironment) -> int:
    row = route.state.get_records(
        f"SELECT COUNT_BIG(*) AS receipt_count FROM [{route.state_database}].[{STATE_SCHEMA}].[dpone_commit_receipt]",
        as_dict=True,
    )[0]
    return int(row["receipt_count"])


def database_collation(connector: Any) -> str:
    row = connector.get_records(
        "SELECT CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation')) AS collation_name",
        as_dict=True,
    )[0]
    value = str(row["collation_name"] or "")
    if not value:
        raise AssertionError("mssql_database_collation_unavailable")
    return value


def require_collation_mode(collation: str, mode: str) -> None:
    if f"_{mode.upper()}_" not in collation.upper():
        raise AssertionError(f"expected_{mode.lower()}_collation:{collation}")


def target_coordinates(config: LoadConfig) -> tuple[str | None, str, str]:
    return config.target_database, config.target_schema, config.target_table


def safe(value: str) -> str:
    if _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"unsafe integration identifier: {value!r}")
    return value


__all__ = [
    "CASE_SENSITIVE_COLLATION",
    "active_owners",
    "case_sensitive_target",
    "commit_owner",
    "config",
    "database_collation",
    "pre_source_failure",
    "receipt_count",
    "require_collation_mode",
    "set_process",
    "state_key",
    "target_coordinates",
]
