"""Synthetic corpus and explicitly configured disposable native TLS endpoints.

Only the fixture administrator creates objects; the production route receives
its usual connector and never needs source write authority. One INSERT with
optimize_on_insert=0 creates one part per daily partition, preserving the raw
version corpus without changing production read settings or server-wide merges.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.integration.mssql.mssql_live_support import clickhouse_connector

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sources.clickhouse import ClickHouseSource


@dataclass(frozen=True)
class RawCase:
    """Ordered business columns and hand-authored row multiset, without provenance."""

    columns: tuple[tuple[str, str, str], ...]
    rows: tuple[tuple[Any, ...], ...]

    def window_rows(self, start: datetime, end: datetime) -> tuple[tuple[Any, ...], ...]:
        return tuple(row for row in self.rows if start <= row[3] < end)

    def source_ddl(self) -> str:
        return ",".join(f"`{name}` {source}" for name, source, _ in self.columns)

    def target_ddl(self) -> str:
        return ",".join(f"[{name}] {target}" for name, _, target in self.columns)

    def target_projection(self) -> str:
        return ",".join(f"[{name}]" for name, _, _ in self.columns)


def raw_case(width: int) -> RawCase:
    """Seven-column narrow and exactly 100-column mixed nullable wide profiles."""
    if width not in {7, 100}:
        raise ValueError("unsupported synthetic width")
    columns = [
        ("row_key", "Int64", "bigint NOT NULL"),
        ("version", "UInt32", "bigint NOT NULL"),
        ("is_deleted", "UInt8", "bigint NOT NULL"),
        ("happened_at", "DateTime64(6, 'UTC')", "datetime2(6) NOT NULL"),
        ("text_value", "Nullable(String)", "nvarchar(max) NULL"),
        ("nullable_key", "Nullable(Int64)", "bigint NULL"),
        ("ratio", "Nullable(Float64)", "float(53) NULL"),
    ]
    rows = [
        (9, 1, 0, datetime(2024, 1, 1), "outside", 9, 0.0),
        (1, 1, 0, datetime(2024, 1, 2), "λ\tline\n雪", -(1 << 63), -1.5),
        (1, 2, 0, datetime(2024, 1, 2, 12), "", None, 0.0),
        (1, 3, 1, datetime(2024, 1, 2, 23, 59, 59, 999999), None, (1 << 63) - 1, None),
        (1, 3, 1, datetime(2024, 1, 2, 23, 59, 59, 999999), None, (1 << 63) - 1, None),
        (8, 1, 0, datetime(2024, 1, 3), "exclusive end", 8, 2.5),
    ]
    for index in range(width - 7):
        is_text = index % 2 == 0
        columns.append(
            (
                f"extra_{index:03d}",
                "Nullable(String)" if is_text else "Nullable(Int64)",
                "nvarchar(max) NULL" if is_text else "bigint NULL",
            )
        )
        rows = [row + (None if index % 3 == 0 else f"雪-{index}" if is_text else -index,) for row in rows]
    return RawCase(tuple(columns), tuple(rows))


def tls_source():
    """Fail an enabled cell on missing authority instead of silently skipping it."""
    required = ("DPONE_IT_CH_HOST", "DPONE_IT_CH_CA_CERT")
    for name in required:
        if not os.environ.get(name):
            pytest.fail(f"raw snapshot live requires explicit {name}")
    if not (os.environ.get("DPONE_IT_CH_PORT") or os.environ.get("DPONE_IT_CH_PORT_FORWARD")):
        pytest.fail("raw snapshot live requires explicit native TLS port")
    if not Path(os.environ["DPONE_IT_CH_CA_CERT"]).is_file():
        pytest.fail("raw snapshot live CA file unavailable")
    source = clickhouse_connector()
    source.secure = True
    source.ca_cert = os.environ["DPONE_IT_CH_CA_CERT"]
    source.connect_timeout = 5
    source.send_receive_timeout = 30
    return source


def route_process(
    tmp_path,
    source_connector,
    target_connector,
    sink,
    source_table,
    target_table,
    backend,
    window="full",
    replica_scope="single_server",
):
    """Hydrate the same application composition used by ordinary route execution."""
    execution = {
        "import_backend": backend,
        "verification_backend": "target_local",
        "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": 1},
        "native_chunks": {
            "max_total_encoded_bytes": 8 << 20,
            "stage_allocated_bytes_stop_threshold": 1 << 30,
            "max_rows": 2,
            "max_bytes": 4 << 20,
            "max_row_bytes": 1 << 20,
            "max_pending": 1,
            "max_staging_tables": 32,
        },
    }
    if backend == "mssql_sqlclient":
        execution["layout_version"] = 2
    options = {
        "source_type": "clickhouse",
        "sink_type": "mssql",
        "lineage": False,
        "physical_design": {"columns": {"is_deleted": {"target_type": {"mssql": "bigint"}}}},
        "native_transfer": {
            "source_snapshot": {"mode": "exact_raw_rows", "replica_scope": replica_scope},
            "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
            "execution": execution,
        },
    }
    if window != "full":
        options["mssql_native_window"] = {
            "column": "happened_at",
            "anchor": "data_interval_end",
            "lookback": "P1D",
            "chunk_interval": "P1D",
            "timezone": "UTC",
        }
        options["interval"] = {
            "interval_end": "2024-01-03T00:00:00Z" if window == "nonempty" else "2030-01-02T00:00:00Z"
        }
    config = LoadConfig(
        "source",
        "target",
        source_connector.database,
        source_table,
        "dbo",
        target_table,
        target_database=target_connector.database,
        staging_database=target_connector.database,
        staging_schema="dbo",
        load_strategy=LoadStrategy.FULL_REFRESH if window == "full" else LoadStrategy.PARTITION_REPLACE,
        options=options,
    )
    process = SimpleNamespace(
        name="synthetic_raw_snapshot_live",
        load_config=config,
        source_obj=ClickHouseSource(source_connector, sink.logger, sink_connector=target_connector),
        sink_obj=sink,
        load_identity_service=LoadIdentityService(),
        ensure_runtime_bindings=lambda: None,
        raw_config={
            "runtime": {
                "storage": {
                    **{
                        key: str(tmp_path / name)
                        for key, name in (
                            ("work_dir", "work"),
                            ("checkpoint_dir", "state"),
                            ("evidence_dir", "evidence"),
                            ("debug_dir", "debug"),
                        )
                    },
                    "min_free_bytes": 1,
                }
            }
        },
    )
    return process, config


@contextmanager
def preserve_on_failure(cleanups, retained_description):
    """Preserve all authority on failure; independently clean exact settled objects."""
    try:
        yield
    except BaseException as error:
        error.add_note("Fixture preserved for recovery: " + retained_description)
        raise
    else:
        errors = []
        for cleanup in cleanups:
            try:
                cleanup()
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("synthetic fixture cleanup failures", errors)


@contextmanager
def preserving_governed_route(target, *, target_database, target_schema, target_table):
    """Use the existing governance producers, retaining the catalog on test failure.

    The standard governed_mssql_route unconditionally drops its state database.
    Fault tests instead preserve it with the target/stages whenever an assertion
    or recovery fails; operator reconciliation then has its complete authority.
    """
    from tools import mssql_stress_governance as governance

    state_database = "dpone_gov_" + uuid.uuid4().hex[:16]
    master = governance.mssql_connector_for_database(target, "master")
    state = None
    try:
        master.execute_query(f"CREATE DATABASE [{state_database}]")

        def drop_settled_state_database():
            # ODBC pools retain sessions after close; this unique fixture database
            # is disposable only after every body/custody assertion has passed.
            master.execute_query(f"ALTER DATABASE [{state_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            master.execute_query(f"DROP DATABASE [{state_database}]")

        with preserve_on_failure(
            [drop_settled_state_database],
            f"state_database={state_database}; target={target_table}",
        ):
            state = governance.mssql_connector_for_database(target, state_database)
            try:
                state.execute_query(f"CREATE SCHEMA [{governance.STATE_SCHEMA}] AUTHORIZATION [dbo]")
                ddl = governance.render_generic_transaction_catalog_ddl(
                    database=state_database,
                    schema=governance.STATE_SCHEMA,
                )
                for batch in governance._GO_SEPARATOR.split(ddl):
                    if batch.strip():
                        state.execute_query(batch)
                governance.ensure_target_identity_registry(target, database=target_database)
                storage = governance.MssqlGenericTransactionStateStorage(
                    state,
                    database=state_database,
                    schema=governance.STATE_SCHEMA,
                )
                governance.bind_factual_mssql_database_authority(
                    storage,
                    master,
                    target_database=target_database,
                    staging_database=target_database,
                    state_database=state_database,
                )
                campaign = governance.GovernedMssqlCampaign(
                    target,
                    state,
                    target_database,
                    state_database,
                    storage,
                )
                yield campaign.route(target_schema=target_schema, target_table=target_table)
            finally:
                if state is not None:
                    state.close()
    finally:
        master.close()


def changed_source_rows(case, change):
    """Independent expected business multiset after a synthetic committed change."""
    if change == "patch":
        return tuple(row[:4] + ("patched",) + row[5:] if row[:2] == (1, 1) else row for row in case.rows)
    if change == "mask":
        return tuple(row for row in case.rows if row[:2] != (1, 2))
    if change == "insert":
        return case.rows + ((77, 4, 0, datetime(2024, 1, 4), "new arrival", None, None),)
    if change == "merge":
        return (case.rows[0], case.rows[3], case.rows[5])
    raise ValueError("unknown synthetic source change")


def require_causal_source_rejection(error):
    """Do not count normalized vendor or cleanup failures as provenance proof."""
    assert str(error) in {
        "mssql_native.source_provenance_incomplete",
        "mssql_native.source_snapshot_profile_changed",
    }
    assert error.__context__ is None, "underlying vendor or cleanup failure is not provenance evidence"
    assert not getattr(error, "__notes__", ()), "cleanup failure is not provenance evidence"


def commit_source_change(control, table, case, change):
    """Commit a table-local change and prove it happened before SELECT release."""
    from collections import Counter

    expected = changed_source_rows(case, change)
    if change == "patch":
        control.execute_query(
            f"UPDATE `{table}` SET text_value='patched' WHERE row_key=1 AND version=1 "
            "SETTINGS allow_experimental_lightweight_update=1"
        )
    elif change == "mask":
        control.execute_query(
            f"DELETE FROM `{table}` WHERE row_key=1 AND version=2 "
            "SETTINGS lightweight_deletes_sync=2, lightweight_delete_mode='alter_update'"
        )
    elif change == "insert":
        control.execute_query(f"INSERT INTO `{table}` SETTINGS optimize_on_insert=0 VALUES", [expected[-1]])
    else:
        control.execute_query(f"OPTIMIZE TABLE `{table}` FINAL")
    visible = control.get_records(
        f"SELECT row_key,version,is_deleted,text_value FROM `{table}` "
        "SETTINGS final=0,apply_patch_parts=1,apply_deleted_mask=1"
    )
    assert Counter(visible) == Counter((row[0], row[1], row[2], row[4]) for row in expected)
    parts = control.get_records(
        "SELECT countIf(startsWith(partition_id,'patch-')),countIf(has_lightweight_delete=1) "
        "FROM system.parts WHERE database=%(database)s AND table=%(table)s AND active",
        {"database": control.database, "table": table},
    )[0]
    if change == "patch":
        assert parts[0] > 0, "patch overlay must exist at the acquisition barrier"
    if change == "mask":
        assert parts[1] > 0, "delete mask must exist at the acquisition barrier"
    return {"patch_parts_before_select": parts[0], "masked_parts_before_select": parts[1]}
