"""Snapshot-consistent source-domain guards for PostgreSQL → MSSQL."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.sources.strategies.postgres.postgres_mssql_source_value_guard import (
    PostgresMssqlSourceValueError,
    PostgresMssqlSourceValueGuard,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    issue_repeatable_read_snapshot_lease,
)
from dpone.runtime.sources.strategies.postgres.postgres_whole_file_export_service import (
    PostgresWholeFileExportService,
)


class _Connector:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.connection = object()
        self.rows = list(rows or [])
        self.events: list[str] = []
        self.queries: list[str] = []

    def get_records(self, query: str, *, as_dict: bool = False):
        assert as_dict is True
        self.events.append("guard")
        self.queries.append(query)
        return list(self.rows)

    def rollback(self) -> None:
        self.events.append("rollback")

    def copy_to_file(self, **_kwargs):  # pragma: no cover - guarded failure invariant.
        self.events.append("copy")
        raise AssertionError("COPY must not start after a failed source-domain guard")


class _Logger:
    def log_etl_progress(self, *_args, **_kwargs) -> None:
        return None


class _Strategy:
    def __init__(self, connector: _Connector) -> None:
        self.connector = connector
        self.logger = _Logger()

    def _targets_mssql(self, _load_config) -> bool:
        return True

    def _effective_postgres_file_wire(self, _load_config) -> tuple[str, bool]:
        return "mssql-delimited", False

    def _new_extraction_lifecycle(self) -> ExtractionLifecycleAuthority:
        return ExtractionLifecycleAuthority()

    def _begin_repeatable_read_snapshot(
        self,
        lifecycle: ExtractionLifecycleAuthority,
    ):
        self.connector.events.append("begin_repeatable_read")
        return issue_repeatable_read_snapshot_lease(
            connector=self.connector,
            lifecycle=lifecycle,
            raw_snapshot_token="10:20:",
        )

    def _render_query(self, _connector, query: str) -> str:
        return query

    def _verify_postgres_source_authority(self, snapshot_lease, _load_config):
        snapshot_lease.require_for(self.connector)
        self.connector.events.append("source_authority")
        return SimpleNamespace(digest=b"s" * 32)

    @staticmethod
    def _prepare_copy_select_sql(select_sql, _schema, _load_config, **_kwargs):
        return select_sql


def test_guard_compiles_exact_filtered_query_for_every_mssql_temporal_domain() -> None:
    connector = _Connector()

    PostgresMssqlSourceValueGuard(connector).validate(
        'SELECT * FROM public."events" WHERE tenant_id = 7',
        (
            ("event_date", "date"),
            ("naive_at", "timestamp(3) without time zone"),
            ("offset_at", "timestamptz(3)"),
            ("clock_at", "time(6) without time zone"),
            ('odd"name', "timestamp with time zone"),
            ("payload", "text"),
        ),
    )

    assert len(connector.queries) == 1
    query = connector.queries[0]
    assert 'FROM (SELECT * FROM public."events" WHERE tenant_id = 7) AS dpone_source_guard' in query
    assert 'NOT isfinite(dpone_source_guard."event_date")' in query
    assert "DATE '0001-01-01'" in query
    assert "TIMESTAMP '10000-01-01 00:00:00'" in query
    assert "(dpone_source_guard.\"offset_at\" AT TIME ZONE 'UTC')" in query
    assert "TIME '24:00:00'" in query
    assert 'dpone_source_guard."odd""name"' in query
    assert 'dpone_source_guard."payload"' not in query


def test_guard_raises_stable_typed_error_for_the_exact_offending_column() -> None:
    connector = _Connector([{"offending_column": "occurred_at"}])

    with pytest.raises(
        PostgresMssqlSourceValueError,
        match="postgres_mssql.source_value_unrepresentable:occurred_at:timestamptz",
    ) as raised:
        PostgresMssqlSourceValueGuard(connector).validate(
            "SELECT occurred_at FROM public.events",
            (("occurred_at", "timestamptz"),),
        )

    assert raised.value.code == "DPONE_POSTGRES_MSSQL_SOURCE_VALUE_UNREPRESENTABLE"
    assert raised.value.column == "occurred_at"
    assert raised.value.source_type == "timestamptz"


def test_whole_export_rejects_missing_source_domain_contract_before_temp_and_copy(tmp_path: Path) -> None:
    connector = _Connector()
    strategy = _Strategy(connector)
    load_config = SimpleNamespace(options={"work_dir": str(tmp_path)})

    with pytest.raises(TypeError, match="source_value_guard_relation_schema_required"):
        PostgresWholeFileExportService(strategy).export_full(
            "SELECT occurred_at FROM public.events",
            [("occurred_at", "datetimeoffset(6)")],
            load_config,
            relation_schema=None,
        )

    assert "copy" not in connector.events
    assert connector.events[:2] == ["begin_repeatable_read", "source_authority"]
    assert connector.events[-1] == "rollback"
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_whole_export_fuses_temporal_domain_guard_into_the_single_copy_scan(tmp_path: Path) -> None:
    class _InvalidCopyValue(Exception):
        sqlstate = "22P02"

    class _CopyConnector(_Connector):
        def copy_to_file(self, **kwargs):
            self.events.append("copy")
            query = str(kwargs["query_sql"])
            self.queries.append(query)
            Path(kwargs["output_path"]).write_bytes(b"partial source payload\n")
            marker = re.search(r"dpone_pg_mssql_copy_guard_v1_[0-9a-f]{32}_0", query)
            assert marker is not None
            raise _InvalidCopyValue(f'invalid input syntax for type integer: "{marker.group()}"')

    connector = _CopyConnector()
    strategy = _Strategy(connector)
    load_config = SimpleNamespace(options={"work_dir": str(tmp_path)})

    with pytest.raises(
        PostgresMssqlSourceValueError,
        match="postgres_mssql.source_value_unrepresentable:occurred_at:timestamptz",
    ) as raised:
        PostgresWholeFileExportService(strategy).export_full(
            "SELECT occurred_at, payload FROM public.events",
            [("occurred_at", "datetimeoffset(6)"), ("payload", "nvarchar(max)")],
            load_config,
            relation_schema=(("occurred_at", "timestamptz"), ("payload", "text")),
        )

    assert raised.value.code == "DPONE_POSTGRES_MSSQL_SOURCE_VALUE_UNREPRESENTABLE"
    assert connector.events == ["begin_repeatable_read", "source_authority", "copy", "rollback"]
    assert len(connector.queries) == 1
    assert re.search(r"dpone_pg_mssql_copy_guard_v1_[0-9a-f]{32}_0", connector.queries[0])
    assert "FROM (SELECT occurred_at, payload FROM public.events) AS dpone_source_guard" in connector.queries[0]
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_fused_guard_does_not_translate_unrelated_or_legacy_marker_errors() -> None:
    class _InvalidCopyValue(Exception):
        sqlstate = "22P02"

    guarded = PostgresMssqlSourceValueGuard(_Connector()).guard_copy_query(
        "SELECT occurred_at FROM public.events",
        (("occurred_at", "timestamptz"),),
    )

    assert guarded.translated_error(_InvalidCopyValue("dpone_source_guard_0")) is None
    assert guarded.translated_error(_InvalidCopyValue("dpone_pg_mssql_copy_guard_v1_near_0")) is None
    assert guarded.translated_error(ValueError(f"{guarded.marker_namespace}0")) is None


def test_fused_guard_preserves_base_exception_identity_and_cleans_partial_file(tmp_path: Path) -> None:
    class _Cancellation(BaseException):
        sqlstate = "22P02"

    cancellation = _Cancellation("cancelled")

    class _CopyConnector(_Connector):
        def copy_to_file(self, **kwargs):
            self.events.append("copy")
            Path(kwargs["output_path"]).write_bytes(b"partial source payload\n")
            raise cancellation

    connector = _CopyConnector()
    strategy = _Strategy(connector)

    with pytest.raises(_Cancellation) as raised:
        PostgresWholeFileExportService(strategy).export_full(
            "SELECT occurred_at FROM public.events",
            [("occurred_at", "datetimeoffset(6)")],
            SimpleNamespace(options={"work_dir": str(tmp_path)}),
            relation_schema=(("occurred_at", "timestamptz"),),
        )

    assert raised.value is cancellation
    assert connector.events == ["begin_repeatable_read", "source_authority", "copy", "rollback"]
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("source_type", "predicate_fragment", "invalid_projection_fragment"),
    [
        ("date", "NOT isfinite", 'THEN dpone_source_guard."value" + CAST('),
        ("timestamp without time zone", "TIMESTAMP '10000-01-01", "* INTERVAL '0 seconds'"),
        ("timestamp with time zone", "AT TIME ZONE 'UTC'", "* INTERVAL '0 seconds'"),
        ("time without time zone", "TIME '24:00:00'", "* INTERVAL '0 seconds'"),
    ],
)
def test_fused_guard_preserves_valid_temporal_values_and_traps_each_invalid_domain(
    source_type: str,
    predicate_fragment: str,
    invalid_projection_fragment: str,
) -> None:
    guarded = PostgresMssqlSourceValueGuard(_Connector()).guard_copy_query(
        "SELECT value, payload FROM public.events",
        (("value", source_type), ("payload", "text")),
    )
    query = str(guarded.query_sql)

    assert predicate_fragment in query
    assert invalid_projection_fragment in query
    assert 'ELSE dpone_source_guard."value" END AS "value"' in query
    assert 'dpone_source_guard."payload" AS "payload"' in query


def test_exact_key_export_keeps_raw_postgres_types_for_the_source_guard(monkeypatch) -> None:
    connector = _Connector()
    lifecycle = ExtractionLifecycleAuthority()
    lease = issue_repeatable_read_snapshot_lease(
        connector=connector,
        lifecycle=lifecycle,
        raw_snapshot_token="20:30:",
    )
    captured: dict[str, object] = {}

    class _KeyStrategy:
        def __init__(self) -> None:
            self.connector = connector

        def fetch_schema_projection(self, _load_config):
            return SimpleNamespace(
                projected_schema=(("event_at", "datetimeoffset(6)"), ("payload", "nvarchar(max)")),
                relation_schema=(("event_at", "timestamptz"), ("payload", "text")),
            )

    service = PostgresWholeFileExportService(_KeyStrategy())

    def _capture(*args, **kwargs):
        captured["args"] = args
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(service, "_export", _capture)
    load_config = SimpleNamespace(
        unique_key=("event_at",),
        options={
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "event_at": {"type": "datetime", "nullable": False},
                    "payload": {"type": "string"},
                },
            }
        },
    )

    service.export_exact_keys("SELECT event_at FROM public.events", load_config, snapshot_lease=lease)

    assert captured["args"][1] == [("event_at", "datetimeoffset(6)")]
    assert captured["relation_schema"] == [("event_at", "timestamptz")]
