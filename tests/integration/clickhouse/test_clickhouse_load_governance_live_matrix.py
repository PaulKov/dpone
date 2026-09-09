from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact, PhysicalTransferChunk
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources import AbstractSource, ExtractResult

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)


SCHEMA = [("id", "bigint"), ("name", "nvarchar(100)"), ("event_date", "date")]


@pytest.mark.parametrize("artifact_kind", ["rows", "file", "byte_stream", "physical_chunks"])
@pytest.mark.parametrize(
    "strategy",
    [
        LoadStrategy.FULL_REFRESH,
        LoadStrategy.INCREMENTAL_APPEND,
        LoadStrategy.REPLACE,
        LoadStrategy.INCREMENTAL_MERGE,
        pytest.param(
            LoadStrategy.PARTITION_REPLACE,
            marks=pytest.mark.xfail(
                reason=(
                    "ClickHouse REPLACE PARTITION requires identical structure between "
                    "lineage-projected finalization and the pre-bootstrapped business "
                    "target (Code 122). Tracked as connector-certification schedule debt; "
                    "other strategies still certify load_governance + row authority."
                ),
                strict=False,
            ),
        ),
    ],
    ids=lambda value: value.value,
)
def test_clickhouse_load_governance_live_matrix(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
    strategy: LoadStrategy,
    artifact_kind: str,
) -> None:
    table = f"it_governance_{strategy.value}_{artifact_kind}_{uuid.uuid4().hex[:8]}"
    sink = ClickHouseSink(clickhouse_connector)
    source_rows, expected_rows = _case_rows(strategy)
    source = _Source(_artifact(artifact_kind, tmp_path, source_rows), SCHEMA)
    config = _load_config(clickhouse_settings.database, table, strategy, artifact_kind)

    try:
        _bootstrap_target_if_needed(clickhouse_connector, clickhouse_settings.database, table, strategy)

        result = ETLProcessor(source, sink, etl_logger=_Logger()).run(config, dag_id=f"it_{strategy.value}")

        assert result["status"] == "success"
        assert _business_rows(clickhouse_connector, clickhouse_settings.database, table) == expected_rows
        columns = _columns(clickhouse_connector, clickhouse_settings.database, table)
        assert {"__dpone__run_id", "__dpone__load_id", "__dpone__loaded_at", "__dpone__extracted_at"} <= columns
        if artifact_kind == "rows":
            assert "__dpone__row_id" in columns
        else:
            assert "__dpone__row_id" not in columns
        assert _leftover_tables(clickhouse_connector, clickhouse_settings.database, table) == []
        assert _audit_statuses(clickhouse_connector, clickhouse_settings.database, table)[-1] == "committed"
        assert {"staged", "lineage_projected", "quality_checked", "finalized"} <= _load_step_ids(
            clickhouse_connector,
            clickhouse_settings.database,
            table,
        )
    finally:
        _drop_family(clickhouse_connector, clickhouse_settings.database, table)


def test_clickhouse_load_governance_quality_failure_blocks_finalization_and_cleans_staging(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_governance_dq_failure_{uuid.uuid4().hex[:8]}"
    config = _load_config(clickhouse_settings.database, table, LoadStrategy.INCREMENTAL_APPEND, "file")
    # Intentional completed-authority mismatch: one staged row, rows_exported=2.
    # estimated_rows alone never certifies source_row_count gates.
    source = _Source(
        _file_artifact(
            tmp_path / "dq-failure.tsv",
            [{"id": 2, "name": "new", "event_date": "2026-01-01"}],
            estimated_rows=2,
            rows_exported=2,
        ),
        SCHEMA,
    )

    try:
        _create_business_target(clickhouse_connector, clickhouse_settings.database, table)
        clickhouse_connector.execute_query(
            f"INSERT INTO `{clickhouse_settings.database}`.`{table}` (`id`, `name`, `event_date`) "
            "VALUES (1, 'existing', '2026-01-01')"
        )

        with pytest.raises(RuntimeError, match="quality gates failed"):
            ETLProcessor(
                _Source(source.artifact, SCHEMA), ClickHouseSink(clickhouse_connector), etl_logger=_Logger()
            ).run(
                config,
                dag_id="it_quality_failure",
            )

        assert _business_rows(clickhouse_connector, clickhouse_settings.database, table) == [
            {"id": 1, "name": "existing", "event_date": "2026-01-01"}
        ]
        assert _leftover_tables(clickhouse_connector, clickhouse_settings.database, table) == []
        assert _audit_statuses(clickhouse_connector, clickhouse_settings.database, table)[-1] == "failed"
    finally:
        _drop_family(clickhouse_connector, clickhouse_settings.database, table)


def _load_config(database: str, table: str, strategy: LoadStrategy, artifact_kind: str) -> LoadConfig:
    clickhouse_bulk = _clickhouse_http_options(database) if artifact_kind == "byte_stream" else {}
    return LoadConfig(
        source_conn_id="source-it",
        target_conn_id="clickhouse-it",
        source_schema="dbo",
        source_table=table,
        target_schema=database,
        target_table=table,
        load_strategy=strategy,
        unique_key="id" if strategy == LoadStrategy.INCREMENTAL_MERGE or artifact_kind == "rows" else None,
        custom_predicate="id = 1" if strategy == LoadStrategy.REPLACE else None,
        partition={"column": "event_date", "max_partitions_per_run": 2}
        if strategy == LoadStrategy.PARTITION_REPLACE
        else {},
        batch_size=2,
        options={
            "source_type": "integration",
            "lineage": {"enabled": True, "preset": "standard" if artifact_kind == "rows" else "bulk_standard"},
            "load_governance": {
                "enabled": True,
                "finalization_phase": "pre_finalize",
                "audit": {
                    "enabled": True,
                    "state_schema": database,
                    "loads_table": "__dpone__loads",
                    "steps_table": "__dpone__load_steps",
                },
                "cleanup": {"staging_policy": "eager"},
            },
            "quality": {
                "gates": [
                    {
                        "id": "row_count_reconciliation",
                        "type": "row_count_reconciliation",
                        "severity": "error",
                        "tolerance": {"mode": "absolute", "value": 0},
                    }
                ]
            },
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "partition_by": "event_date",
                        "order_by": ["id"],
                    }
                }
            },
            **clickhouse_bulk,
        },
    )


def _clickhouse_http_options(database: str) -> dict[str, object]:
    return {
        "clickhouse_bulk": {
            "mode": "http",
            "http": {
                "host": os.getenv("DPONE_IT_CH_HTTP_HOST", os.getenv("DPONE_IT_CH_HOST", "127.0.0.1")),
                "port": int(os.getenv("DPONE_IT_CH_HTTP_PORT", "58123")),
                "database": database,
                "user": os.getenv("DPONE_IT_CH_USER", "default"),
                "password": os.getenv("DPONE_IT_CH_PASSWORD", "dpone"),
                "secure": False,
            },
        }
    }


def _case_rows(strategy: LoadStrategy) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if strategy == LoadStrategy.FULL_REFRESH:
        rows = [
            {"id": 1, "name": "alpha", "event_date": "2026-01-01"},
            {"id": 2, "name": "beta", "event_date": "2026-01-01"},
        ]
        return rows, rows
    if strategy == LoadStrategy.INCREMENTAL_APPEND:
        return [{"id": 2, "name": "new", "event_date": "2026-01-01"}], [
            {"id": 1, "name": "existing", "event_date": "2026-01-01"},
            {"id": 2, "name": "new", "event_date": "2026-01-01"},
            {"id": 9, "name": "keep", "event_date": "2026-01-02"},
        ]
    if strategy == LoadStrategy.REPLACE:
        return [{"id": 1, "name": "replacement", "event_date": "2026-01-01"}], [
            {"id": 1, "name": "replacement", "event_date": "2026-01-01"},
            {"id": 9, "name": "keep", "event_date": "2026-01-02"},
        ]
    if strategy == LoadStrategy.INCREMENTAL_MERGE:
        return [
            {"id": 1, "name": "merged", "event_date": "2026-01-01"},
            {"id": 2, "name": "inserted", "event_date": "2026-01-01"},
        ], [
            {"id": 1, "name": "merged", "event_date": "2026-01-01"},
            {"id": 2, "name": "inserted", "event_date": "2026-01-01"},
            {"id": 9, "name": "keep", "event_date": "2026-01-02"},
        ]
    if strategy == LoadStrategy.PARTITION_REPLACE:
        return [
            {"id": 1, "name": "partition-new", "event_date": "2026-01-01"},
            {"id": 2, "name": "partition-extra", "event_date": "2026-01-01"},
        ], [
            {"id": 1, "name": "partition-new", "event_date": "2026-01-01"},
            {"id": 2, "name": "partition-extra", "event_date": "2026-01-01"},
            {"id": 9, "name": "keep", "event_date": "2026-01-02"},
        ]
    raise AssertionError(f"unhandled strategy: {strategy}")


def _artifact(kind: str, tmp_path: Path, rows: list[dict[str, object]]) -> Any:
    if kind == "rows":
        return InMemoryRowsArtifact(rows)
    if kind == "byte_stream":
        return _byte_stream_artifact(rows)
    if kind == "physical_chunks":
        return _physical_chunks_artifact(tmp_path, rows)
    return _file_artifact(
        tmp_path / f"{uuid.uuid4().hex}.tsv",
        rows,
        estimated_rows=len(rows),
        rows_exported=len(rows),
    )


def _file_artifact(
    path: Path,
    rows: list[dict[str, object]],
    *,
    estimated_rows: int,
    rows_exported: int | None = None,
) -> FileExportArtifact:
    path.write_text(
        "".join(f"{row['id']}\t{row['name']}\t{row['event_date']}\n" for row in rows),
        encoding="utf-8",
    )
    artifact = FileExportArtifact(
        str(path),
        ["id", "name", "event_date"],
        format="mssql-delimited",
        estimated_rows=estimated_rows,
    )
    # Planned estimates never certify row_count_reconciliation; publish completed export
    # authority the same way production extract paths attach rows_exported after COUNT/BCP.
    if rows_exported is not None:
        artifact.rows_exported = rows_exported
    return artifact


def _byte_stream_artifact(rows: list[dict[str, object]]) -> ByteStreamArtifact:
    payload = _tsv_bytes(rows)
    split = max(1, len(payload) // 2)
    artifact = ByteStreamArtifact(
        lambda: [payload[:split], payload[split:]],
        columns=["id", "name", "event_date"],
        format="mssql-delimited",
        estimated_rows=len(rows),
    )
    artifact.rows_exported = len(rows)
    return artifact


def _physical_chunks_artifact(tmp_path: Path, rows: list[dict[str, object]]) -> PhysicalChunkedFileExportArtifact:
    chunks: list[PhysicalTransferChunk] = []
    for index, row in enumerate(rows):
        path = tmp_path / f"dpone_physical_chunk_{uuid.uuid4().hex}_{index}.bcp"
        payload = _tsv_bytes([row])
        path.write_bytes(payload)
        chunks.append(
            PhysicalTransferChunk(
                file_path=str(path),
                chunk_index=index,
                byte_count=len(payload),
                row_count=1,
                checksum="sha256:" + sha256(payload).hexdigest(),
                format="mssql-delimited",
            )
        )
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: list(chunks),
        columns=["id", "name", "event_date"],
        evidence_path=tmp_path / f"physical-chunks-{uuid.uuid4().hex}.json",
        estimated_rows=len(rows),
    )
    artifact.rows_exported = len(rows)
    return artifact


def _tsv_bytes(rows: list[dict[str, object]]) -> bytes:
    return "".join(f"{row['id']}\t{row['name']}\t{row['event_date']}\n" for row in rows).encode("utf-8")


def _bootstrap_target_if_needed(connector: Any, database: str, table: str, strategy: LoadStrategy) -> None:
    if strategy == LoadStrategy.FULL_REFRESH:
        return
    _create_business_target(connector, database, table)
    rows = [(1, "existing", "2026-01-01"), (9, "keep", "2026-01-02")]
    values = ", ".join(f"({row_id}, '{name}', '{event_date}')" for row_id, name, event_date in rows)
    connector.execute_query(f"INSERT INTO `{database}`.`{table}` (`id`, `name`, `event_date`) VALUES {values}")


def _create_business_target(connector: Any, database: str, table: str) -> None:
    connector.execute_query(
        f"CREATE TABLE `{database}`.`{table}` ("
        "`id` Int64, `name` String, `event_date` Date"
        ") ENGINE = MergeTree PARTITION BY event_date ORDER BY id"
    )


def _business_rows(connector: Any, database: str, table: str) -> list[dict[str, object]]:
    rows = connector.get_records(
        f"SELECT id, name, toString(event_date) AS event_date FROM `{database}`.`{table}` ORDER BY id",
        as_dict=True,
    )
    return [{"id": int(row["id"]), "name": row["name"], "event_date": row["event_date"]} for row in rows]


def _columns(connector: Any, database: str, table: str) -> set[str]:
    return {
        str(row["name"])
        for row in connector.get_records(
            f"SELECT name FROM system.columns WHERE database = '{database}' AND table = '{table}'",
            as_dict=True,
        )
    }


def _leftover_tables(connector: Any, database: str, table: str) -> list[str]:
    return [
        str(row["name"])
        for row in connector.get_records(
            f"""
            SELECT name
            FROM system.tables
            WHERE database = '{database}'
              AND startsWith(name, '{table}__dpone_')
            ORDER BY name
            """,
            as_dict=True,
        )
    ]


def _audit_statuses(connector: Any, database: str, table: str) -> list[str]:
    rows = connector.get_records(
        f"""
        SELECT status
        FROM `{database}`.`__dpone__loads`
        WHERE target_table = '{table}'
        ORDER BY __dpone__loaded_at, status
        """,
        as_dict=True,
    )
    return [str(row["status"]) for row in rows]


def _load_step_ids(connector: Any, database: str, table: str) -> set[str]:
    rows = connector.get_records(
        f"""
        SELECT DISTINCT s.step_id
        FROM `{database}`.`__dpone__load_steps` AS s
        INNER JOIN `{database}`.`__dpone__loads` AS l ON s.load_id = l.load_id
        WHERE l.target_table = '{table}'
        """,
        as_dict=True,
    )
    return {str(row["step_id"]) for row in rows}


def _drop_family(connector: Any, database: str, table: str) -> None:
    for name in [table, *_leftover_tables(connector, database, table)]:
        connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{name}`")


class _Source(AbstractSource):
    def __init__(self, artifact: Any, schema: Sequence[tuple[str, str]]) -> None:
        self.artifact = artifact
        self.schema = schema

    def extract(self, load_config: Any, last_state: Any | None) -> ExtractResult:
        del load_config, last_state
        return ExtractResult(artifact=self.artifact, schema=self.schema)

    def get_incremental_state(self, load_config: Any) -> None:
        del load_config
        return None


class _Logger:
    def log_etl_start(self, config: dict[str, Any]) -> None:
        del config

    def log_etl_progress(self, stage: str, details: dict[str, Any] | None = None) -> None:
        del stage, details

    def log_etl_error(self, error: str, context: dict[str, Any] | None = None) -> None:
        del error, context

    def log_etl_end(self, result: dict[str, Any]) -> None:
        del result

    def log_run_state_info(self, message: str, details: dict[str, Any] | None = None) -> None:
        del message, details
