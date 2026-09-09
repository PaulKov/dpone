from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.runtime.etl.result_metrics import populate_success_result
from dpone.runtime.etl.source_state import SourceStateService
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.state.load_audit_mapping import (
    load_audit_exact_metric_params,
    load_audit_insert_params,
)
from dpone.runtime.state.mssql import (
    MSSQLLoadAuditStorage,
    MSSQLRunStateStorage,
    MSSQLXMinStateStorage,
)
from dpone.runtime.state.mssql_contract import (
    COMMIT_RECEIPT_CONTRACT,
    LOAD_AUDIT_CONTRACT,
    RUN_STATE_CONTRACT,
    SOURCE_STATE_CONTRACT,
    MssqlExternalTableContract,
)
from dpone.runtime.state.mssql_repair_contract import (
    REPAIR_AUTHORITY_CONTRACT,
    REPAIR_CONSUMPTION_CONTRACT,
)

_EXTERNAL_STATE_CONTRACTS: dict[str, MssqlExternalTableContract] = {
    "dpone_source_state": SOURCE_STATE_CONTRACT,
    "dpone_commit_receipt": COMMIT_RECEIPT_CONTRACT,
    "dpone_run_state": RUN_STATE_CONTRACT,
    "dpone_load_audit": LOAD_AUDIT_CONTRACT,
    "dpone_repair_authority": REPAIR_AUTHORITY_CONTRACT,
    "dpone_repair_authority_consumption": REPAIR_CONSUMPTION_CONTRACT,
}


class _RecordingConnector:
    def __init__(self, *, columns: set[str] | None = None) -> None:
        self.queries: list[str] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.columns = columns if columns is not None else _external_state_columns()

    def execute_query(self, query: object, *_args: object, **_kwargs: object) -> int:
        rendered = str(query)
        self.queries.append(rendered)
        self.execute_calls.append((rendered, _args))
        return 0

    def get_records(self, query: object, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
        rendered = str(query)
        self.queries.append(rendered)
        values = _args[0] if _args and isinstance(_args[0], tuple) else ()
        table = str(values[-1]) if values else ""
        if "sys.indexes" in rendered:
            if "i.is_primary_key" in rendered and table == "dpone_source_state":
                return [
                    {
                        "index_id": 2,
                        "index_name": "renamed_active_target_authority",
                        "is_unique": True,
                        "is_primary_key": False,
                        "is_unique_constraint": False,
                        "is_disabled": False,
                        "is_hypothetical": False,
                        "has_filter": True,
                        "filter_definition": "([superseded_at_utc] IS NULL)",
                        "type_desc": "NONCLUSTERED",
                        "column_name": column,
                        "key_ordinal": ordinal,
                        "is_included_column": False,
                        "is_descending_key": False,
                    }
                    for ordinal, column in enumerate(
                        ("target_identity",),
                        start=1,
                    )
                ]
            return self._index_rows(table)
        if "sys.columns" in rendered:
            return self._column_rows(table)
        if "sys.triggers" in rendered:
            return [
                {
                    "trigger_name": "trg_dpone_repair_authority_immutable",
                    "is_disabled": False,
                    "is_instead_of_trigger": True,
                    "trigger_definition": "THROW 51000, 'DPONE_REPAIR_AUTHORITY_IMMUTABLE', 1",
                    "event_type": event,
                }
                for event in ("UPDATE", "DELETE")
            ]
        return []

    def _column_rows(self, table: str) -> list[dict[str, object]]:
        contract = _EXTERNAL_STATE_CONTRACTS[table]
        shapes = {shape.name: shape for shape in contract.shapes}
        rows: list[dict[str, object]] = []
        for column in sorted(contract.columns & self.columns):
            row: dict[str, object] = {"column_name": column}
            if shape := shapes.get(column):
                row.update(
                    type_name=shape.type_name,
                    max_length=shape.max_length,
                    precision=shape.precision,
                    scale=shape.scale,
                    is_nullable=shape.nullable,
                    is_identity=shape.identity or False,
                    is_computed=shape.is_computed,
                    is_sparse=shape.is_sparse,
                    is_rowguidcol=shape.is_rowguidcol,
                    generated_always_type=shape.generated_always_type,
                    is_hidden=shape.is_hidden,
                    is_masked=shape.is_masked,
                    is_encrypted=shape.is_encrypted,
                    is_ansi_padded=shape.is_ansi_padded,
                    is_filestream=shape.is_filestream,
                    is_column_set=shape.is_column_set,
                    uses_database_default_collation=shape.uses_database_default_collation,
                    is_user_defined=shape.is_user_defined,
                    is_assembly_type=shape.is_assembly_type,
                    has_bound_rule=shape.has_bound_rule,
                    has_bound_default=shape.has_bound_default,
                )
            rows.append(row)
        return rows

    @staticmethod
    def _index_rows(table: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for index_id, columns in enumerate(
            _EXTERNAL_STATE_CONTRACTS[table].unique_indexes,
            start=1,
        ):
            rows.extend(
                {
                    "index_id": index_id,
                    "column_name": column,
                    "key_ordinal": ordinal,
                }
                for ordinal, column in enumerate(columns, start=1)
            )
        return rows

    @staticmethod
    def quote_identifier(value: object) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        return ".".join(f"[{part}]" for part in (database, schema, table) if part)


def _external_state_columns() -> set[str]:
    return {
        "id",
        "run_state_key",
        "state_key",
        "contract_version",
        "environment",
        "process_name",
        "source_connection",
        "source_database",
        "source_schema",
        "source_table",
        "target_database",
        "target_schema",
        "target_table",
        "target_identity",
        "unique_key_json",
        "schema_hash",
        "scope_hash",
        "xmin_value",
        "state_revision",
        "is_initial",
        "wraparound_detected",
        "frozen_xid",
        "source_snapshot_token",
        "last_load_id",
        "superseded_at_utc",
        "superseded_by_state_key",
        "receipt_id",
        "publication_receipt_id",
        "load_id",
        "previous_xmin",
        "candidate_xmin",
        "previous_revision",
        "candidate_revision",
        "committed_at",
        "dag_id",
        "load_strategy",
        "execution_date",
        "state",
        "started_at",
        "ended_at",
        "duration_min",
        "error_message",
        "rows_read",
        "rows_written",
        "rows_updated",
        "rows_deleted",
        "run_id",
        "status",
        "strategy",
        "staged_at",
        "failed_at",
        "extracted_rows",
        "staged_rows",
        "inserted_rows",
        "updated_rows",
        "loaded_rows",
        "deleted_rows",
        "reactivated_rows",
        "unchanged_rows",
        "soft_deleted_rows",
        "hard_deleted_rows",
        "active_rows",
        "total_rows",
        "commit_receipt_id",
        "commit_outcome",
        "artifact_uri",
        "authority_id",
        "authority_digest",
        "transfer_from_state_key",
        "transfer_from_xmin",
        "transfer_from_revision",
        "expected_checkpoint_absent",
        "expected_xmin",
        "expected_revision",
        "reason",
        "expires_at_utc",
        "allow_full_baseline",
        "allow_max_delete_rows",
        "allow_max_delete_ratio",
        "created_at_utc",
        "used_full_baseline",
        "observed_delete_rows",
        "observed_delete_ratio",
        "consumed_at_utc",
        "__dpone__loaded_at",
        "__dpone__updated_at",
    }


@pytest.mark.parametrize(
    ("storage_type", "create_method"),
    [
        pytest.param(MSSQLXMinStateStorage, "create_state_table", id="checkpoint-and-receipt"),
        pytest.param(MSSQLRunStateStorage, "create_state_table", id="run-state"),
        pytest.param(MSSQLLoadAuditStorage, "create_load_table", id="load-audit"),
    ],
)
def test_external_mssql_state_provisioning_never_executes_create(
    storage_type: type[Any],
    create_method: str,
) -> None:
    connector = _RecordingConnector()
    kwargs: dict[str, Any] = {
        "connector": connector,
        "database": "DWH_Dev",
        "schema": "system",
        "provisioning": "external",
    }
    if storage_type is MSSQLXMinStateStorage:
        kwargs.update(
            table="dpone_source_state",
            receipt_table="dpone_commit_receipt",
            atomicity="target_atomic",
        )
    elif storage_type is MSSQLRunStateStorage:
        kwargs["table"] = "dpone_run_state"
    else:
        kwargs["table"] = "dpone_load_audit"
    storage = storage_type(**kwargs)

    getattr(storage, create_method)()

    assert connector.queries
    assert all("CREATE" not in query.upper() for query in connector.queries)


def test_external_load_audit_rejects_missing_exact_metric_column_without_ddl() -> None:
    connector = _RecordingConnector(columns=_external_state_columns() - {"commit_outcome"})
    storage = MSSQLLoadAuditStorage(
        connector,
        database="DWH_Dev",
        schema="system",
        table="dpone_load_audit",
        provisioning="external",
    )

    with pytest.raises(RuntimeError, match=r"dpone_load_audit:commit_outcome$"):
        storage.create_load_table()

    assert connector.queries
    assert all("CREATE" not in query.upper() for query in connector.queries)


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_value",
        target_schema="sample_metrics",
        target_table="metrics_value",
    )


def test_snapshot_envelope_requires_committed_receipt_before_processor_success() -> None:
    with pytest.raises(RuntimeError, match="committed checkpoint receipt"):
        SourceStateService().persist_after_load(
            source=object(),
            sink=object(),
            original_load_config=_load_config(),
            effective_load_config=_load_config(),
            extract_result=SimpleNamespace(snapshot_envelope=object(), state=object()),
            load_result=LoadResult(inserted_rows=1, updated_rows=0, total_rows=1),
            should_persist=True,
        )


@pytest.mark.parametrize(
    "outcome",
    [AtomicCommitOutcome.COMMITTED, AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE],
)
def test_snapshot_envelope_accepts_only_receipt_backed_commit_outcomes(outcome: AtomicCommitOutcome) -> None:
    SourceStateService().persist_after_load(
        source=object(),
        sink=object(),
        original_load_config=_load_config(),
        effective_load_config=_load_config(),
        extract_result=SimpleNamespace(snapshot_envelope=object(), state=object()),
        load_result=LoadResult(
            inserted_rows=1,
            updated_rows=0,
            total_rows=1,
            commit_receipt_id="receipt-1",
            commit_outcome=outcome,
        ),
        should_persist=True,
    )


def test_processor_preserves_exact_snapshot_metrics_and_commit_receipt() -> None:
    load_result = LoadResult(
        inserted_rows=2,
        updated_rows=3,
        total_rows=12,
        staging_rows=10,
        soft_deleted_rows=2,
        reactivated_rows=1,
        unchanged_rows=4,
        hard_deleted_rows=0,
        active_rows=10,
        commit_receipt_id="receipt-1",
        commit_outcome=AtomicCommitOutcome.COMMITTED,
    )
    result: dict[str, Any] = {}

    populate_success_result(
        result,
        load_result,
        validation_info={"key_diff": 0},
        reconciliation_metrics={"soft_deleted": 2, "reactivated": 1},
    )

    assert result == {
        "extracted_rows": 10,
        "loaded_rows": 2,
        "inserted_rows": 2,
        "updated_rows": 3,
        "total_rows": 12,
        "final_rows": 12,
        "staging_rows": 10,
        "soft_deleted_rows": 2,
        "reactivated_rows": 1,
        "unchanged_rows": 4,
        "hard_deleted_rows": 0,
        "active_rows": 10,
        "commit_receipt_id": "receipt-1",
        "commit_outcome": AtomicCommitOutcome.COMMITTED,
        "replaced_rows": 0,
        "deleted_lookback_rows": 0,
        "status": "success",
        "validation_info": {"key_diff": 0},
        "reconciliation_metrics": {"soft_deleted": 2, "reactivated": 1},
    }


def _committed_audit_record():
    started = LoadIdentityService().start(_load_config(), process_name="sample_metrics_metrics_value")
    return LoadIdentityService().mark_committed(
        started,
        LoadResult(
            inserted_rows=2,
            updated_rows=3,
            total_rows=12,
            staging_rows=10,
            soft_deleted_rows=2,
            reactivated_rows=1,
            unchanged_rows=4,
            hard_deleted_rows=0,
            active_rows=10,
            commit_receipt_id="receipt-1",
            commit_outcome=AtomicCommitOutcome.COMMITTED,
        ),
    )


def test_load_audit_maps_exact_metrics_without_breaking_legacy_mapping() -> None:
    record = _committed_audit_record()

    assert record.loaded_rows == 12
    assert record.deleted_rows == 2
    assert load_audit_exact_metric_params(record) == (
        2,
        1,
        4,
        2,
        0,
        10,
        12,
        "receipt-1",
        "committed",
    )
    assert len(load_audit_insert_params(record)) == 20


def test_mssql_load_audit_ddl_and_upsert_preserve_exact_metrics() -> None:
    connector = _RecordingConnector()
    storage = MSSQLLoadAuditStorage(
        connector,
        database="DWH_Dev",
        schema="system",
        table="dpone_load_audit",
    )

    storage.record_load_committed(_committed_audit_record())

    exact_columns = {
        "deleted_rows",
        "reactivated_rows",
        "unchanged_rows",
        "soft_deleted_rows",
        "hard_deleted_rows",
        "active_rows",
        "total_rows",
        "commit_receipt_id",
        "commit_outcome",
    }
    rendered = "\n".join(connector.queries)
    assert exact_columns <= LOAD_AUDIT_CONTRACT.columns
    assert all(column in rendered for column in exact_columns)
    merge_sql, merge_args = connector.execute_calls[-1]
    merge_params = merge_args[0]
    assert isinstance(merge_params, tuple)
    assert merge_sql.count("?") == len(merge_params)
