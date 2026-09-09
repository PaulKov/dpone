from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from dpone.config.state import resolve_mssql_state_location_defaults
from dpone.contracts.incremental_snapshot import snapshot_token_digest
from dpone.contracts.repair_authority import (
    ExpectedCheckpoint,
    RepairAllowance,
    RepairAuthority,
    TargetAuthorityTransfer,
    repair_authority_digest,
)
from dpone.ports.source_state_storage import CheckpointCommitOutcome, MssqlStateLocation, SourceStateKey
from dpone.runtime.bootstrap_state import RuntimeStateBootstrap
from dpone.runtime.bootstrap_state_models import RuntimeStateBindings
from dpone.runtime.state.models import RunState, RunStateStatus
from dpone.runtime.state.mssql import MSSQLXMinStateStorage
from dpone.runtime.state.mssql_contract import (
    COMMIT_RECEIPT_CONTRACT,
    LOAD_AUDIT_CONTRACT,
    RUN_STATE_CONTRACT,
    SOURCE_STATE_CONTRACT,
)
from dpone.runtime.state.mssql_operational import MSSQLLoadAuditStorage, MSSQLRunStateStorage
from dpone.runtime.state.mssql_repair_contract import (
    REPAIR_AUTHORITY_CONTRACT,
    REPAIR_CONSUMPTION_CONTRACT,
)
from dpone.runtime.state.mssql_run_state_migration import render_run_state_v2_create_sql
from dpone.runtime.state.xmin_storage import XMinState


def _key(**overrides: object) -> SourceStateKey:
    values: dict[str, object] = {
        "environment": "prod",
        "process": "platform.sample_metrics.metrics_value",
        "source_connection": "postgres_sample_metrics_source",
        "source_database": "sample-metrics",
        "source_schema": "public",
        "source_table": "metrics_value",
        "target_database": "dwh_example",
        "target_schema": "sample_metrics",
        "target_table": "metrics_value",
        "target_identity": b"t" * 32,
        "unique_key": ("guid",),
        "schema_hash": "sha256:schema",
        "scope_hash": "sha256:scope",
    }
    values.update(overrides)
    return SourceStateKey(**values)  # type: ignore[arg-type]


def _transfer_authority(*, key: SourceStateKey, old_key: bytes, xmin: int = 90, revision: int = 4) -> RepairAuthority:
    expected = ExpectedCheckpoint(absent=True, xmin=None, revision=None)
    allow = RepairAllowance(full_baseline=True, max_delete_rows=None, max_delete_ratio=None)
    transfer = TargetAuthorityTransfer(state_key=old_key, xmin=xmin, revision=revision)
    expires = datetime(2026, 1, 2, tzinfo=UTC)
    digest = repair_authority_digest(
        authority_id="repair-transfer-1",
        state_key=key.digest,
        expected_checkpoint=expected,
        scope_hash=key.scope_hash,
        reason="Approved state-contract transfer",
        expires_at_utc=expires,
        allow=allow,
        transfer_from=transfer,
    )
    return RepairAuthority(
        authority_id="repair-transfer-1",
        state_key=key.digest,
        expected_checkpoint=expected,
        scope_hash=key.scope_hash,
        reason="Approved state-contract transfer",
        expires_at_utc=expires,
        allow=allow,
        authority_digest=digest,
        transfer_from=transfer,
    )


class _Executor:
    def __init__(
        self,
        *,
        fail: bool = False,
        shape_overrides: dict[str, tuple[str, object]] | None = None,
    ) -> None:
        self.fail = fail
        self.shape_overrides = shape_overrides or {}
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def get_records(self, query, params=None, as_dict=False):
        del as_dict
        values = tuple(params or ())
        rendered = str(query)
        self.calls.append((rendered, values))
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
        contracts = {
            "dpone_source_state": SOURCE_STATE_CONTRACT,
            "dpone_commit_receipt": COMMIT_RECEIPT_CONTRACT,
            "dpone_run_state": RUN_STATE_CONTRACT,
            "dpone_load_audit": LOAD_AUDIT_CONTRACT,
            "dpone_repair_authority": REPAIR_AUTHORITY_CONTRACT,
            "dpone_repair_authority_consumption": REPAIR_CONSUMPTION_CONTRACT,
        }
        if "sys.indexes" in rendered:
            if "i.is_primary_key" in rendered and values[-1:] == ("dpone_source_state",):
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
            table = str(values[-1])
            return [
                {"index_id": index, "column_name": column, "key_ordinal": ordinal}
                for index, columns in enumerate(contracts[table].unique_indexes, start=1)
                for ordinal, column in enumerate(columns, start=1)
            ]
        if "sys.columns" in rendered:
            table = str(values[-1])
            contract = contracts[table]
            shapes = {shape.name: shape for shape in contract.shapes}
            output = [
                {
                    "column_name": column,
                    "type_name": shapes[column].type_name,
                    "max_length": shapes[column].max_length,
                    "precision": shapes[column].precision,
                    "scale": shapes[column].scale,
                    "is_nullable": shapes[column].nullable,
                    "is_identity": shapes[column].identity or False,
                    "is_computed": shapes[column].is_computed,
                    "is_sparse": shapes[column].is_sparse,
                    "is_rowguidcol": shapes[column].is_rowguidcol,
                    "generated_always_type": shapes[column].generated_always_type,
                    "is_hidden": shapes[column].is_hidden,
                    "is_masked": shapes[column].is_masked,
                    "is_encrypted": shapes[column].is_encrypted,
                    "is_ansi_padded": shapes[column].is_ansi_padded,
                    "is_filestream": shapes[column].is_filestream,
                    "is_column_set": shapes[column].is_column_set,
                    "uses_database_default_collation": shapes[column].uses_database_default_collation,
                    "is_user_defined": shapes[column].is_user_defined,
                    "is_assembly_type": shapes[column].is_assembly_type,
                    "has_bound_rule": shapes[column].has_bound_rule,
                    "has_bound_default": shapes[column].has_bound_default,
                }
                for column in contract.columns
            ]
            for row in output:
                if row["column_name"] in self.shape_overrides:
                    field, value = self.shape_overrides[str(row["column_name"])]
                    row[field] = value
            return output
        if "xmin_value, state_revision, __dpone__updated_at" in rendered:
            return [
                {
                    "xmin_value": 100,
                    "state_revision": 7,
                    "__dpone__updated_at": datetime.now(UTC),
                    "is_initial": False,
                    "wraparound_detected": False,
                    "frozen_xid": None,
                }
            ]
        if self.fail:
            raise RuntimeError("checkpoint rejected")
        return [{"receipt_id": "load-1", "candidate_xmin": 101, "candidate_revision": 1}]

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        parts = [part for part in (database, schema, table) if part]
        return ".".join(f"[{part}]" for part in parts)

    def execute_query(self, query, params=None):
        self.calls.append((str(query), tuple(params or ())))
        return 1


class _RevisionExecutor(_Executor):
    def __init__(self, *, xmin: int, revision: int) -> None:
        super().__init__()
        self.xmin = xmin
        self.revision = revision

    def get_records(self, query, params=None, as_dict=False):
        rendered = str(query)
        values = tuple(params or ())
        if "DECLARE @cas_rows" not in rendered:
            return super().get_records(query, params, as_dict)
        self.calls.append((rendered, values))
        if values[9:11] != (self.xmin, self.revision):
            raise RuntimeError("DPONE_XMIN_CHECKPOINT_CAS_MISMATCH")
        self.xmin = int(values[0])
        self.revision = int(values[1])
        return [
            {
                "receipt_id": values[12],
                "candidate_xmin": values[16],
                "candidate_revision": values[18],
            }
        ]


class _MissingRepairTriggerExecutor(_Executor):
    def get_records(self, query, params=None, as_dict=False):
        if "sys.triggers" in str(query):
            self.calls.append((str(query), tuple(params or ())))
            return []
        return super().get_records(query, params=params, as_dict=as_dict)


class _MutableRepairTriggerExecutor(_Executor):
    trigger_definition = (
        "CREATE TRIGGER [dbo].[trg_dpone_repair_authority_immutable] "
        "ON [dbo].[dpone_repair_authority] INSTEAD OF UPDATE, DELETE AS "
        "-- DPONE_REPAIR_AUTHORITY_IMMUTABLE\nUPDATE a SET reason = N'mutable' FROM dbo.a;"
    )

    def get_records(self, query, params=None, as_dict=False):
        if "sys.triggers" in str(query):
            self.calls.append((str(query), tuple(params or ())))
            return [
                {
                    "trigger_name": "trg_dpone_repair_authority_immutable",
                    "is_disabled": False,
                    "is_instead_of_trigger": True,
                    "trigger_definition": self.trigger_definition,
                    "event_type": event,
                }
                for event in ("UPDATE", "DELETE")
            ]
        return super().get_records(query, params=params, as_dict=as_dict)


class _SuffixCommentMutableTriggerExecutor(_MutableRepairTriggerExecutor):
    trigger_definition = (
        "CREATE TRIGGER [dbo].[trg_dpone_repair_authority_immutable] "
        "ON [dbo].[dpone_repair_authority] INSTEAD OF UPDATE, DELETE AS "
        "UPDATE dbo.a SET reason = N'mutable'; "
        "-- AS THROW 51000, 'DPONE_REPAIR_AUTHORITY_IMMUTABLE', 1;"
    )


class _NotForReplicationRepairTriggerExecutor(_Executor):
    def get_records(self, query, params=None, as_dict=False):
        rows = super().get_records(query, params=params, as_dict=as_dict)
        if "sys.sql_modules" in str(query):
            for row in rows:
                row["is_not_for_replication"] = True
        return rows


class _MissingActiveAuthorityIndexExecutor(_Executor):
    def get_records(self, query, params=None, as_dict=False):
        if "i.is_primary_key" in str(query) and tuple(params or ())[-1:] == ("dpone_source_state",):
            self.calls.append((str(query), tuple(params or ())))
            return []
        return super().get_records(query, params=params, as_dict=as_dict)


class _HypotheticalActiveAuthorityIndexExecutor(_Executor):
    def get_records(self, query, params=None, as_dict=False):
        rows = super().get_records(query, params=params, as_dict=as_dict)
        if "i.is_primary_key" in str(query) and tuple(params or ())[-1:] == ("dpone_source_state",):
            for row in rows:
                row["is_hypothetical"] = True
        return rows


class _LegacyRunStateExecutor(_Executor):
    def get_records(self, query, params=None, as_dict=False):
        values = tuple(params or ())
        if "sys.columns" in str(query) and values == ("etl_state", "etl_run_state"):
            self.calls.append((str(query), values))
            return [
                {"column_name": column}
                for column in RUN_STATE_CONTRACT.columns
                if column not in {"process_name", "run_state_key"}
            ]
        return super().get_records(query, params=params, as_dict=as_dict)


def test_state_identity_is_deterministic_and_collision_safe() -> None:
    first = _key()
    same = _key()

    assert first.digest == same.digest
    assert len(first.digest) == 32
    assert first.digest != _key(environment="dev").digest
    assert first.digest != _key(process="platform.sample_metrics.sl_values").digest
    # Raw aliases are diagnostics only; SQL Server's target-local registry is
    # the sole physical target authority.
    assert first.digest == _key(target_database="dwh_example", target_table="METRICS_VALUE").digest
    assert first.digest != _key(target_identity=b"u" * 32).digest
    assert first.digest != _key(unique_key=("supplier_guid", "guid")).digest


def test_checkpoint_outcome_preserves_legacy_third_positional_committed_flag() -> None:
    outcome = CheckpointCommitOutcome("receipt", 101, False)

    assert outcome.committed is False
    assert outcome.candidate_revision == 0


def test_state_location_quotes_prod_three_part_names() -> None:
    location = MssqlStateLocation(
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
    )

    assert location.state_table_name == "[Example_System].[dbo].[dpone_source_state]"
    assert location.receipt_table_name == "[Example_System].[dbo].[dpone_commit_receipt]"


@pytest.mark.parametrize(
    "override",
    [
        {"state_key": ("max_length", 31)},
        {"xmin_value": ("type_name", "int")},
        {"source_snapshot_token": ("max_length", 72)},
        {"candidate_xmin": ("is_nullable", True)},
        {"state_revision": ("is_nullable", True)},
        {"candidate_revision": ("type_name", "int")},
    ],
)
def test_external_state_shape_rejects_truncating_or_nullable_drift(
    override: dict[str, tuple[str, object]],
) -> None:
    storage = MSSQLXMinStateStorage(
        connector=_Executor(shape_overrides=override),
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(RuntimeError, match="mssql_external_state_contract_shape"):
        storage.create_state_table()


def test_external_state_unique_index_preflight_rejects_disabled_or_filtered_indexes() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    storage.create_state_table()

    index_queries = [
        query for query, _params in executor.calls if "sys.indexes" in query and "i.filter_definition" not in query
    ]
    assert index_queries
    assert all("i.is_disabled = 0" in query for query in index_queries)
    assert all("i.has_filter = 0" in query for query in index_queries)


def test_external_state_requires_unique_filtered_active_target_authority_index() -> None:
    storage = MSSQLXMinStateStorage(
        connector=_MissingActiveAuthorityIndexExecutor(),
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(RuntimeError, match="mssql_external_active_target_authority_index"):
        storage.create_state_table()


def test_active_target_authority_index_is_matched_by_semantics_not_name() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    storage.create_state_table()

    semantic_queries = [query for query, _params in executor.calls if "i.is_primary_key" in query]
    assert semantic_queries
    assert all("i.name = ?" not in query for query in semantic_queries)


def test_external_state_rejects_hypothetical_active_target_authority_index() -> None:
    storage = MSSQLXMinStateStorage(
        connector=_HypotheticalActiveAuthorityIndexExecutor(),
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(RuntimeError, match="mssql_external_active_target_authority_index"):
        storage.create_state_table()


def test_external_repair_authority_requires_exact_immutable_trigger() -> None:
    storage = MSSQLXMinStateStorage(
        connector=_MissingRepairTriggerExecutor(),
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(RuntimeError, match="mssql_external_repair_authority_immutable_trigger"):
        storage.create_state_table()


@pytest.mark.parametrize(
    "executor_type",
    (
        _MutableRepairTriggerExecutor,
        _SuffixCommentMutableTriggerExecutor,
        _NotForReplicationRepairTriggerExecutor,
    ),
)
def test_repair_authority_trigger_drift_cannot_bypass_immutability_preflight(executor_type) -> None:
    storage = MSSQLXMinStateStorage(
        connector=executor_type(),
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(RuntimeError, match="mssql_external_repair_authority_immutable_trigger"):
        storage.create_state_table()


def test_run_state_process_dimension_prevents_same_dag_execution_collision() -> None:
    executor = _Executor()
    storage = MSSQLRunStateStorage(
        executor,
        schema="system",
        table="dpone_run_state",
        identity_policy="process_scoped_v2_required",
    )
    storage._table_created = True
    execution = datetime.now(UTC)

    def state(process_name: str) -> RunState:
        return RunState(
            dag_id="DAG__platform__sample_metrics__sync",
            process_name=process_name,
            source_schema="public",
            source_table=process_name.rsplit(".", 1)[-1],
            target_schema="sample_metrics",
            target_table=process_name.rsplit(".", 1)[-1],
            load_strategy="incremental_merge",
            execution_date=execution,
            state=RunStateStatus.RUNNING,
            started_at=execution,
        )

    storage.save_run_state(state("platform.sample_metrics.metrics_value"))
    storage.save_run_state(state("platform.sample_metrics.sl_values"))

    first_sql, first_params = executor.calls[-2]
    second_sql, second_params = executor.calls[-1]
    assert "target.run_state_key = source.run_state_key" in first_sql
    assert "mssql_run_state_identity_collision" in first_sql
    assert first_sql.count("?") == len(first_params)
    assert second_sql.count("?") == len(second_params)
    assert first_params[2] == "platform.sample_metrics.metrics_value"
    assert second_params[2] == "platform.sample_metrics.sl_values"
    assert first_params[0] != second_params[0]
    assert len(first_params[0]) == 32


def test_run_state_identity_uses_index_width_safe_hash_for_long_dimensions() -> None:
    executor = _Executor()
    storage = MSSQLRunStateStorage(
        executor,
        schema="system",
        table="dpone_run_state",
        identity_policy="process_scoped_v2_required",
    )
    storage._table_created = True
    execution = datetime.now(UTC)
    state = RunState(
        dag_id="d" * 512,
        process_name="p" * 512,
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy="incremental_merge",
        execution_date=execution,
        state=RunStateStatus.RUNNING,
        started_at=execution,
    )

    storage.save_run_state(state)

    sql, params = executor.calls[-1]
    assert "PRIMARY KEY" not in sql
    assert "mssql_run_state_identity_collision" in sql
    assert isinstance(params[0], bytes)
    assert len(params[0]) == 32
    assert RUN_STATE_CONTRACT.unique_indexes == (("id",), ("run_state_key",))


def test_run_state_identity_and_datetime2_value_share_one_utc_normalization() -> None:
    executor = _Executor()
    storage = MSSQLRunStateStorage(
        executor,
        schema="system",
        table="dpone_run_state",
        identity_policy="process_scoped_v2_required",
    )
    storage._table_created = True
    moscow = timezone(timedelta(hours=3))

    def state(execution_date: datetime) -> RunState:
        return RunState(
            dag_id="dag",
            process_name="pipeline",
            source_schema="public",
            source_table="orders",
            target_schema="landing",
            target_table="orders",
            load_strategy="incremental_merge",
            execution_date=execution_date,
            state=RunStateStatus.RUNNING,
            started_at=execution_date,
        )

    storage.save_run_state(state(datetime(2026, 1, 1, 3, tzinfo=moscow)))
    first_sql, first_params = executor.calls[-1]
    storage.save_run_state(state(datetime(2026, 1, 1, 0, tzinfo=UTC)))
    _second_sql, second_params = executor.calls[-1]

    assert first_params[0] == second_params[0]
    assert first_params[3] == second_params[3] == datetime(2026, 1, 1)
    assert "target.execution_date = source.execution_date" in first_sql


def test_legacy_runtime_run_state_preserves_v1_identity_and_sql() -> None:
    executor = _LegacyRunStateExecutor()
    legacy_manifest = {
        "type": "mssql",
        "connection_id": "mssql-dwh",
        "connection_type": "vault",
        "table": {"schema": "etl_state", "name": "etl_xmin_state"},
    }
    location = resolve_mssql_state_location_defaults(
        legacy_manifest,
        default_database="DWH_Dev",
        default_schema=None,
    )
    storage = RuntimeStateBootstrap.build_run_state_storage(
        state_bindings=RuntimeStateBindings(
            state_type="mssql",
            proxy_config={},
            xmin_state_storage=object(),
            shared_mssql_state_connector=executor,
            mssql_state_location=location,
        ),
        state_cfg=legacy_manifest,
        sink_obj=object(),
    )
    execution = datetime(2026, 1, 1, tzinfo=UTC)
    state = RunState(
        dag_id="legacy-dag",
        process_name="new-process-dimension",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy="incremental_merge",
        execution_date=execution,
        state=RunStateStatus.RUNNING,
        started_at=execution,
    )

    storage.save_run_state(state)

    create_sql = next(sql for sql, _params in executor.calls if "CREATE TABLE" in sql)
    merge_sql, merge_params = executor.calls[-1]
    assert "run_state_key" not in create_sql
    assert "process_name" not in create_sql
    assert "target.dag_id = source.dag_id" in merge_sql
    assert "run_state_key" not in merge_sql
    assert "process_name" not in merge_sql
    assert merge_sql.count("?") == len(merge_params)
    assert location.atomicity == "after_target"
    assert location.provisioning == "runtime"


def test_target_atomic_bootstrap_requires_explicit_legacy_run_state_v2_migration() -> None:
    executor = _LegacyRunStateExecutor()
    manifest = {
        "type": "mssql",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "table": {"schema": "etl_state", "name": "etl_xmin_state"},
    }
    location = resolve_mssql_state_location_defaults(
        manifest,
        default_database="DWH_Dev",
        default_schema=None,
    )

    with pytest.raises(RuntimeError, match="v1_to_v2_migration_required"):
        RuntimeStateBootstrap.build_run_state_storage(
            state_bindings=RuntimeStateBindings(
                state_type="mssql",
                proxy_config={},
                xmin_state_storage=object(),
                shared_mssql_state_connector=executor,
                mssql_state_location=location,
            ),
            state_cfg=manifest,
            sink_obj=object(),
        )

    assert not any("sys.indexes" in sql for sql, _params in executor.calls)


def test_target_atomic_bootstrap_accepts_exact_v2_run_state() -> None:
    executor = _Executor()
    manifest = {
        "type": "mssql",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "table": {"schema": "system", "name": "dpone_source_state"},
        "run_table": {"name": "dpone_run_state"},
    }
    location = resolve_mssql_state_location_defaults(
        manifest,
        default_database="DWH_Dev",
        default_schema=None,
    )

    storage = RuntimeStateBootstrap.build_run_state_storage(
        state_bindings=RuntimeStateBindings(
            state_type="mssql",
            proxy_config={},
            xmin_state_storage=object(),
            shared_mssql_state_connector=executor,
            mssql_state_location=location,
        ),
        state_cfg=manifest,
        sink_obj=object(),
    )

    assert isinstance(storage, MSSQLRunStateStorage)
    assert storage.identity_policy.value == "process_scoped_v2_required"
    assert any("sys.indexes" in sql for sql, _params in executor.calls)


def test_run_state_v2_migration_helper_creates_separate_collision_safe_table() -> None:
    ddl = render_run_state_v2_create_sql(
        "[DWH_Dev].[system].[dpone_run_state_v2]",
        constraint_token="dpone_run_state_v2",
    )

    assert ddl.startswith("CREATE TABLE [DWH_Dev].[system].[dpone_run_state_v2]")
    assert "run_state_key binary(32) NOT NULL" in ddl
    assert "process_name nvarchar(512) NOT NULL" in ddl
    assert "UNIQUE NONCLUSTERED (run_state_key)" in ddl
    assert "INSERT" not in ddl


def test_external_run_state_rejects_process_identity_shape_drift() -> None:
    storage = MSSQLRunStateStorage(
        _Executor(shape_overrides={"process_name": ("max_length", 512)}),
        database="DWH_Dev",
        schema="system",
        table="dpone_run_state",
        provisioning="external",
    )

    with pytest.raises(RuntimeError, match="mssql_external_state_contract_shape.*process_name"):
        storage.create_state_table()


def test_external_run_state_requires_identity_primary_key_shape() -> None:
    storage = MSSQLRunStateStorage(
        _Executor(shape_overrides={"id": ("is_identity", False)}),
        database="DWH_Dev",
        schema="system",
        table="dpone_run_state",
        provisioning="external",
    )

    with pytest.raises(RuntimeError, match="mssql_external_state_contract_shape.*id"):
        storage.create_state_table()


def test_external_load_audit_rejects_lineage_shape_drift() -> None:
    storage = MSSQLLoadAuditStorage(
        _Executor(shape_overrides={"run_id": ("is_nullable", True)}),
        database="DWH_Dev",
        schema="system",
        table="dpone_load_audit",
        provisioning="external",
    )

    with pytest.raises(RuntimeError, match="mssql_external_state_contract_shape.*run_id"):
        storage.create_load_table()


def test_checkpoint_and_receipt_share_caller_executor_without_commit() -> None:
    default_executor = _Executor(fail=True)
    transaction_executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=default_executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )
    candidate = XMinState(101, datetime.now(UTC))

    outcome = storage.compare_and_set_with_receipt(
        executor=transaction_executor,
        key=_key(),
        expected=XMinState(100, datetime.now(UTC)),
        candidate=candidate,
        load_id="load-1",
        snapshot_token=snapshot_token_digest("100:100:"),
    )

    assert outcome.receipt_id == "load-1"
    assert outcome.candidate_xmin == 101
    assert outcome.candidate_revision == 1
    assert default_executor.calls
    assert all("sys." in sql for sql, _params in default_executor.calls)
    assert len(transaction_executor.calls) == 1
    sql, _params = transaction_executor.calls[0]
    assert sql.count("?") == len(_params)
    assert "[Example_System].[dbo].[dpone_source_state]" in sql
    assert "[Example_System].[dbo].[dpone_commit_receipt]" in sql
    assert "DPONE_XMIN_CHECKPOINT_CAS_MISMATCH" in sql
    assert "? >= xmin_value" in sql
    assert "superseded_at_utc IS NULL" in sql
    assert "COMMIT TRANSACTION" not in sql.upper()
    assert "COMMIT;" not in sql.upper()
    assert "MERGE" not in sql.upper()


def test_target_authority_guard_uses_binary_physical_target_identity_and_state_key() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )
    key = _key()

    storage.assert_target_authority(executor=executor, key=key)

    sql, params = executor.calls[-1]
    assert "DPONE_XMIN_TARGET_AUTHORITY_CONFLICT" in sql
    assert "WITH (UPDLOCK, HOLDLOCK)" in sql
    assert "superseded_at_utc IS NULL" in sql
    assert "target_database = ?" not in sql
    assert "target_schema = ?" not in sql
    assert "target_table = ?" not in sql
    assert params == (key.target_identity, key.digest)


def test_target_authority_transfer_is_exact_and_marks_only_the_old_active_owner() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )
    storage._table_created = True
    key = _key(schema_hash="sha256:new-schema")
    old_key = _key(schema_hash="sha256:old-schema").digest
    authority = _transfer_authority(key=key, old_key=old_key)

    storage.assert_or_transfer_target_authority(executor=executor, key=key, authority=authority)

    sql, params = executor.calls[-1]
    assert params == (key.target_identity, key.digest, old_key, 90, 4)
    assert "DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_NOT_REQUIRED" in sql
    assert "DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_BINDING_MISMATCH" in sql
    assert "DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_DESTINATION_EXISTS" in sql
    assert "superseded_at_utc = SYSUTCDATETIME()" in sql
    assert "superseded_by_state_key = @new_state_key" in sql
    assert "xmin_value = @old_xmin" in sql
    assert "state_revision = @old_revision" in sql
    assert "COMMIT" not in sql.upper()


def test_loaded_mssql_checkpoint_carries_monotonic_revision() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )
    storage._table_created = True

    state = storage.load_state_by_key(_key())

    assert state is not None
    assert state.xmin_value == 100
    assert state.revision == 7
    state_query = next(sql for sql, _params in executor.calls if "xmin_value, state_revision" in sql)
    assert "superseded_at_utc IS NULL" in state_query


def test_initial_checkpoint_starts_revision_at_one_with_complete_receipt() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="DWH_Dev",
        schema="system",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )
    storage._table_created = True

    outcome = storage.compare_and_set_with_receipt(
        executor=executor,
        key=_key(environment="dev", target_database="DWH_Dev"),
        expected=None,
        candidate=XMinState(101, datetime.now(UTC), is_initial=True),
        load_id="load-1",
        snapshot_token=snapshot_token_digest("baseline"),
    )

    assert outcome.candidate_revision == 1
    sql, params = executor.calls[-1]
    assert "xmin_value, state_revision, is_initial" in sql
    assert "previous_revision, candidate_revision" in sql
    assert "WHERE NOT EXISTS" in sql
    assert sql.count("?") == len(params)


def test_checkpoint_failure_is_propagated_to_transaction_owner() -> None:
    executor = _Executor(fail=True)
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="DWH_Dev",
        schema="system",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(RuntimeError, match="checkpoint rejected"):
        storage.compare_and_set_with_receipt(
            executor=executor,
            key=_key(environment="dev", target_database="DWH_Dev"),
            expected=None,
            candidate=XMinState(101, datetime.now(UTC)),
            load_id="load-1",
            snapshot_token=snapshot_token_digest("100:100:"),
        )


def test_checkpoint_regression_is_rejected_before_transactional_sql() -> None:
    executor = _Executor()
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )

    with pytest.raises(ValueError, match="checkpoint_regression"):
        storage.compare_and_set_with_receipt(
            executor=executor,
            key=_key(),
            expected=XMinState(101, datetime.now(UTC)),
            candidate=XMinState(100, datetime.now(UTC)),
            load_id="load-regression",
            snapshot_token=snapshot_token_digest("101:101:"),
        )

    assert all("sys." in sql for sql, _params in executor.calls)


def test_equal_xmin_stale_revision_cannot_advance_checkpoint_twice() -> None:
    executor = _RevisionExecutor(xmin=100, revision=7)
    storage = MSSQLXMinStateStorage(
        connector=executor,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        atomicity="target_atomic",
    )
    storage._table_created = True
    now = datetime.now(UTC)
    expected = XMinState(100, now, revision=7)
    candidate = XMinState(100, now)

    outcome = storage.compare_and_set_with_receipt(
        executor=executor,
        key=_key(),
        expected=expected,
        candidate=candidate,
        load_id="load-first",
        snapshot_token=snapshot_token_digest("100:100:first"),
    )

    assert outcome.candidate_xmin == 100
    assert outcome.candidate_revision == 8
    assert executor.revision == 8
    first_sql, first_params = executor.calls[-1]
    assert "state_revision = ?" in first_sql
    assert "AND state_revision = ?" in first_sql
    assert first_sql.count("?") == len(first_params)

    with pytest.raises(RuntimeError, match="DPONE_XMIN_CHECKPOINT_CAS_MISMATCH"):
        storage.compare_and_set_with_receipt(
            executor=executor,
            key=_key(),
            expected=expected,
            candidate=candidate,
            load_id="load-stale",
            snapshot_token=snapshot_token_digest("100:100:stale"),
        )

    assert executor.revision == 8
