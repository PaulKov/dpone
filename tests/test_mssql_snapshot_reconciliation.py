from __future__ import annotations

import re
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.incremental_snapshot import (
    DELTA_HASH_COLUMN,
    DeltaSnapshotReceipt,
    IncrementalSnapshotEnvelope,
    KeySnapshotReceipt,
    KeySnapshotReconciliationPolicy,
)
from dpone.contracts.repair_authority import RepairAuthorityError, TargetAuthorityTransfer
from dpone.contracts.technical_columns import SoftDeleteMode, SoftDeletePolicy
from dpone.ports.source_state_storage import CheckpointCommitOutcome
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.etl.extracted_payload_load import ExtractedPayloadLoadService
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.lineage import StrategyMetadataEnricher
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head import (
    MssqlIncrementalPublicationHeadGuard,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_contract import (
    MssqlSnapshotContract,
    _validate_business_shape,
    _validate_managed_columns,
    _validate_required_technical_shape,
    _validate_soft_delete_shape,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_finalizer import (
    MssqlCommitOutcomeUnknown,
    MssqlSnapshotFinalizer,
    _business_columns,
    _changed_lineage_assignments,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_hash import row_hash_expression
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_options import (
    MssqlSnapshotOptionError,
    lock_timeout_ms,
    target_lock_resource,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import staging_scalar_expression
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import (
    SnapshotActionMetrics,
    SnapshotReconciliationError,
    SnapshotReconciliationSql,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_staging import (
    MssqlSnapshotStagingNormalizer,
    NormalizedSnapshotStaging,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target_shape import metadata_matches_type
from dpone.runtime.state.xmin_storage import XMinState
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.mssql_hex_binary import build_mssql_staging_select_expression
from dpone.runtime.support.mssql_snapshot_projection import (
    MSSQL_TEXT_KEY_COLLATION,
    validate_text_key_metadata,
)


def test_soft_delete_modes_render_one_source_of_truth() -> None:
    timestamp = SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_ONLY)
    combined = SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_AND_FLAG)
    flag = SoftDeletePolicy(SoftDeleteMode.FLAG_ONLY)

    assert timestamp.target_definitions() == ("[__dpone__deleted_at] datetime2(7) NULL",)
    assert combined.target_definitions() == (
        "[__dpone__deleted_at] datetime2(7) NULL",
        "[__dpone__is_deleted] AS CONVERT(bit, CASE WHEN [__dpone__deleted_at] IS NULL THEN 0 ELSE 1 END) PERSISTED",
    )
    assert flag.target_definitions() == ("[__dpone__is_deleted] bit NOT NULL DEFAULT (0)",)


@pytest.mark.parametrize(
    ("mode", "active_predicate", "reactivation_assignment"),
    [
        (SoftDeleteMode.TIMESTAMP_ONLY, "t.[__dpone__deleted_at] IS NULL", "[__dpone__deleted_at] = NULL"),
        (
            SoftDeleteMode.TIMESTAMP_AND_FLAG,
            "t.[__dpone__deleted_at] IS NULL",
            "[__dpone__deleted_at] = NULL",
        ),
        (SoftDeleteMode.FLAG_ONLY, "t.[__dpone__is_deleted] = 0", "[__dpone__is_deleted] = 0"),
    ],
)
def test_lifecycle_sql_is_set_based_idempotent_and_reactivates(
    mode: SoftDeleteMode,
    active_predicate: str,
    reactivation_assignment: str,
) -> None:
    renderer = SnapshotReconciliationSql(lambda value: f"[{value}]", SoftDeletePolicy(mode))

    delete_sql = renderer.soft_delete_missing(
        target="[DWH_Dev].[sample_metrics].[metrics_value]",
        keys="[DWH_Dev].[sample_metrics].[stg_keys]",
        unique_key=("guid",),
    )
    reactivate_sql = renderer.reactivate(
        target="[DWH_Dev].[sample_metrics].[metrics_value]",
        delta="[DWH_Dev].[sample_metrics].[stg_delta]",
        unique_key=("guid",),
        assignments=("[metric_value] = s.[metric_value]",),
        row_hash_expression="'abc'",
    )

    assert "MERGE" not in delete_sql.upper()
    assert "NOT EXISTS" in delete_sql
    assert active_predicate in delete_sql
    assert "OUTPUT" in delete_sql
    assert reactivation_assignment in reactivate_sql
    assert "OUTPUT" in reactivate_sql


def test_exact_metrics_are_disjoint_for_noop_delete_repeat_and_reactivation() -> None:
    noop = SnapshotActionMetrics.from_action_counts(
        inserted=0,
        updated=0,
        reactivated=0,
        soft_deleted=0,
        hard_deleted=0,
        active=3,
        total=3,
        staging=0,
    )
    first_delete = SnapshotActionMetrics.from_action_counts(
        inserted=0,
        updated=0,
        reactivated=0,
        soft_deleted=1,
        hard_deleted=0,
        active=2,
        total=3,
        staging=0,
    )
    repeated_delete = SnapshotActionMetrics.from_action_counts(
        inserted=0,
        updated=0,
        reactivated=0,
        soft_deleted=0,
        hard_deleted=0,
        active=2,
        total=3,
        staging=0,
    )
    reactivated = SnapshotActionMetrics.from_action_counts(
        inserted=0,
        updated=0,
        reactivated=1,
        soft_deleted=0,
        hard_deleted=0,
        active=3,
        total=3,
        staging=1,
    )

    assert noop.unchanged == 3
    assert first_delete.soft_deleted == 1
    assert repeated_delete.soft_deleted == 0
    assert reactivated.reactivated == 1
    assert reactivated.unchanged == 2


def test_metrics_reject_overlapping_or_impossible_action_counts() -> None:
    with pytest.raises(SnapshotReconciliationError, match="metric_invariant"):
        SnapshotActionMetrics.from_action_counts(
            inserted=2,
            updated=2,
            reactivated=1,
            soft_deleted=0,
            hard_deleted=0,
            active=3,
            total=3,
            staging=5,
        )


def test_detection_timestamp_is_utc_and_single_per_transaction() -> None:
    now = datetime.now(UTC)
    metrics = SnapshotActionMetrics.from_action_counts(
        inserted=0,
        updated=0,
        reactivated=0,
        soft_deleted=0,
        hard_deleted=0,
        active=0,
        total=0,
        staging=0,
        snapshot_effective_at=now,
    )

    assert metrics.snapshot_effective_at == now


class _DdlConnector:
    def __init__(self) -> None:
        self.statements: list[str] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def execute_query(self, sql, params=None):
        del params
        self.statements.append(str(sql))

    def get_records(self, sql, params=None, as_dict=False):
        del params, as_dict
        self.statements.append(str(sql))
        if "schema_exists" in str(sql):
            return [{"schema_exists": 1}]
        return []


class _DdlStrategy:
    def __init__(self, connector: _DdlConnector, *, exists: bool = False) -> None:
        self.connector = connector
        self.exists = exists

    def _table_exists(self, _load_config) -> bool:
        return self.exists

    @staticmethod
    def _target_name(_load_config) -> str:
        return "[DWH_Dev].[sample_metrics].[metrics_config]"


def _target_config(*, apply_runtime: bool, target_type: str = "nvarchar(450)") -> SimpleNamespace:
    return SimpleNamespace(
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_config",
        options={
            "schema_contract": {"columns": {"metric_code": {"nullable": False}}},
            "physical_design": {
                "apply_runtime": apply_runtime,
                "columns": {"metric_code": {"target_type": {"mssql": target_type}}},
            },
        },
    )


def test_target_bootstrap_honors_metric_code_override_and_bulk_lineage_shape() -> None:
    connector = _DdlConnector()
    contract = MssqlSnapshotContract(_DdlStrategy(connector))

    contract.ensure_target(
        _target_config(apply_runtime=True),
        [("metric_code", "text")],
        ("metric_code",),
        SoftDeletePolicy(),
    )

    ddl = "\n".join(connector.statements)
    assert f"[metric_code] nvarchar(450) COLLATE {MSSQL_TEXT_KEY_COLLATION} NOT NULL" in ddl
    assert "[__dpone__run_id] char(26) NOT NULL" in ddl
    assert "[__dpone__load_id] char(26) NOT NULL" in ddl
    assert "[__dpone__extracted_at] datetime2(7) NOT NULL" in ddl
    assert "[__dpone__row_id]" not in ddl
    assert "CREATE UNIQUE NONCLUSTERED INDEX" in ddl


def test_external_target_mode_never_creates_table_or_index() -> None:
    connector = _DdlConnector()
    contract = MssqlSnapshotContract(_DdlStrategy(connector))

    with pytest.raises(SnapshotReconciliationError, match="target_external_provisioning_required"):
        contract.ensure_target(
            _target_config(apply_runtime=False),
            [("metric_code", "text")],
            ("metric_code",),
            SoftDeletePolicy(),
        )

    assert not any("CREATE " in statement.upper() for statement in connector.statements)


def test_target_unique_key_preflight_rejects_disabled_or_filtered_indexes() -> None:
    connector = _DdlConnector()
    contract = MssqlSnapshotContract(_DdlStrategy(connector, exists=True))

    contract._unique_indexes(_target_config(apply_runtime=False))

    query = next(statement for statement in connector.statements if "sys.indexes" in statement)
    assert "i.is_disabled = 0" in query
    assert "i.has_filter = 0" in query


def test_soft_delete_shape_accepts_canonical_computed_flag_and_rejects_mutable_flag() -> None:
    timestamp = {
        "column_name": "__dpone__deleted_at",
        "type_name": "datetime2",
        "scale": 7,
        "is_nullable": 1,
        "is_computed": 0,
    }
    computed = {
        "column_name": "__dpone__is_deleted",
        "type_name": "bit",
        "is_computed": 1,
        "is_persisted": 1,
        "computed_definition": ("CONVERT(bit, CASE WHEN [__dpone__deleted_at] IS NULL THEN 0 ELSE 1 END)"),
    }
    _validate_soft_delete_shape(
        SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_AND_FLAG),
        {"__dpone__deleted_at": timestamp, "__dpone__is_deleted": computed},
    )
    parenthesized = {
        **computed,
        "computed_definition": ("((CONVERT([bit],(CASE WHEN ([__dpone__deleted_at] IS NULL) THEN (0) ELSE (1) END))))"),
    }
    _validate_soft_delete_shape(
        SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_AND_FLAG),
        {"__dpone__deleted_at": timestamp, "__dpone__is_deleted": parenthesized},
    )

    mutable = {**computed, "is_computed": 0, "is_persisted": 0}
    with pytest.raises(SnapshotReconciliationError, match="computed_shape_invalid"):
        _validate_soft_delete_shape(
            SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_AND_FLAG),
            {"__dpone__deleted_at": timestamp, "__dpone__is_deleted": mutable},
        )


def test_flag_only_shape_requires_bit_not_null_default_zero() -> None:
    flag = {
        "column_name": "__dpone__is_deleted",
        "type_name": "bit",
        "is_nullable": 0,
        "is_computed": 0,
        "default_definition": "((0))",
    }

    _validate_soft_delete_shape(
        SoftDeletePolicy(SoftDeleteMode.FLAG_ONLY),
        {"__dpone__is_deleted": flag},
    )


def test_external_target_requires_exact_bulk_standard_technical_shapes() -> None:
    metadata = {
        column: {
            "type_name": "char",
            "max_length": length,
            "is_nullable": 0,
            "is_computed": 0,
        }
        for column, length in {
            "__dpone__run_id": 26,
            "__dpone__load_id": 26,
            "__dpone__row_hash": 64,
        }.items()
    }
    metadata.update(
        {
            column: {
                "type_name": "datetime2",
                "scale": 7,
                "is_nullable": 0,
                "is_computed": 0,
            }
            for column in ("__dpone__extracted_at", "__dpone__loaded_at")
        }
    )

    _validate_required_technical_shape(metadata)

    metadata["__dpone__row_hash"] = {
        **metadata["__dpone__row_hash"],
        "is_nullable": 1,
    }
    with pytest.raises(SnapshotReconciliationError, match="target_technical_shape_invalid"):
        _validate_required_technical_shape(metadata)


def test_target_rejects_unknown_dpone_columns_including_published_xmin() -> None:
    policy = SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_ONLY)
    expected = {
        column: {}
        for column in (
            "__dpone__run_id",
            "__dpone__load_id",
            "__dpone__row_hash",
            "__dpone__extracted_at",
            "__dpone__loaded_at",
            "__dpone__deleted_at",
        )
    }

    _validate_managed_columns(policy, expected)
    with pytest.raises(SnapshotReconciliationError, match="unexpected_technical_columns"):
        _validate_managed_columns(policy, {**expected, "__dpone__xmin": {}})

    connector = _DdlConnector()
    MssqlSnapshotContract(_DdlStrategy(connector))._managed_column_metadata(_target_config(apply_runtime=False))
    query = connector.statements[-1]
    assert "c.name LIKE N'__dpone__%'" in query
    assert "c.name IN" not in query


def test_business_hash_excludes_every_dpone_column_and_changed_rows_refresh_lineage() -> None:
    schema = [
        ("guid", "uuid"),
        ("metric_value", "int"),
        ("__dpone__load_id", "string"),
        ("__dpone__row_id", "string"),
        ("__dpone__extracted_at", "timestamp"),
        ("__dpone__xmin", "bigint"),
    ]

    assert _business_columns(schema) == ["guid", "metric_value"]
    assert _changed_lineage_assignments(lambda value: f"[{value}]") == (
        "[__dpone__run_id] = ?",
        "[__dpone__load_id] = ?",
        "[__dpone__extracted_at] = ?",
    )


def test_bulk_standard_etl_path_preserves_envelope_and_finalizer_uses_load_identity() -> None:
    finalizer, connector, _state, _config, _old_envelope, delta, keys, policy = _finalizer_case()
    token = "sha256:" + "a" * 64
    scope = "sha256:" + "b" * 64
    receipt = KeySnapshotReceipt(
        snapshot_token=token,
        scope_hash=scope,
        key_columns=("guid",),
        row_count=1,
        checksum="sha256:" + "c" * 64,
        complete=True,
    )
    delta_schema = (("guid", "uuid"), ("value", "int"), (DELTA_HASH_COLUMN, "char(64)"))
    delta_receipt = DeltaSnapshotReceipt(
        snapshot_token=token,
        scope_hash=scope,
        columns=tuple(name for name, _dtype in delta_schema),
        row_count=1,
        checksum="sha256:" + "d" * 64,
        complete=True,
    )
    envelope = IncrementalSnapshotEnvelope(
        delta_artifact=SimpleNamespace(receipt=delta_receipt, estimated_rows=1, cleanup=lambda: None),
        key_artifact=SimpleNamespace(receipt=receipt, cleanup=lambda: None),
        delta_schema=delta_schema,
        key_schema=(("guid", "uuid"), ("__dpone__key_hash", "varchar(64)")),
        previous_checkpoint=None,
        candidate_checkpoint=XMinState(101, datetime.now(UTC)),
        snapshot_token=token,
        scope_hash=scope,
        state_key=SimpleNamespace(
            digest=b"1" * 32,
            target_identity=b"t" * 32,
            target_database="DWH_Dev",
            target_schema="sample_metrics",
            target_table="metrics_value",
        ),
        baseline=True,
    )
    load_config = LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_value",
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_value",
        staging_database="DWH_Dev",
        staging_schema="sample_metrics",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["guid"],
        options={
            "lineage": {"enabled": True, "preset": "bulk_standard"},
            "soft_delete": {"mode": "timestamp_only"},
            "reconciliation": {"enabled": True, "mode": "key_snapshot"},
            "physical_design": {"apply_runtime": False},
            "schema_contract": {"columns": {"guid": {"nullable": False}, "value": {"nullable": False}}},
        },
    )

    class _FinalizingPayloadLoader:
        def __init__(self) -> None:
            self.payload = None
            self.load_config = None

        def load_single_payload(self, effective_config, payload, *_args, **_kwargs):
            payload = StrategyMetadataEnricher().enrich_payload(
                payload,
                load_config=effective_config,
            )
            self.payload = payload
            self.load_config = effective_config
            return finalizer._finalize(
                effective_config,
                payload.schema,
                payload.artifact,
                delta,
                keys,
                policy,
            )

    loader = _FinalizingPayloadLoader()
    service = ExtractedPayloadLoadService(
        sink=SimpleNamespace(),
        logger=SimpleNamespace(log_etl_progress=lambda *_args: None),
        load_identity_service=SimpleNamespace(),
        payload_load_service=loader,
    )
    extraction_lifecycle = ExtractionLifecycleAuthority()
    extraction_started_at = datetime(2026, 1, 1, tzinfo=UTC)
    extraction_lifecycle.acquire(started_at=extraction_started_at)
    extraction_lifecycle.complete(
        completed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
    )
    extract_result = SimpleNamespace(
        artifact=envelope,
        schema=list(envelope.delta_schema),
        relation_schema=None,
        extraction_lifecycle=extraction_lifecycle,
    )
    load_record = SimpleNamespace(
        run_id="run-etl",
        load_id="load-etl",
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    reconciliation = SimpleNamespace(run_if_enabled=lambda *_args: None)

    result, _metrics = service.load_extracted_payload(
        load_config=load_config,
        extract_result=extract_result,
        load_record=load_record,
        reconciliation_service=reconciliation,
    )

    assert loader.payload.artifact is envelope
    assert loader.load_config.options["__dpone_load_identity"]["run_id"] == "run-etl"
    assert loader.load_config.options["__dpone_load_identity"]["load_id"] == "load-etl"
    assert loader.load_config.options["__dpone_load_identity"]["extracted_at"] == (extraction_started_at.isoformat())
    insert_params = next(params for sql, params in connector.calls if "INSERT INTO [target]" in sql)
    assert insert_params[:2] == ("run-etl", "load-etl")
    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED


class _FinalizerConnector:
    def __init__(self, *, lose_commit_ack: bool = False) -> None:
        self.lose_commit_ack = lose_commit_ack
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0
        self.target_rows = 0
        self.active_target_rows = 0
        self.delta_rows = 1
        self.key_snapshot_rows = 1
        self.legacy_shadow_owner: str | None = None
        self.transaction_open = False
        self._before_transaction: tuple[int, int] | None = None

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def begin(self) -> None:
        assert not self.transaction_open
        self.transaction_open = True
        self._before_transaction = (self.target_rows, self.active_target_rows)
        self.calls.append(("BEGIN", ()))

    def execute_query(self, sql, params=None) -> None:
        self.calls.append((str(sql), tuple(params or ())))

    def execute_many_typed(self, query, rows, *, parameter_types) -> int:
        del query, parameter_types
        return len(rows)

    def bcp_import_format(self, *args, **kwargs) -> int:
        del args, kwargs
        return 0

    def get_records(self, sql, params=None, as_dict=False):
        del as_dict
        rendered = str(sql)
        values = tuple(params or ())
        self.calls.append((rendered, values))
        if "dpone_target_identity" in rendered:
            return [{"binding_id": "11111111-1111-4111-8111-111111111111"}]
        if "sp_getapplock" in rendered:
            return [{"lock_result": 0}]
        if "sys.extended_properties" in rendered and "AS run_key" in rendered:
            if self.legacy_shadow_owner is None:
                return []
            return [{"run_key": self.legacy_shadow_owner}]
        if "snapshot_effective_at" in rendered:
            return [{"snapshot_effective_at": datetime(2026, 1, 1)}]
        if "action_count" in rendered:
            if "INSERT INTO [target]" in rendered:
                inserted = self.delta_rows
                self.target_rows += inserted
                self.active_target_rows += inserted
                return [{"action_count": inserted}]
            if "soft_deleted" in rendered:
                deleted = self.active_target_rows if self.key_snapshot_rows == 0 else 0
                self.active_target_rows -= deleted
                return [{"action_count": deleted}]
            return [{"action_count": 0}]
        if "COUNT_BIG" in rendered:
            if "NOT EXISTS" in rendered:
                if "FROM [target] AS t" in rendered and "FROM [keys] AS k" in rendered:
                    return [{"row_count": self.active_target_rows if self.key_snapshot_rows == 0 else 0}]
                return [{"row_count": 0}]
            if " WHERE " in rendered:
                return [{"row_count": self.active_target_rows}]
            return [{"row_count": self.target_rows}]
        return []

    def commit_transaction(self) -> None:
        self.commits += 1
        self.transaction_open = False
        self._before_transaction = None
        if self.lose_commit_ack:
            raise ConnectionError("ack lost")

    def rollback(self) -> None:
        self.rollbacks += 1
        if self._before_transaction is not None:
            self.target_rows, self.active_target_rows = self._before_transaction
        self.transaction_open = False
        self._before_transaction = None

    def close(self) -> None:
        self.closes += 1
        self.transaction_open = False
        self._before_transaction = None


class _FinalizerStrategy:
    def __init__(self, connector: _FinalizerConnector) -> None:
        self.connector = connector
        self.staging_manager = object()

    @staticmethod
    def _target_name(_load_config) -> str:
        return "[target]"

    @staticmethod
    def _staging_name(staging) -> str:
        return f"[{staging.table}]"

    @staticmethod
    def _staging_select_expression(_staging, column, alias) -> str:
        return f"{alias}.[{column}]"


class _AtomicState:
    atomicity = "target_atomic"

    def __init__(self, *, fail: bool = False, probe: bool = False) -> None:
        self.fail = fail
        self.probe = probe
        self.executor = None
        self.authorities = []
        self.repair_admissions = []
        self.repair_consumptions = []
        self.checkpoint_commits = []

    def assert_target_authority(self, *, executor, key):
        self.executor = executor
        self.authorities.append(key)

    def assert_or_transfer_target_authority(self, *, executor, key, authority):
        self.assert_target_authority(executor=executor, key=key)
        if authority is not None:
            assert hasattr(authority, "transfer_from")

    def compare_and_set_with_receipt(self, *, executor, **kwargs):
        self.executor = executor
        self.checkpoint_commits.append(kwargs)
        if self.fail:
            raise RuntimeError("state denied")
        return CheckpointCommitOutcome("load-1", 101)

    def assert_physical_target_identity(self, *, executor, key):
        del executor, key

    def probe_receipt(self, **_kwargs):
        return CheckpointCommitOutcome("load-1", 101) if self.probe else None

    def admit_repair_authority(self, **kwargs):
        self.repair_admissions.append(kwargs)
        if not kwargs.get("authority_ref"):
            raise RepairAuthorityError("repair_authority.required")
        return SimpleNamespace(
            authority_id=str(kwargs["authority_ref"]),
            authority_digest="sha256:" + "a" * 64,
            transfer_from=None,
        )

    def consume_repair_authority(self, **kwargs):
        self.repair_consumptions.append(kwargs)


class _StaleRevisionState(_AtomicState):
    def compare_and_set_with_receipt(self, *, executor, expected, candidate, **_kwargs):
        self.executor = executor
        assert expected.xmin_value == candidate.xmin_value == 100
        assert expected.revision == 7
        raise RuntimeError("DPONE_XMIN_CHECKPOINT_CAS_MISMATCH")


class _ConflictingTargetAuthorityState(_AtomicState):
    def assert_target_authority(self, *, executor, key):
        del executor, key
        raise RuntimeError("DPONE_XMIN_TARGET_AUTHORITY_CONFLICT")


class _TransferState(_AtomicState):
    def __init__(
        self,
        *,
        old_key: bytes,
        old_xmin: int = 90,
        old_revision: int = 4,
        transfer_key: bytes | None = None,
        include_transfer: bool = True,
        fail_consumption: bool = False,
        already_consumed: bool = False,
    ) -> None:
        super().__init__()
        self.old_key = old_key
        self.old_xmin = old_xmin
        self.old_revision = old_revision
        self.transfer_key = transfer_key or old_key
        self.include_transfer = include_transfer
        self.fail_consumption = fail_consumption
        self.already_consumed = already_consumed
        self.active_owner: bytes | None = old_key
        self.events: list[str] = []

    def admit_repair_authority(self, **kwargs):
        self.events.append("admit")
        self.repair_admissions.append(kwargs)
        if self.already_consumed:
            raise RepairAuthorityError("repair_authority.already_consumed")
        transfer = (
            TargetAuthorityTransfer(
                state_key=self.transfer_key,
                xmin=self.old_xmin,
                revision=self.old_revision,
            )
            if self.include_transfer
            else None
        )
        return SimpleNamespace(
            authority_id=str(kwargs["authority_ref"]),
            authority_digest="sha256:" + "a" * 64,
            transfer_from=transfer,
        )

    def assert_or_transfer_target_authority(self, *, executor, key, authority):
        self.executor = executor
        if authority is None or authority.transfer_from is None:
            self.events.append("assert")
            if self.active_owner not in (None, key.digest):
                raise RuntimeError("DPONE_XMIN_TARGET_AUTHORITY_CONFLICT")
            return
        self.events.append("transfer")
        transfer = authority.transfer_from
        if self.active_owner is None:
            raise RuntimeError("DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_NOT_REQUIRED")
        if (
            self.active_owner != transfer.state_key
            or transfer.xmin != self.old_xmin
            or transfer.revision != self.old_revision
        ):
            raise RuntimeError("DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_BINDING_MISMATCH")
        self.active_owner = None

    def compare_and_set_with_receipt(self, *, executor, key, **_kwargs):
        self.events.append("cas")
        self.executor = executor
        self.active_owner = key.digest
        return CheckpointCommitOutcome("load-1", 101, candidate_revision=1)

    def consume_repair_authority(self, **kwargs):
        self.events.append("consume")
        self.repair_consumptions.append(kwargs)
        if self.fail_consumption:
            raise RuntimeError("repair consumption denied")

    def restore_old_owner(self) -> None:
        self.active_owner = self.old_key


def _restore_transfer_on_rollback(connector: _FinalizerConnector, state: _TransferState) -> None:
    rollback = connector.rollback

    def _rollback() -> None:
        state.restore_old_owner()
        rollback()

    connector.rollback = _rollback  # type: ignore[method-assign]


class _MismatchedProbeRevisionState(_AtomicState):
    def compare_and_set_with_receipt(self, *, executor, **_kwargs):
        self.executor = executor
        return CheckpointCommitOutcome("load-1", 101, candidate_revision=8)

    def probe_receipt(self, **_kwargs):
        return CheckpointCommitOutcome("load-1", 101, candidate_revision=7)


def _finalizer_case(
    *,
    fail_state: bool = False,
    lose_commit_ack: bool = False,
    probe: bool = False,
    initial_target_rows: int = 0,
    repair_authority_ref: str | None = None,
    snapshot_rows: int = 1,
):
    connector = _FinalizerConnector(lose_commit_ack=lose_commit_ack)
    state = _AtomicState(fail=fail_state, probe=probe)
    connector.target_rows = initial_target_rows
    connector.active_target_rows = initial_target_rows
    connector.delta_rows = snapshot_rows
    connector.key_snapshot_rows = snapshot_rows
    finalizer = MssqlSnapshotFinalizer(_FinalizerStrategy(connector), state)
    finalizer._contract = SimpleNamespace(ensure_target=lambda *_args: None, validate_target=lambda *_args: None)
    config = SimpleNamespace(
        target_schema="sample_metrics",
        target_table="metrics_value",
        target_database="DWH_Dev",
        staging_database="DWH_Dev",
        repair_authority_ref=repair_authority_ref,
        options={
            "soft_delete": {"mode": "timestamp_only"},
            "__dpone_load_identity": {
                "run_id": "run-1",
                "load_id": "load-1",
                "extracted_at": "2025-12-31T21:00:00+00:00",
            },
        },
    )
    now = datetime.now(UTC)
    envelope = SimpleNamespace(
        baseline=True,
        key_receipt=SimpleNamespace(key_columns=("guid",), row_count=snapshot_rows),
        state_key=SimpleNamespace(
            digest=b"1" * 32,
            target_identity=b"t" * 32,
            target_database="DWH_Dev",
            target_schema="sample_metrics",
            target_table="metrics_value",
        ),
        previous_checkpoint=None,
        candidate_checkpoint=XMinState(101, now),
        snapshot_token="sha256:" + "a" * 64,
        scope_hash="sha256:" + "b" * 64,
    )
    delta = SimpleNamespace(table="delta", row_count=snapshot_rows)
    keys = SimpleNamespace(table="keys", row_count=snapshot_rows)
    policy = KeySnapshotReconciliationPolicy(enabled=True, mode="key_snapshot")
    return finalizer, connector, state, config, envelope, delta, keys, policy


def test_baseline_finalizer_commits_target_and_receipt_on_same_executor() -> None:
    finalizer, connector, state, config, envelope, delta, keys, policy = _finalizer_case()

    result = finalizer._finalize(config, [("guid", "uuid"), ("value", "int")], envelope, delta, keys, policy)

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED
    assert result.commit_receipt_id == "load-1"
    assert connector.commits == 1
    assert connector.rollbacks == 0
    assert state.executor is connector
    assert state.repair_admissions == []
    lock_call = next(call for call in connector.calls if "sp_getapplock" in call[0])
    assert lock_call[1][1] == 300_000
    insert_sql = next(sql for sql, _params in connector.calls if "INSERT INTO [target]" in sql)
    assert "[__dpone__run_id]" in insert_sql
    assert "[__dpone__load_id]" in insert_sql
    assert "[__dpone__extracted_at]" in insert_sql


def test_incremental_publication_head_is_rechecked_after_target_lock_before_business_dml() -> None:
    finalizer, connector, state, config, envelope, delta, keys, policy = _finalizer_case()
    config.options["xmin_execution"] = {"mode": "incremental", "handoff_id": "orders_v1"}
    envelope.previous_checkpoint = XMinState(100, datetime.now(UTC), revision=1)
    events: list[str] = []

    class _Guard:
        def acquire(self, _config, _envelope, *, timeout_ms):
            assert timeout_ms == 300_000
            assert any("sp_getapplock" in sql for sql, _params in connector.calls)
            events.append("publication-lock")
            return object()

        def require_locked(self, _authority, _envelope):
            events.append("publication-head")
            raise RuntimeError("postgres_xmin_handoff.publication_authority_changed")

    def assert_identity(*, executor, key):
        assert executor is connector
        assert key is envelope.state_key
        events.append("physical-identity")

    state.assert_physical_target_identity = assert_identity
    finalizer._publication_head = _Guard()

    with pytest.raises(RuntimeError, match="publication_authority_changed"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert events == ["publication-lock", "physical-identity", "publication-head"]
    assert connector.rollbacks == 1
    assert not any(
        "UPDATE t SET" in sql or "INSERT INTO [target]" in sql or "COUNT_BIG" in sql for sql, _params in connector.calls
    )


def test_pre_07427_shadow_seed_requires_migration_before_business_dml() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case()
    config.options["xmin_execution"] = {"mode": "incremental", "handoff_id": "orders_v1"}
    envelope.previous_checkpoint = XMinState(100, datetime.now(UTC), revision=1)
    connector.legacy_shadow_owner = "pre-0.74.27-initial-campaign"

    class _LegacyShadowSeedState(_AtomicState):
        def probe_receipt(self, **_kwargs):
            return CheckpointCommitOutcome("legacy-seed", 100, candidate_revision=1)

    state = _LegacyShadowSeedState()
    finalizer._state = state
    finalizer._publication_head = MssqlIncrementalPublicationHeadGuard(
        connector=connector,
        state_storage=state,
    )

    with pytest.raises(RuntimeError, match="legacy_publication_migration_required"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.rollbacks == 1
    assert not any(
        "UPDATE t SET" in sql or "INSERT INTO [target]" in sql or "COUNT_BIG" in sql for sql, _params in connector.calls
    )


def test_nonempty_target_missing_state_requires_one_shot_full_baseline_authority() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=1,
    )

    with pytest.raises(SnapshotReconciliationError, match="repair_authority.required"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.rollbacks == 1
    assert not any("UPDATE t SET" in sql or "INSERT INTO [target]" in sql for sql, _params in connector.calls)


def test_approved_full_baseline_consumes_authority_after_checkpoint_receipt() -> None:
    finalizer, connector, state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=1,
        repair_authority_ref="repair-work-item-001",
    )

    result = finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED
    assert result.reconciliation_metrics["repair_authority_id"] == "repair-work-item-001"
    assert state.repair_consumptions[0]["receipt_id"] == result.commit_receipt_id
    cas_index = next(i for i, call in enumerate(connector.calls) if "INSERT INTO [target]" in call[0])
    assert cas_index < len(connector.calls)


def test_empty_snapshot_without_authority_rolls_back_before_business_or_state_mutation() -> None:
    finalizer, connector, state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=3,
        snapshot_rows=0,
    )

    with pytest.raises(SnapshotReconciliationError, match="repair_authority.required"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1
    assert not any("UPDATE t SET" in sql or "INSERT INTO [target]" in sql for sql, _params in connector.calls)
    assert state.executor is None
    assert state.repair_admissions
    assert state.repair_consumptions == []
    assert state.checkpoint_commits == []


def test_approved_empty_snapshot_all_delete_consumes_authority_with_receipt() -> None:
    finalizer, connector, state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=3,
        repair_authority_ref="repair-work-item-all-delete",
        snapshot_rows=0,
    )

    result = finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert result.soft_deleted_rows == 3
    assert result.active_rows == 0
    assert result.reconciliation_metrics["repair_authority_id"] == "repair-work-item-all-delete"
    admission = state.repair_admissions[0]
    assert admission["require_empty_snapshot_override"] is True
    assert admission["missing_rows"] == 3
    assert admission["missing_ratio"] == 1.0
    consumption = state.repair_consumptions[0]
    assert consumption["receipt_id"] == result.commit_receipt_id
    assert consumption["observed_delete_rows"] == 3
    assert consumption["observed_delete_ratio"] == 1.0
    assert connector.commits == 1


def test_empty_source_and_empty_target_baseline_still_requires_full_repair_authority() -> None:
    finalizer, connector, state, config, envelope, delta, keys, policy = _finalizer_case(snapshot_rows=0)

    with pytest.raises(SnapshotReconciliationError, match="repair_authority.required"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1
    assert state.repair_consumptions == []

    config.repair_authority_ref = "repair-work-item-empty-baseline"
    result = finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert result.active_rows == result.total_rows == 0
    assert state.repair_admissions[-1]["require_full_baseline"] is True
    assert state.repair_consumptions[-1]["used_full_baseline"] is True
    assert connector.commits == 1


def test_snapshot_lineage_keeps_extraction_time_distinct_from_finalization_time() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case()

    finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    mutation_params = [
        params
        for sql, params in connector.calls
        if "[__dpone__extracted_at] = ?" in sql or "INSERT INTO [target]" in sql
    ]
    assert mutation_params
    for params in mutation_params:
        assert params[:2] == ("run-1", "load-1")
        assert params[2] == datetime(2025, 12, 31, 21)
        assert params[3] != params[2]


def test_insert_absent_has_one_target_column_per_select_value() -> None:
    finalizer, connector, _state, config, _envelope, delta, _keys, _policy = _finalizer_case()
    renderer = SnapshotReconciliationSql(
        connector.quote_identifier,
        SoftDeletePolicy(SoftDeleteMode.TIMESTAMP_ONLY),
    )

    finalizer._insert_absent(
        config,
        delta,
        ("guid",),
        ("guid", "value"),
        "'hash'",
        renderer,
        "run-1",
        "load-1",
        datetime(2025, 12, 31),
        datetime(2026, 1, 1),
    )

    sql = next(statement for statement, _params in connector.calls if "INSERT INTO [target]" in statement)
    assert (
        "INSERT INTO [target] ([guid], [value], [__dpone__run_id], [__dpone__load_id], "
        "[__dpone__row_hash], [__dpone__extracted_at], [__dpone__loaded_at])"
    ) in sql
    assert "SELECT s.[guid], s.[value], ?, ?, 'hash', ?, ? FROM [delta] AS s" in sql


def test_snapshot_applock_timeout_is_configurable_and_bounded() -> None:
    config = SimpleNamespace(options={"mssql_snapshot_lock_timeout_ms": 45_000})
    assert lock_timeout_ms(config) == 45_000

    config.options["mssql_snapshot_lock_timeout_ms"] = 3_600_001
    with pytest.raises(MssqlSnapshotOptionError, match="lock_timeout_invalid"):
        lock_timeout_ms(config)


def test_checkpoint_denial_rolls_back_target_dml() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(fail_state=True)

    with pytest.raises(RuntimeError, match="state denied"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1


def test_target_lock_uses_physical_identity_not_checkpoint_identity() -> None:
    first = _finalizer_case()
    second = _finalizer_case()
    second[4].state_key = SimpleNamespace(
        digest=b"2" * 32,
        target_identity=b"t" * 32,
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_value",
    )

    first[0]._finalize(first[3], [("guid", "uuid")], first[4], first[5], first[6], first[7])
    second[0]._finalize(second[3], [("guid", "uuid")], second[4], second[5], second[6], second[7])

    first_lock = next((sql, params) for sql, params in first[1].calls if "sp_getapplock" in sql)
    second_lock = next((sql, params) for sql, params in second[1].calls if "sp_getapplock" in sql)
    assert first_lock[1][0] == second_lock[1][0] == target_lock_resource(b"t" * 32)
    assert first_lock[1][0].startswith("dpone:target:")
    assert "[DWH_Dev].sys.sp_getapplock" in first_lock[0]
    assert target_lock_resource(b"u" * 32) != first_lock[1][0]


def test_conflicting_target_authority_rolls_back_before_business_dml() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case()
    finalizer._state = _ConflictingTargetAuthorityState()

    with pytest.raises(RuntimeError, match="DPONE_XMIN_TARGET_AUTHORITY_CONFLICT"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1
    assert not any("INSERT INTO [target]" in sql or "UPDATE t SET" in sql for sql, _params in connector.calls)


def test_exact_target_authority_transfer_commits_with_new_checkpoint_and_consumption() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=1,
        repair_authority_ref="repair-transfer-001",
    )
    old_key = b"o" * 32
    state = _TransferState(old_key=old_key)
    finalizer._state = state

    result = finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED
    assert result.reconciliation_metrics["repair_target_authority_transfer"] is True
    assert state.active_owner == envelope.state_key.digest
    assert state.events == ["admit", "transfer", "cas", "consume"]
    assert len(state.repair_consumptions) == 1
    assert connector.commits == 1
    assert connector.rollbacks == 0


def test_full_baseline_without_transfer_binding_cannot_take_an_active_old_owner() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=1,
        repair_authority_ref="repair-without-transfer",
    )
    finalizer._state = _TransferState(old_key=b"o" * 32, include_transfer=False)

    with pytest.raises(RuntimeError, match="DPONE_XMIN_TARGET_AUTHORITY_CONFLICT"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1


def test_transfer_binding_fails_when_no_old_active_owner_needs_transfer() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(
        repair_authority_ref="repair-transfer-unnecessary",
    )
    state = _TransferState(old_key=b"o" * 32)
    state.active_owner = None
    finalizer._state = state

    with pytest.raises(RuntimeError, match="TRANSFER_NOT_REQUIRED"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert state.events == ["admit", "transfer"]
    assert connector.commits == 0
    assert connector.rollbacks == 1


def test_target_authority_transfer_mismatch_and_reuse_fail_before_business_dml() -> None:
    for state, error in (
        (_TransferState(old_key=b"o" * 32, transfer_key=b"x" * 32), "TRANSFER_BINDING_MISMATCH"),
        (_TransferState(old_key=b"o" * 32, already_consumed=True), "already_consumed"),
    ):
        finalizer, connector, _old_state, config, envelope, delta, keys, policy = _finalizer_case(
            initial_target_rows=1,
            repair_authority_ref="repair-transfer-invalid",
        )
        finalizer._state = state

        with pytest.raises((RuntimeError, SnapshotReconciliationError), match=error):
            finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

        assert connector.commits == 0
        assert connector.rollbacks == 1
        assert not any("INSERT INTO [target]" in sql or "UPDATE t SET" in sql for sql, _params in connector.calls)


def test_stale_old_process_fails_after_authority_was_transferred() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(initial_target_rows=1)
    old_key = b"o" * 32
    new_key = bytes(envelope.state_key.digest)
    state = _TransferState(old_key=old_key)
    state.active_owner = new_key
    finalizer._state = state
    envelope.state_key = SimpleNamespace(
        digest=old_key,
        target_identity=b"t" * 32,
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_value",
    )
    envelope.previous_checkpoint = XMinState(90, datetime.now(UTC), revision=4)
    envelope.baseline = False

    with pytest.raises(RuntimeError, match="DPONE_XMIN_TARGET_AUTHORITY_CONFLICT"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert state.active_owner == new_key
    assert state.events == ["assert"]
    assert connector.rollbacks == 1


def test_transfer_rolls_back_to_old_active_owner_when_consumption_fails() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case(
        initial_target_rows=1,
        repair_authority_ref="repair-transfer-rollback",
    )
    old_key = b"o" * 32
    state = _TransferState(old_key=old_key, fail_consumption=True)
    finalizer._state = state
    _restore_transfer_on_rollback(connector, state)

    with pytest.raises(RuntimeError, match="repair consumption denied"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert state.events == ["admit", "transfer", "cas", "consume"]
    assert state.active_owner == old_key
    assert connector.commits == 0
    assert connector.rollbacks == 1


def test_equal_xmin_stale_revision_rolls_back_target_dml() -> None:
    finalizer, connector, _state, config, envelope, delta, keys, policy = _finalizer_case()
    finalizer._state = _StaleRevisionState()
    now = datetime.now(UTC)
    envelope.previous_checkpoint = XMinState(100, now, revision=7)
    envelope.candidate_checkpoint = XMinState(100, now)

    with pytest.raises(RuntimeError, match="DPONE_XMIN_CHECKPOINT_CAS_MISMATCH"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1


def test_receipt_written_precommit_failure_rolls_back_and_retry_is_safe(monkeypatch) -> None:
    case = _finalizer_case()
    finalizer, connector, state, config, envelope, delta, keys, policy = case
    real_compare_and_set = state.compare_and_set_with_receipt

    def persist_receipt_then_fail(**kwargs):
        real_compare_and_set(**kwargs)
        raise RuntimeError("failure before commit invocation")

    monkeypatch.setattr(state, "compare_and_set_with_receipt", persist_receipt_then_fail)
    with pytest.raises(RuntimeError, match="failure before commit invocation"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 0
    assert connector.rollbacks == 1
    assert connector.closes == 0
    assert connector.transaction_open is False
    assert connector.target_rows == 0

    monkeypatch.setattr(state, "compare_and_set_with_receipt", real_compare_and_set)
    result = finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED
    assert connector.commits == 1
    assert connector.rollbacks == 1
    assert connector.closes == 0
    assert connector.transaction_open is False


def test_commit_ack_loss_uses_receipt_without_replaying_mutation() -> None:
    case = _finalizer_case(lose_commit_ack=True, probe=True)
    finalizer, connector, _state, config, envelope, delta, keys, policy = case

    result = finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE
    assert connector.commits == 1
    assert connector.rollbacks == 0
    assert connector.closes == 1


def test_commit_ack_loss_without_receipt_closes_and_raises_typed_unknown() -> None:
    case = _finalizer_case(lose_commit_ack=True, probe=False)
    finalizer, connector, _state, config, envelope, delta, keys, policy = case

    with pytest.raises(MssqlCommitOutcomeUnknown, match="commit_outcome_unknown"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 1
    assert connector.rollbacks == 0
    assert connector.closes == 1


def test_commit_ack_probe_rejects_receipt_with_wrong_state_revision() -> None:
    case = _finalizer_case(lose_commit_ack=True)
    finalizer, connector, _state, config, envelope, delta, keys, policy = case
    finalizer._state = _MismatchedProbeRevisionState()

    with pytest.raises(MssqlCommitOutcomeUnknown, match="commit_outcome_unknown"):
        finalizer._finalize(config, [("guid", "uuid")], envelope, delta, keys, policy)

    assert connector.commits == 1
    assert connector.rollbacks == 0
    assert connector.closes == 1


def test_snapshot_finalizer_rejects_unsafe_raw_bulk_bypass_before_staging() -> None:
    finalizer, _connector, _state, config, envelope, _delta, _keys, _policy = _finalizer_case()
    config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    config.options["schema_contract"] = {"columns": {"guid": {"nullable": False}}}
    config.options["allow_unsafe_raw_mssql_bulk_files"] = True

    with pytest.raises(SnapshotReconciliationError, match="unsafe_raw_bulk_files_forbidden"):
        finalizer.load(config, SimpleNamespace(schema=[]), envelope)


def test_snapshot_finalizer_rejects_changed_target_coordinates_before_staging() -> None:
    finalizer, connector, state, config, envelope, _delta, _keys, _policy = _finalizer_case()
    config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    config.target_table = "Metrics_Value"

    with pytest.raises(SnapshotReconciliationError, match="target_coordinates_changed"):
        finalizer.load(config, SimpleNamespace(schema=[]), envelope)

    assert connector.calls == []
    assert state.executor is None


def test_receipt_probe_commit_outcome_cleans_staging_but_delegates_source_ownership() -> None:
    finalizer, _connector, _state, config, envelope, delta, keys, _policy = _finalizer_case()
    config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    config.options["schema_contract"] = {"columns": {"guid": {"nullable": False}}}
    cleanup_counts = {"delta": 0, "keys": 0, "envelope": 0}
    delta.cleanup = lambda: cleanup_counts.__setitem__("delta", cleanup_counts["delta"] + 1)
    keys.cleanup = lambda: cleanup_counts.__setitem__("keys", cleanup_counts["keys"] + 1)
    envelope.materialize = lambda *_args: delta
    envelope.materialize_keys = lambda *_args: keys
    envelope.delta_schema = (("guid", "uuid"),)
    envelope.key_schema = (("guid", "uuid"),)
    envelope.cleanup = lambda: cleanup_counts.__setitem__("envelope", cleanup_counts["envelope"] + 1)
    finalizer._contract = SimpleNamespace(
        validate_staging=lambda *_args: None,
        index_keys=lambda *_args: None,
    )
    finalizer._normalizer = SimpleNamespace(normalize=lambda *_args: NormalizedSnapshotStaging(delta=delta, keys=keys))
    finalizer._key_staging_config = lambda value: value  # type: ignore[method-assign]
    finalizer._finalize = lambda *_args: LoadResult(  # type: ignore[method-assign]
        inserted_rows=0,
        updated_rows=0,
        total_rows=0,
        commit_receipt_id="load-1",
        commit_outcome=AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
    )

    result = finalizer.load(config, SimpleNamespace(schema=[]), envelope)

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE
    assert cleanup_counts == {"delta": 1, "keys": 1, "envelope": 0}


def test_unknown_commit_preserves_tables_but_releases_authority_leases() -> None:
    finalizer, _connector, _state, config, envelope, delta, keys, _policy = _finalizer_case()
    config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    config.options["schema_contract"] = {"columns": {"guid": {"nullable": False}}}
    cleanup_counts = {"delta": 0, "keys": 0}
    released: list[object] = []
    staging_manager = SimpleNamespace(release_authority_lease=released.append)
    delta.staging_manager = staging_manager
    keys.staging_manager = staging_manager
    delta.cleanup = lambda: cleanup_counts.__setitem__("delta", cleanup_counts["delta"] + 1)
    keys.cleanup = lambda: cleanup_counts.__setitem__("keys", cleanup_counts["keys"] + 1)
    envelope.materialize = lambda *_args: delta
    envelope.materialize_keys = lambda *_args: keys
    envelope.delta_schema = (("guid", "uuid"),)
    envelope.key_schema = (("guid", "uuid"),)
    finalizer._contract = SimpleNamespace(
        validate_staging=lambda *_args: None,
        index_keys=lambda *_args: None,
    )
    finalizer._normalizer = SimpleNamespace(normalize=lambda *_args: NormalizedSnapshotStaging(delta=delta, keys=keys))

    def unknown_commit(*_args):
        raise MssqlCommitOutcomeUnknown()

    finalizer._finalize = unknown_commit  # type: ignore[method-assign]

    with pytest.raises(MssqlCommitOutcomeUnknown, match="commit_outcome_unknown"):
        finalizer.load(config, SimpleNamespace(schema=[]), envelope)

    assert cleanup_counts == {"delta": 0, "keys": 0}
    assert released == [delta, keys]


def test_unknown_commit_surfaces_authority_release_failure_without_dropping_tables() -> None:
    finalizer, _connector, _state, config, envelope, delta, keys, _policy = _finalizer_case()
    config.options["reconciliation"] = {"enabled": True, "mode": "key_snapshot"}
    config.options["schema_contract"] = {"columns": {"guid": {"nullable": False}}}
    cleanup_counts = {"delta": 0, "keys": 0}

    def fail_release(_artifact) -> None:
        raise OSError("reviewed lease close failure")

    staging_manager = SimpleNamespace(release_authority_lease=fail_release)
    for name, artifact in (("delta", delta), ("keys", keys)):
        artifact.staging_manager = staging_manager
        artifact.cleanup = lambda name=name: cleanup_counts.__setitem__(name, cleanup_counts[name] + 1)
    envelope.materialize = lambda *_args: delta
    envelope.materialize_keys = lambda *_args: keys
    envelope.delta_schema = (("guid", "uuid"),)
    envelope.key_schema = (("guid", "uuid"),)
    finalizer._contract = SimpleNamespace(
        validate_staging=lambda *_args: None,
        index_keys=lambda *_args: None,
    )
    finalizer._normalizer = SimpleNamespace(normalize=lambda *_args: NormalizedSnapshotStaging(delta=delta, keys=keys))

    def unknown_commit(*_args):
        raise MssqlCommitOutcomeUnknown()

    finalizer._finalize = unknown_commit  # type: ignore[method-assign]

    with pytest.raises(MssqlCommitOutcomeUnknown, match="commit_outcome_unknown") as caught:
        finalizer.load(config, SimpleNamespace(schema=[]), envelope)

    assert cleanup_counts == {"delta": 0, "keys": 0}
    assert caught.value.__notes__ == [
        "staging authority lease release failed: artifact=delta; cause_type=OSError",
        "staging authority lease release failed: artifact=keys; cause_type=OSError",
    ]


class _StageConnector:
    def __init__(self, *, invalid: str | None = None, fail_delta_native_drop: bool = False) -> None:
        self.invalid = invalid
        self.fail_delta_native_drop = fail_delta_native_drop
        self.statements: list[str] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        prefix = f"[{database}]." if database else ""
        return f"{prefix}[{schema}].[{table}]"

    def execute_query(self, sql, params=None):
        del params
        rendered = str(sql)
        self.statements.append(rendered)
        if self.fail_delta_native_drop and "DROP TABLE IF EXISTS" in rendered and "delta_raw_native_" in rendered:
            raise RuntimeError("delta native cleanup failed")
        return 1

    def get_records(self, sql, params=None, as_dict=False):
        del params, as_dict
        rendered = str(sql)
        self.statements.append(rendered)
        if "AS raw_reserved_bytes" in rendered:
            return [{"raw_reserved_bytes": 65_536, "available_data_bytes": 1_073_741_824}]
        if "schema_exists" in rendered:
            return [{"schema_exists": 1}]
        if self.invalid == "delta_digest" and "invalid_digest" in rendered and "delta_raw" in rendered:
            return [{"invalid_digest": 1}]
        if self.invalid == "length" and "invalid_value" in rendered and "DATALENGTH" in rendered:
            return [{"invalid_value": 1}]
        if "AS [invalid_hash]" in rendered and "AS [too_long]" in rendered:
            key_length_invalid = self.invalid == "key_length" and "keys_raw_decoded_" in rendered
            return [
                {
                    "invalid_hash": 0,
                    "too_long": 1 if self.invalid == "length" or key_length_invalid else 0,
                    "invalid": 0,
                    "lossy": 0,
                }
            ]
        if "COUNT_BIG(*) AS row_count" in rendered:
            return [{"row_count": 0 if " WHERE " in rendered else 1}]
        return []


class _StageStrategy:
    def __init__(self, connector: _StageConnector) -> None:
        self.connector = connector
        self.staging_manager = MSSQLStagingManager(connector)

    def _staging_name(self, staging: StagingTableArtifact) -> str:
        return self.connector.qualified_name(staging.schema, staging.table, database=staging.database)

    def _staging_select_expression(self, staging, column, alias):
        return build_mssql_staging_select_expression(
            column=column,
            alias=alias,
            quote_identifier=self.connector.quote_identifier,
            column_types=staging.column_types,
            hex_binary_columns=staging.hex_binary_columns,
            target_column_types=staging.target_column_types,
            bulk_text_codec=staging.bulk_text_codec,
        )


def _snapshot_stage_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_config",
        target_database="DWH_Dev",
        target_schema="sample_metrics",
        target_table="metrics_config",
        staging_database="DWH_Dev",
        staging_schema="sample_metrics",
        staging_table="delta_raw",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["metric_code"],
        options={
            "reconciliation": {"enabled": True, "mode": "key_snapshot"},
            "schema_contract": {
                "columns": {
                    "metric_code": {"nullable": False},
                    "metric_value": {"nullable": False},
                    "calculated_at": {"nullable": True},
                }
            },
            "physical_design": {
                "apply_runtime": False,
                "columns": {"metric_code": {"target_type": {"mssql": "nvarchar(450)"}}},
            },
        },
    )


def _raw_snapshot_stages(connector: _StageConnector):
    strategy = _StageStrategy(connector)
    config = _snapshot_stage_config()
    delta_schema = (
        ("metric_code", "text"),
        ("metric_value", "double precision"),
        (DELTA_HASH_COLUMN, "char(64)"),
    )
    key_schema = (("metric_code", "text"), ("__dpone__key_hash", "char(64)"))
    delta = strategy.staging_manager.create(config, delta_schema)
    key_config = SimpleNamespace(**vars(config))
    key_config.staging_table = "keys_raw"
    keys = strategy.staging_manager.create(key_config, key_schema)
    codec = BulkTextCodec()
    for artifact in (delta, keys):
        artifact.row_count = 1
        artifact.bulk_text_codec = codec
    envelope = SimpleNamespace(
        delta_schema=delta_schema,
        key_schema=key_schema,
        delta_receipt=SimpleNamespace(complete=True, row_count=1),
        key_receipt=SimpleNamespace(complete=True, row_count=1, key_columns=("metric_code",)),
    )
    return strategy, config, envelope, delta, keys


def test_raw_wire_is_unbounded_then_normalized_to_indexable_decoded_keys() -> None:
    connector = _StageConnector()
    strategy, config, envelope, raw_delta, raw_keys = _raw_snapshot_stages(connector)

    normalized = MssqlSnapshotStagingNormalizer(strategy).normalize(
        config,
        envelope,
        raw_delta,
        raw_keys,
    )

    raw_ddl = next(sql for sql in connector.statements if "CREATE TABLE" in sql and "delta_raw]" in sql)
    native_ddl = next(sql for sql in connector.statements if "CREATE TABLE" in sql and "delta_raw_native_" in sql)
    key_native_ddl = next(sql for sql in connector.statements if "CREATE TABLE" in sql and "keys_raw_native_" in sql)
    assert "[metric_code] nvarchar(max) NULL" in raw_ddl
    assert "[metric_value] nvarchar(max) NULL" in raw_ddl
    assert f"[metric_code] nvarchar(450) COLLATE {MSSQL_TEXT_KEY_COLLATION} NOT NULL" in native_ddl
    assert f"[metric_code] nvarchar(450) COLLATE {MSSQL_TEXT_KEY_COLLATION} NOT NULL" in key_native_ddl
    decode_projection = next(
        sql for sql in connector.statements if "INSERT INTO" in sql and "delta_raw_decoded_" in sql
    )
    assert "NCHAR(29)" in decode_projection
    assert decode_projection.count("CASE WHEN (r.[metric_code]) COLLATE Latin1_General_100_BIN2") == 1
    projection = next(sql for sql in connector.statements if "INSERT INTO" in sql and "delta_raw_native_" in sql)
    assert " WITH (TABLOCK) " in projection
    assert "NCHAR(29)" not in projection
    assert " AS [metric_code]" in projection
    decoded_name = next(
        statement.split("INSERT INTO ", 1)[1].split(" ", 1)[0]
        for statement in connector.statements
        if "INSERT INTO" in statement and "delta_raw_decoded_" in statement
    )
    assert f"{decoded_name} AS r" in projection
    assert normalized.delta.bulk_text_codec is None
    assert normalized.keys.bulk_text_codec is None
    key_digest_sql = next(sql for sql in connector.statements if "invalid_digest" in sql and "keys_raw]" in sql)
    assert "CONCAT(COALESCE" not in key_digest_sql
    conversion_sql = next(sql for sql in connector.statements if "[too_long]" in sql and "DATALENGTH" in sql)
    assert "NCHAR(29)" not in conversion_sql
    assert "DATALENGTH(r.[metric_code])" in conversion_sql
    assert "> 900" in conversion_sql
    assert "CASE WHEN r.[__dpone__delta_hash]" not in conversion_sql
    assert "DATALENGTH(CONVERT(varchar(max), r.[__dpone__delta_hash])) > 64" in conversion_sql


@pytest.mark.parametrize("dtype", ["uuid", "text"])
def test_single_column_raw_key_digest_uses_legal_scalar_hash_input(dtype: str) -> None:
    connector = _StageConnector()
    strategy = _StageStrategy(connector)
    artifact = StagingTableArtifact(
        database="DWH_Dev",
        schema="sample_metrics",
        table="single_key_raw",
        columns=("key", "__dpone__key_hash"),
        staging_manager=strategy.staging_manager,
        column_types={"key": "nvarchar(max)", "__dpone__key_hash": "char(64)"},
        bulk_text_codec=BulkTextCodec(),
    )

    MssqlSnapshotStagingNormalizer(strategy)._validate_raw_digest(
        artifact,
        (("key", dtype), ("__dpone__key_hash", "char(64)")),
        "__dpone__key_hash",
    )

    sql = next(statement for statement in connector.statements if "invalid_digest" in statement)
    assert "CONCAT(COALESCE" not in sql
    assert "HASHBYTES('SHA2_256', CONVERT(varbinary(max), COALESCE" in sql


def test_scalar_codec_expression_never_leaks_select_alias_into_update_or_hash() -> None:
    connector = _StageConnector()
    strategy, _config, _envelope, raw_delta, _raw_keys = _raw_snapshot_stages(connector)

    scalar = staging_scalar_expression(strategy, raw_delta, "metric_code", "s")
    expression = row_hash_expression(
        (("metric_code", "nvarchar(450)"), ("metric_value", "float(53)")),
        lambda column: staging_scalar_expression(strategy, raw_delta, column, "s"),
    )

    assert " AS [metric_code]" not in scalar
    assert " AS [metric_code]" not in expression
    assert "CONVERT(varchar(99)," in expression
    assert ", 3)" in expression
    assert "DATALENGTH(CONVERT(varbinary(max)" in expression


def test_single_business_column_hash_never_renders_unary_concat() -> None:
    expression = row_hash_expression(
        (("guid", "uniqueidentifier"),),
        lambda column: f"s.[{column}]",
    )

    assert "HASHBYTES('SHA2_256', CONCAT(CONCAT(" not in expression
    assert expression.count("CONCAT(") == 2


def test_raw_digest_and_bounded_key_length_fail_before_native_staging() -> None:
    digest_connector = _StageConnector(invalid="delta_digest")
    strategy, config, envelope, raw_delta, raw_keys = _raw_snapshot_stages(digest_connector)
    with pytest.raises(SnapshotReconciliationError, match="delta_checksum_mismatch"):
        MssqlSnapshotStagingNormalizer(strategy).normalize(config, envelope, raw_delta, raw_keys)
    assert not any("_native_" in sql and "CREATE TABLE" in sql for sql in digest_connector.statements)

    length_connector = _StageConnector(invalid="length")
    strategy, config, envelope, raw_delta, raw_keys = _raw_snapshot_stages(length_connector)
    with pytest.raises(SnapshotReconciliationError, match="normalized_staging_value_too_long"):
        MssqlSnapshotStagingNormalizer(strategy).normalize(config, envelope, raw_delta, raw_keys)
    assert not any("_native_" in sql and "CREATE TABLE" in sql for sql in length_connector.statements)


def test_key_normalization_error_survives_delta_native_cleanup_failure() -> None:
    connector = _StageConnector(invalid="key_length", fail_delta_native_drop=True)
    strategy, config, envelope, raw_delta, raw_keys = _raw_snapshot_stages(connector)

    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_snapshot_reconciliation.normalized_staging_value_too_long",
    ):
        MssqlSnapshotStagingNormalizer(strategy).normalize(config, envelope, raw_delta, raw_keys)

    assert any("DROP TABLE IF EXISTS" in sql and "keys_raw_decoded_" in sql for sql in connector.statements)
    assert any("DROP TABLE IF EXISTS" in sql and "delta_raw_native_" in sql for sql in connector.statements)


def test_snapshot_native_names_preserve_concurrent_raw_identity_at_length_boundary() -> None:
    connector = _StageConnector()
    strategy, config, envelope, raw_delta, raw_keys = _raw_snapshot_stages(connector)
    raw_delta.table = "x" * 115 + "_delta"
    raw_keys.table = "x" * 115 + "_keys"

    normalized = MssqlSnapshotStagingNormalizer(strategy).normalize(
        config,
        envelope,
        raw_delta,
        raw_keys,
    )

    assert normalized.delta.table != normalized.keys.table
    assert all(len(artifact.table) <= 120 for artifact in (normalized.delta, normalized.keys))
    assert all(re.search(r"_native_[0-9a-f]{12}$", artifact.table) for artifact in (normalized.delta, normalized.keys))
    normalized.cleanup()


def test_full_business_target_shape_rejects_lossy_or_unexpected_columns() -> None:
    config = _snapshot_stage_config()
    schema = (
        ("metric_code", "text"),
        ("metric_value", "double precision"),
        ("calculated_at", "timestamp with time zone"),
        (DELTA_HASH_COLUMN, "char(64)"),
    )
    exact = {
        "metric_code": {
            "type_name": "nvarchar",
            "max_length": 900,
            "precision": 0,
            "scale": 0,
            "is_nullable": 0,
            "is_computed": 0,
        },
        "metric_value": {
            "type_name": "float",
            "max_length": 8,
            "precision": 53,
            "scale": 0,
            "is_nullable": 0,
            "is_computed": 0,
        },
        "calculated_at": {
            "type_name": "datetimeoffset",
            "max_length": 10,
            "precision": 33,
            "scale": 6,
            "is_nullable": 1,
            "is_computed": 0,
        },
    }

    _validate_business_shape(config, schema, ("metric_code",), exact)

    with pytest.raises(SnapshotReconciliationError, match="target_business_shape_invalid"):
        _validate_business_shape(
            config,
            schema,
            ("metric_code",),
            {**exact, "metric_value": {**exact["metric_value"], "type_name": "real", "precision": 24}},
        )
    with pytest.raises(SnapshotReconciliationError, match="target_business_shape_invalid"):
        _validate_business_shape(
            config,
            schema,
            ("metric_code",),
            {**exact, "__dpone__xmin": {"type_name": "bigint", "is_nullable": 1}},
        )
    with pytest.raises(SnapshotReconciliationError, match="target_business_shape_invalid"):
        _validate_business_shape(
            config,
            schema,
            ("metric_code",),
            {**exact, "metric_value": {**exact["metric_value"], "is_nullable": 1}},
        )


@pytest.mark.parametrize(
    ("declared_type", "type_name", "max_length", "precision", "scale"),
    [
        ("nvarchar(450)", "nvarchar", 900, 0, 0),
        ("nvarchar(max)", "nvarchar", -1, 0, 0),
        ("char(64)", "char", 64, 0, 0),
        ("uniqueidentifier", "uniqueidentifier", 16, 0, 0),
        ("int", "int", 4, 10, 0),
        ("float(53)", "float", 8, 53, 0),
        ("date", "date", 3, 10, 0),
        ("datetime2(7)", "datetime2", 8, 27, 7),
        ("datetimeoffset(6)", "datetimeoffset", 10, 33, 6),
        ("time(6)", "time", 5, 15, 6),
    ],
)
def test_exact_do_target_ddl_types_match_sql_server_catalog_shapes(
    declared_type: str,
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
) -> None:
    metadata = {
        "type_name": type_name,
        "max_length": max_length,
        "precision": precision,
        "scale": scale,
    }

    assert metadata_matches_type(metadata, declared_type)


@pytest.mark.parametrize(
    ("declared_type", "metadata"),
    [
        (
            "datetime2(6)",
            {"type_name": "datetime2", "max_length": 8, "precision": 26, "scale": 6},
        ),
        (
            "datetimeoffset(6)",
            {"type_name": "datetimeoffset", "max_length": 10, "precision": 33, "scale": 6},
        ),
        (
            "time(6)",
            {"type_name": "time", "max_length": 5, "precision": 15, "scale": 6},
        ),
        (
            "float(53)",
            {"type_name": "float", "max_length": 8, "precision": 53, "scale": 0},
        ),
        (
            "uniqueidentifier",
            {"type_name": "uniqueidentifier", "max_length": 16, "precision": 0, "scale": 0},
        ),
        (
            "char(64)",
            {"type_name": "char", "max_length": 64, "precision": 0, "scale": 0},
        ),
        (
            "nvarchar(450)",
            {"type_name": "nvarchar", "max_length": 900, "precision": 0, "scale": 0},
        ),
    ],
)
@pytest.mark.parametrize("field", ["max_length", "precision", "scale"])
def test_exact_target_type_matcher_rejects_catalog_metadata_drift(
    declared_type: str,
    metadata: dict[str, object],
    field: str,
) -> None:
    actual_value = metadata[field]
    assert isinstance(actual_value, int)
    drifted = {**metadata, field: actual_value + 1}

    assert not metadata_matches_type(drifted, declared_type)


@pytest.mark.parametrize(
    ("base", "scale", "max_length", "precision"),
    [
        ("datetime2", 0, 6, 19),
        ("datetime2", 1, 6, 21),
        ("datetime2", 2, 6, 22),
        ("datetime2", 3, 7, 23),
        ("datetime2", 4, 7, 24),
        ("datetime2", 5, 8, 25),
        ("datetime2", 6, 8, 26),
        ("datetime2", 7, 8, 27),
        ("time", 0, 3, 8),
        ("time", 1, 3, 10),
        ("time", 2, 3, 11),
        ("time", 3, 4, 12),
        ("time", 4, 4, 13),
        ("time", 5, 5, 14),
        ("time", 6, 5, 15),
        ("time", 7, 5, 16),
        ("datetimeoffset", 0, 8, 26),
        ("datetimeoffset", 1, 8, 28),
        ("datetimeoffset", 2, 8, 29),
        ("datetimeoffset", 3, 9, 30),
        ("datetimeoffset", 4, 9, 31),
        ("datetimeoffset", 5, 10, 32),
        ("datetimeoffset", 6, 10, 33),
        ("datetimeoffset", 7, 10, 34),
    ],
)
def test_temporal_type_matcher_uses_exact_sql_server_catalog_shape(
    base: str,
    scale: int,
    max_length: int,
    precision: int,
) -> None:
    metadata = {
        "type_name": base,
        "max_length": max_length,
        "precision": precision,
        "scale": scale,
    }

    assert metadata_matches_type(metadata, f"{base}({scale})")


@pytest.mark.parametrize(
    ("declared_type", "max_length", "precision"),
    [
        ("float(1)", 4, 24),
        ("float(24)", 4, 24),
        ("float(25)", 8, 53),
        ("float(53)", 8, 53),
        ("float", 8, 53),
    ],
)
def test_float_type_matcher_uses_sql_server_canonical_precision(
    declared_type: str,
    max_length: int,
    precision: int,
) -> None:
    metadata = {
        "type_name": "float",
        "max_length": max_length,
        "precision": precision,
        "scale": 0,
    }

    assert metadata_matches_type(metadata, declared_type)


@pytest.mark.parametrize("missing_field", ["max_length", "precision", "scale"])
def test_exact_target_type_matcher_fails_closed_when_catalog_shape_is_incomplete(missing_field: str) -> None:
    metadata = {
        "type_name": "datetimeoffset",
        "max_length": 10,
        "precision": 33,
        "scale": 6,
    }
    metadata.pop(missing_field)

    assert not metadata_matches_type(metadata, "datetimeoffset(6)")


def test_text_key_target_metadata_requires_exact_binary_collation() -> None:
    metadata = {"metric_code": {"collation_name": MSSQL_TEXT_KEY_COLLATION}}

    validate_text_key_metadata(("metric_code",), {"metric_code": "nvarchar(450)"}, metadata)

    with pytest.raises(SnapshotReconciliationError, match="binary_collation_required"):
        validate_text_key_metadata(
            ("metric_code",),
            {"metric_code": "nvarchar(450)"},
            {"metric_code": {"collation_name": "Cyrillic_General_CI_AS"}},
        )


def test_delta_keys_must_be_subset_of_complete_snapshot_before_transaction() -> None:
    connector = _StageConnector()
    strategy, _config, envelope, delta, keys = _raw_snapshot_stages(connector)
    connector.invalid = "extra_delta"

    original = connector.get_records

    def records(sql, params=None, as_dict=False):
        if "WHERE NOT EXISTS" in str(sql) and " AS d " in str(sql):
            return [(1,)]
        return original(sql, params, as_dict)

    connector.get_records = records
    contract = MssqlSnapshotContract(strategy)
    with pytest.raises(SnapshotReconciliationError, match="delta_key_not_in_snapshot"):
        contract.validate_staging(envelope, delta, keys)


def test_complete_empty_staging_keeps_integrity_checks_and_defers_policy_to_transaction() -> None:
    connector = _StageConnector()
    strategy, _config, envelope, delta, keys = _raw_snapshot_stages(connector)
    delta.row_count = keys.row_count = 0
    envelope.delta_receipt.row_count = envelope.key_receipt.row_count = 0

    MssqlSnapshotContract(strategy).validate_staging(envelope, delta, keys)

    assert any("WHERE NOT EXISTS" in sql for sql in connector.statements)
