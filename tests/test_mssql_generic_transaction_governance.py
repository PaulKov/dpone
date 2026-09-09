from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dpone.backfill.execution_policy import normalize_backfill_execution_policy, plan_hash
from dpone.backfill.planner import plan_chunks
from dpone.backfill.portable_scope_campaign import CampaignPortableScopeBinding
from dpone.backfill.process_lane_contracts import (
    ProcessLaneBinding,
    ProcessLaneDispatch,
)
from dpone.backfill.runtime_execution import BackfillChunkExecutor
from dpone.backfill.shadow_append_authority import (
    SHADOW_APPEND_AUTHORITY_OPTION,
    issue_shadow_append_authority,
)
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationRequest,
    MssqlPayloadCommitEvidence,
    MssqlReceiptMetrics,
    MssqlSourceLifecycleEvidence,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionContractError,
    MssqlTransactionOperation,
    operation_owner_digest,
    operation_scope_hash,
)
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeBindingError,
    PortableScopeColumnContract,
)
from dpone.contracts.run_context import RunContext
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.consumed_payload_evidence import (
    ConsumedPayloadEvidence,
    ConsumedPayloadPartEvidence,
)
from dpone.runtime.etl.mssql_fresh_target_preplan import resolve_mssql_target_columns
from dpone.runtime.etl.mssql_operation_lease import LEASE_OPTION, MssqlOperationLeaseHeartbeat
from dpone.runtime.etl.mssql_process_lane_authority import (
    _ParentMssqlAttemptProjector,
    _ParentMssqlLaneSourceAuthority,
    _ParentPortableScopeBinder,
    _validate_operation_binding,
)
from dpone.runtime.etl.mssql_schema_preplan import MssqlSchemaPreplanner
from dpone.runtime.etl.mssql_transaction_admission import (
    ADMISSION_OPTION,
    MssqlTransactionAdmissionService,
)
from dpone.runtime.etl.mssql_transaction_identity import (
    build_mssql_attempt_request,
    invocation_identity,
    invocation_route_fingerprint,
    operation_request,
    resolve_source_physical_identity,
)
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.governance.mssql_hook_replay_policy import (
    MSSQL_HOOK_GRAPH_SHA256_OPTION,
    mssql_hook_graph_contract,
)
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sinks.mssql_receipt_projection import load_result_from_mssql_receipt
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlIndexState,
    MssqlSchemaCatalogSnapshot,
    catalog_column_from_definition,
)
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
from dpone.runtime.sinks.mssql_transaction_requirement import MSSQL_GENERIC_TRANSACTION_CAPABILITY
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
    MssqlGenericCommitOutcomeUnknown,
    MssqlGenericTransactionFinalizer,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_operation_lock,
    acquire_target_lock,
    acquire_target_then_operation_locks,
    target_lock_resource,
)
from dpone.runtime.sinks.strategies.mssql.mssql_unique_authority import (
    resolve_unique_authority_contract,
)
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION,
)
from dpone.runtime.state.mssql_fresh_session import MssqlFreshSessionFactory
from dpone.runtime.state.mssql_generic_operation_sql import claim_operation_sql
from dpone.runtime.state.mssql_generic_operation_state import MssqlGenericOperationState
from dpone.runtime.state.mssql_generic_operation_trigger import render_operation_trigger_body
from dpone.runtime.state.mssql_generic_transaction import (
    MssqlGenericTransactionState,
    MssqlOperationClaimOutcomeUnknown,
    MssqlOperationClaimRejected,
)
from dpone.runtime.state.mssql_generic_transaction_ddl import (
    GENERIC_TRANSACTION_CATALOG_VERSION,
    render_generic_transaction_catalog_ddl,
)
from dpone.runtime.state.mssql_generic_transaction_errors import OPERATION_CLAIM_REJECTION_CODES
from dpone.runtime.state.mssql_generic_transaction_rows import attempt_from_rows, operation_from_rows
from dpone.runtime.state.mssql_generic_transaction_sql import allocation_sql

_SOURCE_AUTHORITY_SHA256 = "sha256:" + "a" * 64


def test_invocation_attempt_is_retry_stable_but_load_id_is_not_identity() -> None:
    first = _attempt_request(load_id="load-A")
    second = _attempt_request(load_id="load-B")

    assert first.attempt_key == second.attempt_key
    assert first.load_id != second.load_id
    assert _operation_request().operation_key(first) == _operation_request().operation_key(second)


def test_distinct_scheduler_invocation_allows_distinct_operation() -> None:
    first = _attempt_request(load_id="load-A", run_id="scheduled-1")
    second = _attempt_request(load_id="load-A", run_id="scheduled-2")

    assert first.attempt_key != second.attempt_key
    assert _operation_request().operation_key(first) != _operation_request().operation_key(second)


def test_runtime_proven_backfill_invocation_is_stable_across_scheduler_reruns() -> None:
    first_chunk = _backfill_config(chunk_index=1, owner="worker-one")
    second_chunk = _backfill_config(chunk_index=2, owner="worker-two")

    first = invocation_identity(
        RunContext("scheduled-run-one", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
        first_chunk,
        dag_id="dag",
    )
    resumed = invocation_identity(
        RunContext("scheduled-run-two", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
        first_chunk,
        dag_id="dag",
    )
    another_chunk = invocation_identity(
        RunContext("scheduled-run-two", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
        second_chunk,
        dag_id="dag",
    )

    assert first == resumed == another_chunk
    assert first.run_id == "dpone-backfill:backfill"


def test_parent_process_lane_rebinds_portable_scope_once_and_rejects_other_chunk() -> None:
    bound_first = _backfill_config(chunk_index=1, owner="worker-one", inner_mode="incremental_merge")
    bound_second = _backfill_config(chunk_index=2, owner="worker-two", inner_mode="incremental_merge")
    bound_first.unique_key = ["id"]
    bound_second.unique_key = ["id"]

    def without_binding(config: LoadConfig) -> LoadConfig:
        options = dict(config.options)
        options.pop(PORTABLE_SCOPE_BINDING_OPTION)
        return replace(config, options=options)

    first = replace(
        without_binding(bound_first),
        source_schema=bound_first.source_schema.upper(),
        source_table=bound_first.source_table.upper(),
    )
    second = replace(
        without_binding(bound_second),
        source_schema=bound_second.source_schema.upper(),
        source_table=bound_second.source_table.upper(),
    )
    source = _certified_source()
    original_fetch = source.fetch_schema_projection
    original_identity = source.mssql_transaction_source_physical_identity
    catalog_reads = 0
    identity_reads = 0
    target_reads = 0

    def observed_fetch(config: LoadConfig) -> object:
        nonlocal catalog_reads
        assert config.source_schema == bound_first.source_schema
        assert config.source_table == bound_first.source_table
        catalog_reads += 1
        return original_fetch(config)

    def observed_identity(config: LoadConfig) -> object:
        nonlocal identity_reads
        identity_reads += 1
        config.source_schema = bound_first.source_schema
        config.source_table = bound_first.source_table
        return original_identity(config)

    def resolve_target(*_args: object, **_kwargs: object) -> object:
        nonlocal target_reads
        target_reads += 1
        return SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        )

    source.fetch_schema_projection = observed_fetch
    source.mssql_transaction_source_physical_identity = observed_identity
    sink = _admission_sink(SimpleNamespace())
    binder = _ParentPortableScopeBinder(
        source=source,
        sink=sink,
    )
    run_context = RunContext(
        "scheduler-run",
        config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
    )
    projector = _ParentMssqlAttemptProjector(
        source=source,
        sink=sink,
        run_context=run_context,
        dag_id="dag",
        target_resolver=resolve_target,
    )

    rebound_first = binder.bind(projector.bind_source_authority(first))
    rebound_second = binder.bind(projector.bind_source_authority(second))
    assert rebound_first.source_schema == bound_first.source_schema
    assert rebound_first.source_table == bound_first.source_table
    assert rebound_first.options[PORTABLE_SCOPE_BINDING_OPTION] == bound_first.options[PORTABLE_SCOPE_BINDING_OPTION]
    assert rebound_second.options[PORTABLE_SCOPE_BINDING_OPTION] == bound_second.options[PORTABLE_SCOPE_BINDING_OPTION]
    assert catalog_reads == 1

    expected = projector.project(rebound_first)
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []
    child_prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: _AdmissionState(captured),
    ).prepare(
        bound_first,
        source=_certified_source(),
        sink=_admission_sink(
            SimpleNamespace(
                atomicity="target_atomic",
                provisioning="external",
                require_database_authority_binding=lambda: None,
            )
        ),
        run_context=run_context,
        load_record=SimpleNamespace(load_id="load-one"),
        dag_id="dag",
    )
    attempt_request = captured[0][0]
    child_admission = child_prepared.options[ADMISSION_OPTION]
    assert child_admission.operation is not None
    operation = child_admission.operation
    assert (
        attempt_request.invocation,
        attempt_request.target_identity,
        attempt_request.route_fingerprint,
        attempt_request.target_database,
        attempt_request.target_schema,
        attempt_request.target_table,
        attempt_request.strategy,
    ) == (
        expected.invocation,
        expected.target_identity,
        expected.route_fingerprint,
        expected.target_database,
        expected.target_schema,
        expected.target_table,
        expected.strategy,
    )

    def operation_for(request: MssqlAttemptRequest) -> MssqlTransactionOperation:
        attempt = MssqlTransactionAttempt(request, generation=1)
        operation = operation_request(bound_first, request.invocation)
        return MssqlTransactionOperation(
            attempt=attempt,
            operation_key=operation.operation_key(attempt),
            scope_hash=operation.scope_hash,
            owner_digest=operation.owner_digest,
            epoch=1,
            lease_expires_at_utc=operation.lease_expires_at_utc,
        )

    first_dispatch = ProcessLaneDispatch(
        command_id="backfill:1:worker-one",
        load_config=rebound_first,
        binding=ProcessLaneBinding("backfill", 1, "worker-one"),
        parent_context=None,
    )
    second_dispatch = ProcessLaneDispatch(
        command_id="backfill:2:worker-two",
        load_config=rebound_second,
        binding=ProcessLaneBinding("backfill", 2, "worker-two"),
        parent_context=None,
    )

    def validate(dispatch: ProcessLaneDispatch, candidate: MssqlTransactionOperation) -> bool:
        return _validate_operation_binding(
            dispatch,
            candidate,
            bind_scope=binder.bind,
            project_attempt=projector.project,
        )

    assert validate(first_dispatch, operation)
    assert not validate(second_dispatch, operation)
    forged_requests = (
        replace(
            attempt_request,
            invocation=InvocationIdentity(
                expected.invocation.run_id,
                "foreign-process",
                expected.invocation.task_partition,
            ),
        ),
        replace(attempt_request, target_identity=b"x" * 32),
        replace(attempt_request, route_fingerprint=b"x" * 32),
        replace(attempt_request, target_table="other_target"),
    )
    assert all(not validate(first_dispatch, operation_for(request)) for request in forged_requests)
    with pytest.raises(MssqlTransactionContractError, match="operation_attempt_invalid"):
        MssqlTransactionOperation(
            attempt=object(),  # type: ignore[arg-type] - prove constructor fencing.
            operation_key=b"o" * 32,
            scope_hash=b"s" * 32,
            owner_digest=b"w" * 32,
            epoch=1,
        )
    malformed = operation_for(attempt_request)
    object.__setattr__(malformed, "attempt", object())
    assert not validate(first_dispatch, malformed)
    assert catalog_reads == 1
    assert identity_reads == 1
    assert target_reads == 1


def test_parent_process_lane_preserves_clickhouse_relation_without_relation_identity() -> None:
    config = _backfill_config(chunk_index=1, owner="worker-one")
    config.source_schema = "marketing"
    config.source_table = "events"
    config.options["source_type"] = "clickhouse"
    config.options.pop("postgres_source_authority_sha256")
    identity = SourcePhysicalIdentity(
        dialect="clickhouse",
        cluster_identifier="clickhouse-cluster",
        database="marketing",
        effective_principal="reader",
        session_principal="reader",
        server_address="clickhouse",
        server_port=9000,
        topology_role="standalone",
        schema="unsigned-diagnostic-schema",
        relation="unsigned-diagnostic-relation",
    )
    authority = _ParentMssqlLaneSourceAuthority(
        SimpleNamespace(mssql_transaction_source_physical_identity=lambda _config: identity)
    )

    canonical, resolved = authority.bind(config)

    assert resolved is identity
    assert canonical.source_schema == "marketing"
    assert canonical.source_table == "events"


def test_v1_compatible_attempt_key_keeps_physical_target_identity() -> None:
    first = _attempt_request(load_id="load-A")
    rotated = MssqlAttemptRequest(
        invocation=first.invocation,
        target_identity=b"n" * 32,
        route_fingerprint=b"f" * 32,
        load_id="load-B",
        target_database=first.target_database,
        target_schema=first.target_schema,
        target_table=first.target_table,
        strategy=first.strategy,
    )

    assert rotated.attempt_key != first.attempt_key


def test_distinct_task_partition_is_the_multi_target_attempt_discriminator() -> None:
    first = _attempt_request(load_id="load-A")
    second = MssqlAttemptRequest(
        invocation=InvocationIdentity(
            first.invocation.run_id,
            first.invocation.process,
            "pipeline:another-target-task",
        ),
        target_identity=first.target_identity,
        route_fingerprint=first.route_fingerprint,
        load_id="load-A",
        target_database=first.target_database,
        target_schema=first.target_schema,
        target_table="another_target",
        strategy=first.strategy,
    )

    assert second.attempt_key != first.attempt_key


def test_same_invocation_rotated_target_row_is_an_identity_collision() -> None:
    original = _attempt_request(load_id="load-A")
    rotated = MssqlAttemptRequest(
        invocation=original.invocation,
        target_identity=b"n" * 32,
        route_fingerprint=b"f" * 32,
        load_id="load-B",
        target_database=original.target_database,
        target_schema=original.target_schema,
        target_table=original.target_table,
        strategy=original.strategy,
    )
    persisted = {
        "attempt_key": original.attempt_key,
        "target_identity": original.target_identity,
        "invocation_digest": original.invocation.invocation_digest,
        "route_fingerprint": original.route_fingerprint,
        "target_database": original.target_database,
        "target_schema": original.target_schema,
        "target_table": original.target_table,
        "strategy": original.strategy,
        "generation": 1,
        "is_current_generation": True,
    }

    assert rotated.attempt_key != original.attempt_key
    with pytest.raises(RuntimeError, match="mssql_transaction.attempt_identity_collision"):
        attempt_from_rows(rotated, [persisted])


def test_operation_owner_reacquire_keeps_scope_and_changes_owner() -> None:
    scope = operation_scope_hash({"kind": "backfill", "chunk": 7})
    first = MssqlOperationRequest(scope, operation_owner_digest("owner-a"))
    second = MssqlOperationRequest(scope, operation_owner_digest("owner-b"))
    attempt = MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=4)

    assert first.operation_key(attempt) == second.operation_key(attempt)
    assert first.owner_digest != second.owner_digest


def test_operation_lease_requires_timezone() -> None:
    with pytest.raises(MssqlTransactionContractError, match="lease_expiry_timezone_required"):
        MssqlOperationRequest(
            operation_scope_hash({"kind": "target_wide"}),
            operation_owner_digest("owner"),
            datetime(2026, 1, 1),
        )


@pytest.mark.parametrize(
    "naive_sql_value",
    [True, False],
)
def test_sqlserver_operation_lease_readback_is_normalized_to_aware_utc(naive_sql_value: bool) -> None:
    sql_value = datetime.now(UTC) + timedelta(minutes=5)
    if naive_sql_value:
        sql_value = sql_value.replace(tzinfo=None)
    request = MssqlOperationRequest(
        operation_scope_hash({"kind": "chunk"}),
        operation_owner_digest("owner"),
        datetime.now(UTC) + timedelta(minutes=5),
    )
    attempt = MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=1)
    row = _operation_row(attempt, request)
    row["lease_expires_at_utc"] = sql_value

    operation = operation_from_rows(attempt, request, [row])

    assert operation.lease_expires_at_utc is not None
    assert operation.lease_expires_at_utc.tzinfo is UTC
    heartbeat = MssqlOperationLeaseHeartbeat(
        SimpleNamespace(renew_operation_lease=lambda *_args, **_kwargs: True),
        MssqlTransactionAdmission(operation=operation),
    )
    heartbeat.start()
    heartbeat.start()
    heartbeat.stop()
    heartbeat.stop()
    heartbeat.assert_healthy()


def test_sqlserver_operation_lease_readback_rejects_non_datetime() -> None:
    request = MssqlOperationRequest(
        operation_scope_hash({"kind": "chunk"}),
        operation_owner_digest("owner"),
        datetime.now(UTC) + timedelta(minutes=5),
    )
    attempt = MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=1)
    row = _operation_row(attempt, request)
    row["lease_expires_at_utc"] = "2026-08-16T17:00:00"

    with pytest.raises(RuntimeError, match="operation_lease_expires_at_utc_invalid"):
        operation_from_rows(attempt, request, [row])


@pytest.mark.parametrize("result", [0, 1])
def test_operation_applock_accepts_nonnegative_sql_scalar(result: int) -> None:
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda *_args, **_kwargs: [{"lock_result": result}],
    )

    acquire_operation_lock(
        connector,
        database="DWH",
        operation_key=b"o" * 32,
        timeout_ms=1_000,
    )


@pytest.mark.parametrize("result", [-3, -2, -1])
def test_operation_applock_rejects_negative_sql_scalar(result: int) -> None:
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda *_args, **_kwargs: [{"lock_result": result}],
    )

    with pytest.raises(RuntimeError, match="operation_lock_unavailable"):
        acquire_operation_lock(
            connector,
            database="DWH",
            operation_key=b"o" * 32,
            timeout_ms=1_000,
        )


@pytest.mark.parametrize(
    "rows",
    [[], [{"lock_result": None}], [{"lock_result": "0"}], [{"lock_result": False}], [{}]],
)
def test_operation_applock_rejects_missing_or_malformed_scalar(rows: list[dict[str, object]]) -> None:
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda *_args, **_kwargs: rows,
    )

    with pytest.raises(RuntimeError, match="operation_lock_result_invalid"):
        acquire_operation_lock(
            connector,
            database="DWH",
            operation_key=b"o" * 32,
            timeout_ms=1_000,
        )


def test_target_lock_resource_is_bound_to_exact_immutable_target_identity() -> None:
    assert target_lock_resource(b"t" * 32) == "dpone:target:" + (b"t" * 32).hex()
    assert target_lock_resource(b"u" * 32) != target_lock_resource(b"t" * 32)

    for invalid in (b"", b"t" * 31, bytearray(b"t" * 32), "t" * 32, True):
        with pytest.raises(ValueError, match="target_lock_identity_invalid"):
            target_lock_resource(invalid)  # type: ignore[arg-type]


def test_target_applock_uses_transaction_owner_and_exact_database_namespace() -> None:
    calls: list[tuple[str, tuple[object, ...], bool]] = []

    def get_records(sql: str, params: tuple[object, ...], *, as_dict: bool):
        calls.append((sql, params, as_dict))
        return [{"lock_result": 0}]

    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=get_records,
    )
    resource = target_lock_resource(b"t" * 32)

    acquire_target_lock(connector, resource, database="DWH", timeout_ms=1_234)

    assert len(calls) == 1
    sql, params, as_dict = calls[0]
    assert "[DWH].sys.sp_getapplock" in sql
    assert "@LockOwner = 'Transaction'" in sql
    assert params == (resource, 1_234)
    assert as_dict is True


def test_generic_finalizer_lock_contract_is_strictly_target_then_operation() -> None:
    events: list[tuple[str, object]] = []
    connector = object()
    target_identity = b"t" * 32
    operation_key = b"o" * 32

    def target_acquirer(_connector: object, resource: str, **_kwargs: object) -> None:
        events.append(("target", resource))

    def operation_acquirer(_connector: object, **kwargs: object) -> None:
        events.append(("operation", kwargs["operation_key"]))

    acquire_target_then_operation_locks(
        connector,
        database="DWH",
        target_identity=target_identity,
        operation_key=operation_key,
        timeout_ms=1_000,
        target_lock_acquirer=target_acquirer,
        operation_lock_acquirer=operation_acquirer,
    )

    assert events == [
        ("target", target_lock_resource(target_identity)),
        ("operation", operation_key),
    ]


def test_route_fingerprint_excludes_chunk_scope_but_operation_key_does_not() -> None:
    first = _backfill_config(chunk_index=1, owner="one")
    second = _backfill_config(chunk_index=2, owner="two")
    source_identity = _source_physical_identity()
    identity = b"t" * 32
    invocation = InvocationIdentity("run", "process", "pipeline:task")

    assert invocation_route_fingerprint(
        first,
        target_identity=identity,
        source_identity=source_identity,
    ) == invocation_route_fingerprint(
        second,
        target_identity=identity,
        source_identity=source_identity,
    )
    assert operation_request(first, invocation).operation_key(_attempt_request(load_id="one")) != operation_request(
        second,
        invocation,
    ).operation_key(_attempt_request(load_id="one"))


def test_route_fingerprint_distinguishes_same_endpoint_config_on_another_cluster() -> None:
    config = _config()
    identity = b"t" * 32

    first = invocation_route_fingerprint(
        config,
        target_identity=identity,
        source_identity=_source_physical_identity(cluster="pg-cluster-A"),
    )
    second = invocation_route_fingerprint(
        config,
        target_identity=identity,
        source_identity=_source_physical_identity(cluster="pg-cluster-B"),
    )

    assert first != second


def test_postgres_source_identity_uses_only_the_bound_signed_authority() -> None:
    queries: list[str] = []

    class Connector:
        def get_records(self, query: str, *, as_dict: bool):
            queries.append(query)
            raise AssertionError("pre-admission source catalog I/O is forbidden")

    expected = _source_physical_identity(cluster="761991928213")
    source = SimpleNamespace(
        connector=Connector(),
        _postgres_source_authority_verifier=SimpleNamespace(
            preflight=lambda _config: expected,
        ),
    )
    resolved = PostgresSource.mssql_transaction_source_physical_identity(source, _config())

    assert resolved == expected
    assert queries == []


def test_postgres_source_identity_without_bound_verifier_fails_before_source_io() -> None:
    calls = {"catalog": 0, "extract": 0}

    class Connector:
        def get_records(self, _query: str, *, as_dict: bool):
            calls["catalog"] += 1
            raise AssertionError("unbound authority must reject without source I/O")

    source = SimpleNamespace(
        connector=Connector(),
        _postgres_source_authority_verifier=None,
    )

    with pytest.raises(
        RuntimeError,
        match="mssql_transaction.postgres_source_authority_verifier_required",
    ):
        PostgresSource.mssql_transaction_source_physical_identity(source, _config())

    assert calls == {"catalog": 0, "extract": 0}


def test_postgres_route_without_physical_identity_capability_fails_closed() -> None:
    with pytest.raises(MssqlTransactionContractError, match="source_physical_identity_capability_required"):
        resolve_source_physical_identity(SimpleNamespace(connector=object()), _config())


def test_postgres_route_rejects_legacy_unsigned_physical_identity() -> None:
    legacy = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="legacy",
        database="source",
        effective_principal="reader",
        session_principal="reader",
    )
    source = SimpleNamespace(
        mssql_transaction_source_physical_identity=lambda _config: legacy,
    )

    with pytest.raises(
        MssqlTransactionContractError,
        match="postgres_source_authority_identity_required",
    ):
        resolve_source_physical_identity(source, _config())


def test_postgres_route_accepts_catalog_authority_identity() -> None:
    catalog_identity = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier=None,
        database="source",
        effective_principal="reader",
        session_principal="reader",
        server_address="192.0.2.1",
        server_port=5432,
        topology_role="standby",
        version=3,
        authority_sha256=_SOURCE_AUTHORITY_SHA256,
        timeline_id=None,
        database_oid=16384,
        effective_principal_oid=16385,
        session_principal_oid=16385,
        schema="public",
        schema_oid=2200,
        relation="source",
        relation_oid=16390,
        verification_profile="catalog_identity",
    )
    source = SimpleNamespace(
        mssql_transaction_source_physical_identity=lambda _config: catalog_identity,
    )

    assert resolve_source_physical_identity(source, _config()) == catalog_identity


def test_postgres_route_rejects_runtime_authority_digest_mismatch() -> None:
    config = _config()
    config.options["postgres_source_authority_sha256"] = "sha256:" + "b" * 64
    source = SimpleNamespace(
        mssql_transaction_source_physical_identity=lambda _config: _source_physical_identity(),
    )

    with pytest.raises(
        MssqlTransactionContractError,
        match="postgres_source_authority_digest_mismatch",
    ):
        resolve_source_physical_identity(source, config)


def test_unknown_or_overlapping_backfill_scope_fails_closed() -> None:
    config = _config()
    config.options["__dpone_mssql_operation_scope"] = {
        "kind": "backfill_disjoint_range_v1",
        "proven_disjoint": False,
    }
    with pytest.raises(MssqlTransactionContractError, match="operation_scope_not_proven_disjoint"):
        operation_request(config, InvocationIdentity("run", "process", "task"))


def test_external_ddl_has_four_exact_objects_and_owner_epoch_receipt_binding() -> None:
    ddl = render_generic_transaction_catalog_ddl(database="Example_System", schema="dbo")

    assert GENERIC_TRANSACTION_CATALOG_VERSION == 2
    assert ddl.startswith("-- dpone generic MSSQL transaction catalog v2\n")
    for table in ("dpone_target_fence", "dpone_load_attempt", "dpone_load_operation", "dpone_load_receipt"):
        assert f"CREATE TABLE [Example_System].[dbo].[{table}]" in ddl
    assert "UNIQUE NONCLUSTERED (target_identity, generation)" in ddl
    assert "UNIQUE NONCLUSTERED (invocation_digest)" in ddl
    assert "UNIQUE NONCLUSTERED (attempt_key, scope_hash)" in ddl
    assert "FOREIGN KEY (current_attempt_key, target_identity, current_generation, current_route_fingerprint)" in ddl
    assert "operation_key, attempt_key, scope_hash, current_epoch, current_owner_digest" in ddl
    assert "operation_key, attempt_key, scope_hash, operation_epoch, owner_digest" in ddl
    assert "trg_dpone_target_fence_monotonic" in ddl
    assert "DPONE_TARGET_FENCE_GENERATION_INVALID" in ddl
    assert "operation_epoch bigint NOT NULL" in ddl
    assert "owner_digest binary(32) NOT NULL" in ddl
    assert "payload_manifest_sha256 binary(32) NOT NULL" in ddl
    assert "native_contract_sha256 binary(32) NOT NULL" in ddl
    assert "target_after_sha256 binary(32) NOT NULL" in ddl
    assert "extraction_started_at_utc datetime2(7) NOT NULL" in ddl
    assert "extraction_clock_authority nvarchar(128) NOT NULL" in ddl
    assert "snapshot_acquired_at_utc datetime2(7) NULL" in ddl
    assert "snapshot_authority nvarchar(128) NULL" in ddl
    assert "extraction_started_at_utc <= extraction_completed_at_utc" in ddl
    assert "declared_rows = actual_raw_rows" in ddl
    assert "actual_raw_rows = actual_native_rows" in ddl
    assert "DPONE_LOAD_OPERATION_EPOCH_INVALID" in ddl
    assert ddl.count("CREATE TABLE ") == 4
    assert ddl.count("CREATE TRIGGER ") == 4
    assert ddl.count("DATA_COMPRESSION = NONE") == 4
    assert "IDENTITY(" not in ddl.upper()
    assert "MERGE" not in ddl.upper()


def test_allocation_and_operation_claim_are_serializable_and_monotonic() -> None:
    allocation = allocation_sql(fence="[state].[fence]", attempt="[state].[attempt]")
    claim = claim_operation_sql(
        "[state].[operation]",
        "[state].[receipt]",
        "[state].[fence]",
        "[state].[attempt]",
    )

    assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in allocation
    assert "WHERE invocation_digest = ?" in allocation
    assert allocation.index("WHERE invocation_digest = ?") < allocation.index("WHERE attempt_key = ?")
    assert "@generation = current_generation + 1" in allocation
    assert "SET current_generation = @generation" in allocation
    assert "operation_key" not in allocation.split("INSERT INTO [state].[attempt]", 1)[1].split(";", 1)[0]
    assert "current_epoch + 1" in claim
    assert "DPONE_LOAD_OPERATION_LEASE_EXPIRED" in claim
    assert "DPONE_LOAD_OPERATION_LEASE_EXPIRED_OWNER_REACQUIRE_REQUIRED" in claim
    assert "DPONE_LOAD_OPERATION_ALREADY_OWNED" in claim
    assert "@persisted_expiry IS NULL OR @persisted_expiry > SYSUTCDATETIME()" in claim
    assert "@committed_receipt_id IS NULL" in claim
    assert "FROM [state].[receipt] WITH (HOLDLOCK)" in claim
    assert "DPONE_LOAD_OPERATION_STALE_ATTEMPT_GENERATION" in claim
    assert "FROM [state].[fence] AS f WITH (UPDLOCK, HOLDLOCK)" in claim


def test_receipt_probe_uses_read_committed_under_operation_applock() -> None:
    session = _StateSession("receipt-probe", [])
    state = MssqlGenericOperationState(session, database="Example_System", schema="dbo")

    assert state.receipt_by_key(b"o" * 32) is None

    assert len(session.sqls) == 1
    assert "dpone_load_receipt] AS r WITH (READCOMMITTED)" in session.sqls[0]


def test_operation_claim_rejection_allowlist_matches_every_authored_throw_token() -> None:
    authored = (
        claim_operation_sql(
            "[state].[operation]",
            "[state].[receipt]",
            "[state].[fence]",
            "[state].[attempt]",
        )
        + render_operation_trigger_body()
    )

    assert set(OPERATION_CLAIM_REJECTION_CODES) == set(re.findall(r"DPONE_LOAD_OPERATION_[A-Z_]+", authored))


@pytest.mark.parametrize(
    ("vendor_token", "code"),
    tuple(OPERATION_CLAIM_REJECTION_CODES.items()),
)
def test_operation_claim_rolls_back_and_normalizes_exact_catalog_rejections(
    vendor_token: str,
    code: str,
) -> None:
    session = _StateSession(
        "operation-claim",
        [],
        records_error=RuntimeError(f"[Microsoft][ODBC] {vendor_token} (51000)"),
    )
    state = MssqlGenericTransactionState(session, database="Example_System", schema="dbo")

    with pytest.raises(MssqlOperationClaimRejected) as captured:
        state.operations.claim_or_replay(
            MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=1),
            _operation_request(),
        )

    assert captured.value.code == code
    assert str(captured.value) == code
    assert captured.value.vendor_token == vendor_token
    assert session.rollbacks == 1


@pytest.mark.parametrize(
    "message",
    (
        "08S01 transport failure",
        "XDPONE_LOAD_OPERATION_LEASE_EXPIRED",
        "DPONE_LOAD_OPERATION_LEASE_EXPIRED_UNKNOWN",
        "DPONE_LOAD_OPERATION_LEASE_EXPIRED DPONE_LOAD_OPERATION_ALREADY_OWNED",
        "DPONE_LOAD_OPERATION_LEASE_EXPIRED DPONE_LOAD_OPERATION_LEASE_EXPIRED",
    ),
)
def test_operation_claim_preserves_non_exact_driver_error_after_rollback(message: str) -> None:
    error = RuntimeError(message)
    session = _StateSession("operation-claim", [], records_error=error)
    state = MssqlGenericTransactionState(session, database="Example_System", schema="dbo")

    with pytest.raises(RuntimeError) as captured:
        state.operations.claim_or_replay(
            MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=1),
            _operation_request(),
        )

    assert captured.value is error
    assert session.rollbacks == 1


def test_operation_claim_never_normalizes_commit_ack_ambiguity() -> None:
    request = _operation_request()
    attempt = MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=1)
    claim = _StateSession(
        "operation-claim",
        [_operation_row(attempt, request)],
        commit_error=RuntimeError("DPONE_LOAD_OPERATION_LEASE_EXPIRED after commit ACK loss"),
    )
    probe = _StateSession("fresh-probe", [])
    sessions = _FreshSessionSequence(probe)
    state = MssqlGenericTransactionState(
        claim,
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(sessions),
    )

    with pytest.raises(MssqlOperationClaimOutcomeUnknown):
        state.operations.claim_or_replay(attempt, request)

    assert claim.commits == 1
    assert claim.rollbacks == 0
    assert claim.closed
    assert probe.closed


def test_allocation_ack_loss_uses_fresh_probe_then_distinct_claim_session() -> None:
    request = _attempt_request(load_id="load-A")
    operation_request = _operation_request()
    attempt = MssqlTransactionAttempt(request, generation=1)
    allocate = _StateSession(
        "allocate",
        [_attempt_row(request, generation=1, current=True)],
        commit_error=OSError("allocation ack lost"),
    )
    probe = _StateSession("allocation-probe", [_attempt_row(request, generation=1, current=True)])
    claim = _StateSession("operation-claim", [_operation_row(attempt, operation_request)])
    sessions = _FreshSessionSequence(allocate, probe, claim)
    state = MssqlGenericTransactionState(
        _StateSession("template", []),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(sessions),
    )

    admission = state.admit(request, operation_request)

    assert admission.operation is not None
    assert admission.operation.attempt.generation == 1
    assert sessions.templates == ["template", "allocate", "template"]
    assert allocate.closed and probe.closed and claim.closed
    assert claim.commits == 1


def test_allocation_ack_loss_probe_observes_newer_fence_before_source() -> None:
    request = _attempt_request(load_id="load-A")
    allocate = _StateSession(
        "allocate",
        [_attempt_row(request, generation=1, current=True)],
        commit_error=OSError("allocation ack lost"),
    )
    probe_after_b = _StateSession(
        "allocation-probe-after-B",
        [_attempt_row(request, generation=1, current=False)],
    )
    receipt_probe = _StateSession("receipt-probe", [])
    sessions = _FreshSessionSequence(allocate, probe_after_b, receipt_probe)
    state = MssqlGenericTransactionState(
        _StateSession("template", []),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(sessions),
    )

    with pytest.raises(RuntimeError, match="stale_attempt_generation_pre_source"):
        state.admit(request, _operation_request())

    assert "LEFT JOIN [Example_System].[dbo].[dpone_target_fence]" in probe_after_b.sqls[0]
    assert sessions.templates == ["template", "allocate", "template"]


def test_stale_existing_attempt_blocks_before_operation_claim_without_receipt() -> None:
    request = _attempt_request(load_id="load-A")
    allocate = _StateSession("stale-allocation", [_attempt_row(request, generation=1, current=False)])
    receipt_probe = _StateSession("receipt-probe", [])
    sessions = _FreshSessionSequence(allocate, receipt_probe)
    state = MssqlGenericTransactionState(
        _StateSession("template", []),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(sessions),
    )

    with pytest.raises(RuntimeError, match="stale_attempt_generation_pre_source"):
        state.admit(request, _operation_request())

    assert sessions.templates == ["template", "template"]


def test_stale_existing_attempt_allows_only_exact_committed_replay() -> None:
    request = _attempt_request(load_id="load-A")
    operation_request = _operation_request()
    attempt = MssqlTransactionAttempt(request, generation=1, is_current_generation=False)
    receipt = _receipt(
        MssqlTransactionOperation(
            attempt,
            operation_request.operation_key(attempt),
            operation_request.scope_hash,
            operation_request.owner_digest,
            1,
        )
    )
    sessions = _FreshSessionSequence(
        _StateSession("stale-allocation", [_attempt_row(request, generation=1, current=False)]),
        _StateSession("receipt-probe", [_receipt_row(receipt)]),
    )
    state = MssqlGenericTransactionState(
        _StateSession("template", []),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(sessions),
    )

    admission = state.admit(request, operation_request)

    assert admission.replay_receipt == receipt


def test_receipt_probe_replays_without_attempt_allocation_or_operation_claim() -> None:
    request = _attempt_request(load_id="load-A")
    operation_request = _operation_request()
    attempt = MssqlTransactionAttempt(request, generation=1)
    receipt = _receipt(
        MssqlTransactionOperation(
            attempt,
            operation_request.operation_key(attempt),
            operation_request.scope_hash,
            operation_request.owner_digest,
            1,
        )
    )
    receipt_probe = _StateSession("receipt-probe", [_receipt_row(receipt)])
    sessions = _FreshSessionSequence(receipt_probe)
    state = MssqlGenericTransactionState(
        _StateSession("template", []),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(sessions),
    )

    admission = state.replay_if_committed(request, operation_request)

    assert admission is not None
    assert admission.replay_receipt == receipt
    assert sessions.templates == ["template"]
    assert len(receipt_probe.sqls) == 1
    assert "dpone_load_receipt" in receipt_probe.sqls[0]
    assert "WHERE r.operation_key = ?" in receipt_probe.sqls[0]


def test_admission_service_uses_run_context_not_new_load_id() -> None:
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []
    state = _AdmissionState(captured)
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: state,
    )
    sink = type("MSSQLSink", (), {})()
    sink.connector = object()
    sink.state_storage = SimpleNamespace(atomicity="target_atomic", provisioning="external")
    sink.load = lambda *_args, **_kwargs: None
    sink.target_dialect = lambda: "mssql"
    sink.mssql_transaction_governance_capability = lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY
    sink.get_target_catalog_snapshot = _matching_target_snapshot
    source = _certified_source()
    context = RunContext("airflow-run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"})

    first = service.prepare(
        _config(),
        source=source,
        sink=sink,
        run_context=context,
        load_record=SimpleNamespace(load_id="load-A"),
        dag_id="dag",
    )
    second = service.prepare(
        _config(),
        source=source,
        sink=sink,
        run_context=context,
        load_record=SimpleNamespace(load_id="load-B"),
        dag_id="dag",
    )

    assert captured[0][0].attempt_key == captured[1][0].attempt_key
    assert isinstance(first.options[ADMISSION_OPTION], MssqlTransactionAdmission)
    assert isinstance(second.options[ADMISSION_OPTION], MssqlTransactionAdmission)


def test_admission_uses_injected_operation_lease_controller_for_finite_backfill_scope() -> None:
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []
    state = _AdmissionState(captured)
    controller = object()
    factory_calls: list[tuple[object, MssqlTransactionAdmission]] = []

    def operation_lease_factory(
        admitted_state: object,
        admission: MssqlTransactionAdmission,
    ) -> object:
        factory_calls.append((admitted_state, admission))
        return controller

    config = _backfill_config(chunk_index=1, owner="worker-a", inner_mode="incremental_merge")
    config.unique_key = ["id"]
    prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: state,
        operation_lease_factory=operation_lease_factory,
    ).prepare(
        config,
        source=_certified_source(),
        sink=_admission_sink(
            SimpleNamespace(
                atomicity="target_atomic",
                provisioning="external",
                require_database_authority_binding=lambda: None,
            )
        ),
        run_context=RunContext(
            "run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert prepared.options[LEASE_OPTION] is controller
    assert len(factory_calls) == 1
    assert factory_calls[0][0] is state
    assert factory_calls[0][1] is prepared.options[ADMISSION_OPTION]


def test_process_admission_and_processor_handoff_start_one_controller_lifecycle() -> None:
    events: list[str] = []

    class Controller:
        started = False

        def start(self) -> None:
            if self.started:
                return
            self.started = True
            events.append("start")

        def assert_healthy(self) -> None:
            events.append("healthy")

        def stop(self) -> None:
            events.append("stop")

    controller = Controller()
    config = _backfill_config(chunk_index=1, owner="worker-a", inner_mode="incremental_merge")
    config.unique_key = ["id"]
    prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: _AdmissionState([]),
        operation_lease_factory=lambda _state, _admission: controller,
        start_operation_lease_on_admission=True,
    ).prepare(
        config,
        source=_certified_source(),
        sink=_admission_sink(
            SimpleNamespace(
                atomicity="target_atomic",
                provisioning="external",
                require_database_authority_binding=lambda: None,
            )
        ),
        run_context=RunContext(
            "run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    lease = ProcessorRuntimeServices.start_transaction_lease(prepared)
    assert lease is controller
    lease.stop()

    assert events == ["start", "stop"]


def test_process_admission_starts_lease_before_preplan_and_stops_it_on_failure(monkeypatch) -> None:
    events: list[str] = []
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []

    class Controller:
        def start(self) -> None:
            events.append("start")

        def assert_healthy(self) -> None:
            events.append("healthy")

        def stop(self) -> None:
            events.append("stop")

    class State(_AdmissionState):
        def admit(
            self,
            attempt: MssqlAttemptRequest,
            operation: MssqlOperationRequest,
        ) -> MssqlTransactionAdmission:
            events.append("admit")
            return super().admit(attempt, operation)

    def refresh_scope(config: LoadConfig) -> LoadConfig:
        events.append("refresh")
        return config

    def fail_preplan(*_args: object, **_kwargs: object) -> object:
        events.append("preplan")
        raise RuntimeError("preplan failed")

    monkeypatch.setattr(MssqlSchemaPreplanner, "plan", fail_preplan)
    config = _backfill_config(chunk_index=1, owner="worker-a", inner_mode="incremental_merge")
    config.unique_key = ["id"]
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: State(captured),
        operation_lease_factory=lambda _state, _admission: events.append("factory") or Controller(),
        operation_scope_refresher=refresh_scope,
        start_operation_lease_on_admission=True,
    )

    with pytest.raises(RuntimeError, match="preplan failed"):
        service.prepare(
            config,
            source=_certified_source(),
            sink=_admission_sink(
                SimpleNamespace(
                    atomicity="target_atomic",
                    provisioning="external",
                    require_database_authority_binding=lambda: None,
                )
            ),
            run_context=RunContext(
                "run",
                config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
            ),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert events == ["refresh", "admit", "factory", "start", "preplan", "stop"]


def test_shadow_admission_derives_identity_from_live_binding_but_mutates_shadow() -> None:
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []
    resolved_coordinates: list[tuple[str, str, str]] = []

    def resolve_target(*_args: object, **kwargs: object) -> SimpleNamespace:
        resolved_coordinates.append((str(kwargs["database"]), str(kwargs["schema"]), str(kwargs["table"])))
        return SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        )

    config = _config()
    config.load_strategy = LoadStrategy.BACKFILL
    config.unique_key = ["id"]
    config.options["backfill"] = {
        "inner_mode": "incremental_append",
        "chunk": {"column": "id", "from": 0, "to": 1, "step": 1},
        "state": {
            "backend": "audit_schema",
            "schema": "dbo",
            "require_distributed_lock": True,
        },
        "publication": {"mode": "shadow_swap", "retain_backup": True},
    }
    authority = issue_shadow_append_authority(
        config,
        run_key="campaign-a",
        live_table="target",
        shadow_table="target__dpone_initial_shadow",
    )
    config.target_table = authority.shadow_table
    config.options[SHADOW_APPEND_AUTHORITY_OPTION] = authority

    prepared = MssqlTransactionAdmissionService(
        target_resolver=resolve_target,
        state_factory=lambda _storage: _AdmissionState(captured),
    ).prepare(
        config,
        source=_certified_source(),
        sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
        run_context=RunContext(
            "run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert resolved_coordinates == [("DWH", "dbo", "target")]
    admission = prepared.options[ADMISSION_OPTION]
    assert admission.operation is not None
    request = admission.operation.attempt.request
    assert (request.target_database, request.target_schema, request.target_table) == (
        "DWH",
        "dbo",
        "target__dpone_initial_shadow",
    )
    assert request.target_identity == b"t" * 32


def test_source_rr_boundary_opens_only_after_operation_admission_and_feeds_preplan() -> None:
    events: list[str] = []
    boundary = SimpleNamespace(
        abort_preserving=lambda _primary: events.append("abort"),
    )

    class Source:
        connector = object()

        @staticmethod
        def mssql_transaction_checkpoint_mode(_config: LoadConfig) -> MssqlTransactionCheckpointMode:
            return MssqlTransactionCheckpointMode.STATELESS

        @staticmethod
        def mssql_transaction_source_physical_identity(_config: LoadConfig) -> SourcePhysicalIdentity:
            events.append("identity")
            return _source_physical_identity()

        @staticmethod
        def prepare_mssql_source_boundary(_config: LoadConfig) -> object:
            assert events[-1] == "admit"
            events.append("prepare")
            return boundary

        @staticmethod
        def fetch_schema_projection(config: LoadConfig) -> object:
            assert config.options[MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION] is boundary
            events.append("schema")
            return SimpleNamespace(
                projected_schema=(("id", "bigint"),),
                target_projection=None,
            )

    class State(_AdmissionState):
        def preflight(self, _connector: object) -> None:
            events.append("state_preflight")

        def admit(
            self,
            attempt: MssqlAttemptRequest,
            operation: MssqlOperationRequest,
        ) -> MssqlTransactionAdmission:
            events.append("admit")
            return super().admit(attempt, operation)

    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: (
            events.append("target")
            or SimpleNamespace(
                digest=b"t" * 32,
                database_name="DWH",
                schema_name="dbo",
                table_name="target",
            )
        ),
        state_factory=lambda _storage: State([]),
    )

    prepared = service.prepare(
        _config(),
        source=Source(),
        sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
        run_context=RunContext(
            "run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert prepared.options[MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION] is boundary
    assert events == [
        "target",
        "identity",
        "state_preflight",
        "admit",
        "prepare",
        "schema",
    ]


def test_committed_replay_never_opens_prepared_source_boundary() -> None:
    source_calls: list[str] = []
    source = _certified_source()
    source.prepare_mssql_source_boundary = lambda _config: source_calls.append("prepare")
    source.fetch_schema_projection = lambda _config: source_calls.append("schema")

    class ReplayState:
        @staticmethod
        def preflight(_connector: object) -> None:
            return None

        @staticmethod
        def admit(
            _attempt: MssqlAttemptRequest,
            _operation_request: MssqlOperationRequest,
        ) -> MssqlTransactionAdmission:
            return MssqlTransactionAdmission(replay_receipt=_receipt(_operation()))

    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: ReplayState(),
    )

    prepared = service.prepare(
        _config(),
        source=source,
        sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
        run_context=RunContext(
            "run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert prepared.options[ADMISSION_OPTION].replay_receipt is not None
    assert MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION not in prepared.options
    assert source_calls == []


def test_backfill_receipt_replay_precedes_postgres_catalog_binding() -> None:
    """A resumable chunk must not touch PostgreSQL after its receipt committed."""

    events: list[str] = []
    source = _certified_source()
    source.mssql_transaction_source_physical_identity = lambda _config: pytest.fail(
        "child source identity must not resolve for committed replay"
    )
    source.fetch_schema_projection = lambda _config: (_ for _ in ()).throw(
        AssertionError("PostgreSQL catalog must not be read for committed replay")
    )
    operation = _operation()

    class ReplayState:
        @staticmethod
        def preflight(_connector: object) -> None:
            events.append("state_preflight")

        @staticmethod
        def replay_if_committed(
            _attempt: MssqlAttemptRequest,
            _operation_request: MssqlOperationRequest,
        ) -> MssqlTransactionAdmission:
            events.append("receipt_probe")
            return MssqlTransactionAdmission(replay_receipt=_receipt(operation))

        @staticmethod
        def admit(
            _attempt: MssqlAttemptRequest,
            _operation_request: MssqlOperationRequest,
        ) -> MssqlTransactionAdmission:
            raise AssertionError("committed replay must not allocate or claim state")

    class ParentAuthority:
        @staticmethod
        def authorize_receipt_probe(_load_config, *, load_id):
            events.append("parent_ack")
            attempt = replace(operation.attempt.request, load_id=load_id)
            return attempt, MssqlOperationRequest(
                scope_hash=operation.scope_hash,
                owner_digest=operation.owner_digest,
                lease_expires_at_utc=operation.lease_expires_at_utc,
            )

    sink = _admission_sink(
        SimpleNamespace(
            atomicity="target_atomic",
            provisioning="external",
            require_database_authority_binding=lambda: None,
        )
    )
    sink.get_target_catalog_snapshot = lambda _config: (_ for _ in ()).throw(
        AssertionError("target catalog binding must not run for committed replay")
    )
    prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: pytest.fail(
            "child target identity must not resolve for committed replay"
        ),
        state_factory=lambda _storage: ReplayState(),
        operation_lease_factory=ParentAuthority(),
    ).prepare(
        _backfill_config(chunk_index=1, owner="worker"),
        source=source,
        sink=sink,
        run_context=RunContext(
            "retry-run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="retry-load"),
        dag_id="dag",
    )

    assert prepared.options[ADMISSION_OPTION].replay_receipt is not None
    assert events == ["parent_ack", "state_preflight", "receipt_probe"]


@pytest.mark.parametrize("mismatch", ["target", "source", "route"])
def test_parent_receipt_authority_mismatch_blocks_admit_and_business_catalogs(mismatch: str) -> None:
    """A child endpoint drift cannot continue from a parent-issued replay miss."""

    config = _backfill_config(chunk_index=1, owner="worker")
    source = _certified_source()
    source.fetch_schema_projection = lambda _config: pytest.fail("source business catalog must not run")
    sink = _admission_sink(
        SimpleNamespace(
            atomicity="target_atomic",
            provisioning="external",
            require_database_authority_binding=lambda: None,
        )
    )
    sink.get_target_catalog_snapshot = lambda _config: pytest.fail("target business catalog must not run")
    run_context = RunContext(
        "retry-run",
        config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
    )
    events: list[str] = []

    class State:
        @staticmethod
        def preflight(_connector: object) -> None:
            events.append("state_preflight")

        @staticmethod
        def replay_if_committed(_attempt, _requested_operation):
            events.append("receipt_miss")
            return None

        @staticmethod
        def admit(_attempt, _requested_operation):
            events.append("admit")
            raise AssertionError("mismatched parent authority must block admission")

    class ParentAuthority:
        @staticmethod
        def authorize_receipt_probe(load_config, *, load_id):
            invocation = invocation_identity(run_context, load_config, dag_id="dag")
            target_identity = b"x" * 32 if mismatch == "target" else b"t" * 32
            source_identity = _source_physical_identity(
                cluster="pg-cluster-B" if mismatch == "source" else "pg-cluster-A"
            )
            attempt = build_mssql_attempt_request(
                load_config,
                invocation=invocation,
                target_identity=target_identity,
                source_identity=source_identity,
                load_id=load_id,
                request_coordinates=("DWH", "dbo", "target"),
            )
            if mismatch == "route":
                attempt = replace(attempt, route_fingerprint=b"x" * 32)
            return attempt, operation_request(load_config, invocation)

    with pytest.raises(RuntimeError, match="parent_route_authority_mismatch"):
        MssqlTransactionAdmissionService(
            target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
                digest=b"t" * 32,
                database_name="DWH",
                schema_name="dbo",
                table_name="target",
            ),
            state_factory=lambda _storage: State(),
            operation_lease_factory=ParentAuthority(),
        ).prepare(
            config,
            source=source,
            sink=sink,
            run_context=run_context,
            load_record=SimpleNamespace(load_id="retry-load"),
            dag_id="dag",
        )

    assert events == ["state_preflight", "receipt_miss"]


def test_persisted_binding_receipt_miss_reuses_schema_preplan_catalogs_once() -> None:
    config = _backfill_config(chunk_index=1, owner="worker", inner_mode="incremental_merge")
    config.unique_key = ["id"]
    source = _certified_source()
    source_fetch = source.fetch_schema_projection
    calls = {"source_catalog": 0, "target_catalog": 0}

    def fetch_source(candidate: LoadConfig) -> object:
        calls["source_catalog"] += 1
        return source_fetch(candidate)

    def fetch_target(candidate: LoadConfig) -> MssqlSchemaCatalogSnapshot:
        calls["target_catalog"] += 1
        return _matching_target_snapshot(candidate)

    source.fetch_schema_projection = fetch_source
    sink = _admission_sink(
        SimpleNamespace(
            atomicity="target_atomic",
            provisioning="external",
            require_database_authority_binding=lambda: None,
        )
    )
    sink.get_target_catalog_snapshot = fetch_target
    prepared = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: _AdmissionState([]),
    ).prepare(
        config,
        source=source,
        sink=sink,
        run_context=RunContext(
            "run",
            config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
        ),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert prepared.options[ADMISSION_OPTION].operation is not None
    assert calls == {"source_catalog": 1, "target_catalog": 1}


def test_persisted_binding_receipt_miss_rejects_catalog_drift_before_payload() -> None:
    config = _backfill_config(chunk_index=1, owner="worker", inner_mode="incremental_merge")
    config.unique_key = ["id"]
    source = _certified_source()
    projected = SimpleNamespace(
        source_name="id",
        source_type="integer",
        source_collation=None,
        target_name="id",
        target_type="bigint",
        collation=None,
        nullable=True,
    )
    source.fetch_schema_projection = lambda _config: SimpleNamespace(
        relation_schema=(("id", "integer"),),
        projected_schema=(("id", "bigint"),),
        target_projection=SimpleNamespace(columns=(projected,)),
    )
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []

    with pytest.raises(PortableScopeBindingError, match="catalog_changed"):
        MssqlTransactionAdmissionService(
            target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
                digest=b"t" * 32,
                database_name="DWH",
                schema_name="dbo",
                table_name="target",
            ),
            state_factory=lambda _storage: _AdmissionState(captured),
        ).prepare(
            config,
            source=source,
            sink=_admission_sink(
                SimpleNamespace(
                    atomicity="target_atomic",
                    provisioning="external",
                    require_database_authority_binding=lambda: None,
                )
            ),
            run_context=RunContext(
                "run",
                config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
            ),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert len(captured) == 1


def test_admission_service_fails_without_scheduler_stable_run_id() -> None:
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: _AdmissionState([]),
    )
    sink = type("MSSQLSink", (), {})()
    sink.connector = object()
    sink.state_storage = SimpleNamespace(atomicity="target_atomic", provisioning="external")
    sink.load = lambda *_args, **_kwargs: None
    sink.target_dialect = lambda: "mssql"
    sink.mssql_transaction_governance_capability = lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY
    source = _certified_source()

    with pytest.raises(MssqlTransactionContractError, match="stable_invocation_id_required"):
        service.prepare(
            _config(),
            source=source,
            sink=sink,
            run_context=None,
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )


@pytest.mark.parametrize("retry_policy", ["none", "default", "idempotent"])
def test_governed_mssql_rejects_mutating_post_hook_before_target_or_source(
    retry_policy: str,
) -> None:
    calls = {"target": 0, "source": 0}

    def target_resolver(*_args, **_kwargs):
        calls["target"] += 1
        raise AssertionError("target authority must not be read for a blocked post-hook")

    config = _config()
    config.options["hooks"] = {
        "post_hook": [
            {
                "id": "publish_external_receipt",
                "type": "sql",
                "connector": "sink",
                "sql": "UPDATE dbo.external_receipts SET published = 1",
                "mutates_source": True,
                "retry_policy": retry_policy,
            }
        ]
    }
    source = _certified_source()
    source.extract = lambda *_args: calls.__setitem__("source", calls["source"] + 1)
    service = MssqlTransactionAdmissionService(target_resolver=target_resolver)

    with pytest.raises(RuntimeError, match="hook_exactly_once_capability_required"):
        service.prepare(
            config,
            source=source,
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext("run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert calls == {"target": 0, "source": 0}


def test_governed_mssql_rejects_author_declared_read_only_post_hook() -> None:
    config = _config()
    config.options["hooks"] = {
        "post_hook": [
            {
                "id": "observe_sequence",
                "type": "sql",
                "connector": "sink",
                "sql": "SELECT NEXT VALUE FOR dbo.notification_sequence",
                "mutates_source": False,
            }
        ]
    }
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("blocked hooks must not reach target authority")

    with pytest.raises(RuntimeError, match="hook_exactly_once_capability_required:post_hook:observe_sequence"):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            config,
            source=_certified_source(),
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext("run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert target_calls == 0


def test_governed_mssql_rejects_pre_hook_before_catalog_preplan() -> None:
    config = _config()
    config.options["hooks"] = {
        "pre_hook": [
            {
                "id": "refresh_source",
                "type": "sql",
                "connector": "source",
                "sql": "SELECT 1",
                "mutates_source": False,
            }
        ]
    }
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("blocked pre-hook must not reach catalog preplan")

    with pytest.raises(RuntimeError, match="hook_exactly_once_capability_required:pre_hook:refresh_source"):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            config,
            source=_certified_source(),
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext("run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert target_calls == 0


def test_snapshot_envelope_mssql_rejects_inline_hook_before_route_branch() -> None:
    config = _config()
    config.options.update(
        {
            "reconciliation": {"enabled": True, "mode": "key_snapshot"},
            "hooks": {
                "post_hook": [
                    {
                        "id": "publish_snapshot",
                        "type": "sql",
                        "connector": "sink",
                        "sql": "SELECT 1",
                        "mutates_source": False,
                    }
                ]
            },
        }
    )
    target_calls = 0

    def target_resolver(*_args, **_kwargs):
        nonlocal target_calls
        target_calls += 1
        raise AssertionError("blocked snapshot hook must not reach target authority")

    with pytest.raises(
        RuntimeError,
        match="hook_exactly_once_capability_required:post_hook:publish_snapshot",
    ):
        MssqlTransactionAdmissionService(target_resolver=target_resolver).prepare(
            config,
            source=_certified_source(),
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext(
                "run",
                config={"process": "p", "pipeline_id": "pipe", "task_id": "task"},
            ),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    assert target_calls == 0


def test_resolved_hook_sql_bytes_are_bound_not_only_sql_file_path(tmp_path) -> None:
    hook_file = tmp_path / "hook.sql"
    hook_file.write_text("SELECT 1\n", encoding="utf-8")
    config = _config()
    config.options.update(
        {
            "manifest_dir": str(tmp_path),
            "repo_root": str(tmp_path),
            "hooks": {
                "pre_hook": [
                    {
                        "id": "verify",
                        "type": "sql",
                        "sql_file": "hook.sql",
                    }
                ]
            },
        }
    )
    _first_graph, first = mssql_hook_graph_contract(config)

    hook_file.write_text("SELECT 2\n", encoding="utf-8")
    _second_graph, second = mssql_hook_graph_contract(config)

    assert first.sha256 != second.sha256


def test_post_hook_graph_is_bound_into_invocation_route_fingerprint() -> None:
    first = _config()
    second = _config()
    first.options["hooks"] = {"post_hook": [{"id": "verify", "type": "sql", "sql": "SELECT 1"}]}
    second.options["hooks"] = {"post_hook": [{"id": "verify", "type": "sql", "sql": "SELECT 2"}]}

    assert invocation_route_fingerprint(
        first,
        target_identity=b"t" * 32,
        source_identity=_source_physical_identity(),
    ) != invocation_route_fingerprint(
        second,
        target_identity=b"t" * 32,
        source_identity=_source_physical_identity(),
    )


@pytest.mark.parametrize(
    "strategy",
    [
        LoadStrategy.FULL_REFRESH,
        LoadStrategy.INCREMENTAL_APPEND,
        LoadStrategy.INCREMENTAL_MERGE,
        LoadStrategy.REPLACE,
        LoadStrategy.PARTITION_REPLACE,
        LoadStrategy.SNAPSHOT_DIFF,
        LoadStrategy.SCD2,
        LoadStrategy.BACKFILL,
    ],
)
def test_every_generic_mssql_strategy_requires_and_receives_admission(strategy: LoadStrategy) -> None:
    captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]] = []
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="DWH",
            schema_name="dbo",
            table_name="target",
        ),
        state_factory=lambda _storage: _AdmissionState(captured),
    )
    config = _valid_strategy_admission_config(strategy)
    prepared = service.prepare(
        config,
        source=_certified_source(),
        sink=_admission_sink(
            SimpleNamespace(
                atomicity="target_atomic",
                provisioning="external",
                require_database_authority_binding=lambda: None,
            )
        ),
        run_context=RunContext("run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
        load_record=SimpleNamespace(load_id="load"),
        dag_id="dag",
    )

    assert isinstance(prepared.options[ADMISSION_OPTION], MssqlTransactionAdmission)
    assert len(prepared.options[MSSQL_HOOK_GRAPH_SHA256_OPTION]) == 64
    assert len(captured) == 1


@pytest.mark.parametrize(
    ("state", "blocker"),
    [
        (None, "state_storage_required"),
        (SimpleNamespace(atomicity="after_target", provisioning="external"), "target_atomic_state_required"),
        (SimpleNamespace(atomicity="target_atomic", provisioning="runtime"), "external_state_catalog_required"),
    ],
)
def test_generic_mssql_admission_default_denies_unsafe_state(state: object, blocker: str) -> None:
    service = MssqlTransactionAdmissionService()
    with pytest.raises(RuntimeError, match=blocker):
        service.prepare(
            _config(),
            source=_certified_source(),
            sink=_admission_sink(state),
            run_context=RunContext("run", config={"process": "p"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )


def test_declared_mssql_sink_without_frozen_capability_fails_closed() -> None:
    sink = SimpleNamespace(target_dialect=lambda: "mssql")
    with pytest.raises(RuntimeError, match="sink_capability_required"):
        MssqlTransactionAdmissionService().prepare(
            _config(),
            source=_certified_source(),
            sink=sink,
            run_context=RunContext("run", config={"process": "p"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )


def test_custom_source_without_checkpoint_capability_fails_before_state_or_source() -> None:
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: pytest.fail("target resolver must not run"),
    )
    with pytest.raises(RuntimeError, match="source_checkpoint_not_atomic:unknown"):
        service.prepare(
            _config(),
            source=SimpleNamespace(connector=object()),
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext("run", config={"process": "p"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )


@pytest.mark.parametrize(
    "mode",
    (
        "target_derived_or_stateless",
        MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE,
        "unregistered_custom_mode",
    ),
    ids=("legacy-ambiguous", "single-column-unsafe", "unknown"),
)
def test_generic_mssql_admission_rejects_ambiguous_or_unsafe_checkpoint_before_target_io(
    mode: object,
) -> None:
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: pytest.fail("target resolver must not run"),
    )
    source = _certified_source()
    source.mssql_transaction_checkpoint_mode = lambda _config: mode
    source.internal_query_capability = SimpleNamespace(eligible=True)
    source.bind_internal_query_capability = lambda _decision: pytest.fail(
        "artifact fallback must not run before checkpoint admission"
    )

    with pytest.raises(RuntimeError, match="source_checkpoint_not_atomic"):
        service.prepare(
            _config(),
            source=source,
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext("run", config={"process": "p"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )


def test_processor_replay_suppresses_source_hooks_staging_and_dml() -> None:
    source = _UnavailableSource()
    sink = _UnavailableSink()
    receipt = _receipt(_operation())

    class ReplayAdmission:
        @staticmethod
        def prepare(load_config: LoadConfig, **_kwargs: object) -> LoadConfig:
            return load_config

        @staticmethod
        def replay_result(_load_config: LoadConfig) -> LoadResult:
            return load_result_from_mssql_receipt(receipt, outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED)

    result = ETLProcessor(
        source,
        sink,
        etl_logger=_NoopLogger(),
        mssql_transaction_admission_service=ReplayAdmission(),
    ).run(
        _config(),
        RunContext("scheduled-run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
    )

    assert result["status"] == "success"
    assert result["commit_outcome"] == "replay_suppressed"
    assert source.calls == 0
    assert sink.calls == 0


def test_internal_query_without_receiptable_fallback_blocks_before_source() -> None:
    source = _UnavailableSource()
    source.internal_query_capability = SimpleNamespace(eligible=True)
    source.mssql_transaction_checkpoint_mode = lambda _config: MssqlTransactionCheckpointMode.STATELESS
    sink = _UnavailableSink()
    sink.target_dialect = lambda: "mssql"
    sink.mssql_transaction_governance_capability = lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY

    with pytest.raises(RuntimeError, match="internal_query_consumed_payload_evidence_unsupported"):
        ETLProcessor(source, sink, etl_logger=_NoopLogger()).run(
            _config(),
            RunContext("scheduled-run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
        )

    assert source.calls == 0
    assert sink.calls == 0


def test_finalizer_commits_business_dml_and_receipt_once() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState()
    finalizer = _finalizer(connector, state)
    called = 0

    def handler(_staging: object) -> LoadResult:
        nonlocal called
        called += 1
        return LoadResult(inserted_rows=2, updated_rows=1, total_rows=9)

    result = finalizer.finalize(
        _config(),
        MssqlTransactionAdmission(operation=_operation()),
        handler,
        _staging(3),
        staging_rows=3,
        load_id="load-A",
        source_lifecycle_receipt=_source_lifecycle_receipt(),
    )

    assert called == 1
    assert connector.commits == 1
    assert connector.rollbacks == 0
    assert state.receipt_inserts == 1
    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED
    assert result.commit_receipt_id == _operation().receipt_id


def test_finalizer_requires_complete_payload_evidence_before_business_dml() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState()
    called = False

    def handler(_staging: object) -> LoadResult:
        nonlocal called
        called = True
        return LoadResult(1, 0, 1)

    with pytest.raises(RuntimeError, match="consumed_payload_evidence_required"):
        _finalizer(connector, state).finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            handler,
            SimpleNamespace(row_count=1),
            staging_rows=1,
            load_id="load-A",
        )

    assert called is False
    assert connector.commits == 0
    assert connector.rollbacks == 0
    assert state.receipt_inserts == 0


def test_concurrent_durable_receipt_uses_read_rollback_not_ambiguous_commit() -> None:
    connector = _FinalizerConnector(commit_error=OSError("read commit ack lost"))
    state = _FinalizerState()
    state.concurrent_receipt = _receipt(_operation(), payload_evidence=_commit_evidence_for_staging(1))
    called = False

    def handler(_staging: object) -> LoadResult:
        nonlocal called
        called = True
        return LoadResult(1, 0, 1)

    result = _finalizer(connector, state).finalize(
        _config(),
        MssqlTransactionAdmission(operation=_operation()),
        handler,
        _staging(1),
        staging_rows=1,
        load_id="load-A",
        source_lifecycle_receipt=_source_lifecycle_receipt(),
    )

    assert called is False
    assert result.commit_outcome == AtomicCommitOutcome.REPLAY_SUPPRESSED
    assert connector.commits == 0
    assert connector.rollbacks == 1


def test_commit_ack_loss_closes_target_and_accepts_only_fresh_exact_receipt() -> None:
    connector = _FinalizerConnector(commit_error=OSError("ack lost"))
    state = _FinalizerState(probe_after_commit=True)
    result = _finalizer(connector, state).finalize(
        _config(),
        MssqlTransactionAdmission(operation=_operation()),
        lambda _staging: LoadResult(inserted_rows=1, updated_rows=0, total_rows=1),
        _staging(1),
        staging_rows=1,
        load_id="load-A",
        source_lifecycle_receipt=_source_lifecycle_receipt(),
    )

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE
    assert connector.closed == 1
    assert connector.rollbacks == 0
    assert state.fresh_probes == 1


def test_finalizer_checks_lease_once_then_holds_owner_fence_through_commit() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState()
    result = _finalizer(connector, state).finalize(
        _config(),
        MssqlTransactionAdmission(operation=_operation()),
        lambda _staging: LoadResult(1, 0, 1),
        _staging(1),
        staging_rows=1,
        load_id="load-A",
        source_lifecycle_receipt=_source_lifecycle_receipt(),
    )

    assert result.commit_outcome == AtomicCommitOutcome.COMMITTED
    assert state.lease_assertions == [True, False]


def test_schema_and_physical_mutations_execute_inside_fenced_target_transaction() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState()
    revalidations: list[tuple[str, frozenset[str] | None]] = []

    def revalidate(*_args: object, **kwargs: object) -> None:
        if kwargs.get("boundary") == "after" and kwargs.get("kinds") is None:
            connector.calls.append("CATALOG_AFTER_ALL")
        revalidations.append(
            (
                str(kwargs.get("boundary")),
                kwargs.get("kinds") if isinstance(kwargs.get("kinds"), frozenset) else None,
            )
        )

    admission = MssqlTransactionAdmission(operation=_operation())
    from dpone.readiness.physical_state import PhysicalTableState
    from dpone.readiness.schema_evolution import ColumnDef
    from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
        exact_schema_catalog_transition,
        physical_catalog_expectation,
    )
    from dpone.runtime.sinks.mssql_target_catalog_model import (
        MssqlSchemaCatalogSnapshot,
        catalog_column_from_definition,
    )

    before = MssqlSchemaCatalogSnapshot(True, "Latin1_General_100_BIN2")
    after = MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        (
            catalog_column_from_definition(
                ColumnDef("note", "nvarchar(10)"),
                ordinal=1,
                database_collation="Latin1_General_100_BIN2",
            ),
        ),
    )
    schema_expectation = exact_schema_catalog_transition(before, after)
    physical_expectation = physical_catalog_expectation(
        PhysicalTableState(sink_type="mssql", table="[DWH].[dbo].[target]")
    )
    plan = (
        MssqlTargetMutationPlan.from_admission(admission)
        .append(
            "schema_evolution",
            ["ALTER TABLE [DWH].[dbo].[target] ADD [note] nvarchar(10) NULL"],
            expectation=schema_expectation,
        )
        .append(
            "physical_design",
            ["CREATE INDEX [ix_target_note] ON [DWH].[dbo].[target] ([note])"],
            expectation=physical_expectation,
        )
    )

    _finalizer(connector, state, catalog_revalidator=revalidate).finalize(
        _config(),
        admission,
        lambda _staging: LoadResult(1, 0, 1),
        _staging(1),
        staging_rows=1,
        load_id="load-A",
        target_mutation_plan=plan,
        source_lifecycle_receipt=_source_lifecycle_receipt(),
    )

    action_positions = [connector.calls.index(action.sql) for action in plan.actions]
    catalog_after_position = connector.calls.index("CATALOG_AFTER_ALL")
    loaded_at_position = connector.calls.index("SELECT SYSUTCDATETIME() AS loaded_at_utc")
    assert max(action_positions) < catalog_after_position < loaded_at_position
    assert state.lease_assertions == [True, False]
    assert revalidations == [
        ("before", None),
        ("after", frozenset({"schema_columns"})),
        ("after", frozenset({"physical_design"})),
        ("after", None),
        ("after", None),
    ]


def test_target_mutation_requires_exact_before_and_after_expectation() -> None:
    admission = MssqlTransactionAdmission(operation=_operation())
    plan = MssqlTargetMutationPlan.from_admission(admission)

    with pytest.raises(ValueError, match="target_mutation_expectation_required"):
        plan.append(
            "schema_evolution",
            ["ALTER TABLE [DWH].[dbo].[target] ADD [note] nvarchar(10) NULL"],
        )


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL; DROP TABLE [DWH].[dbo].[other]",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL -- hidden",
        "ALTER TABLE [DWH].[dbo].[other] ADD [note] int NULL",
        "ALTER TABLE [DWH].[dbo].[target] DROP CONSTRAINT [pk_target]",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL, [extra] int NULL",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL CHECK ([note] > 0)",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL REFERENCES [dbo].[parent] ([id])",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NOT NULL DEFAULT (0)",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int IDENTITY(1,1) NOT NULL",
        "CREATE INDEX [ix_note] ON [DWH].[dbo].[target] ([note]) WITH (ONLINE = ON)",
        "CREATE INDEX [ix_note] ON [DWH].[dbo].[target] ([note]) WHERE 1 = 1",
        "CREATE INDEX [ix_note] ON [DWH].[dbo].[target] ([note]) REFERENCES [dbo].[parent] ([id])",
        "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL\nDELETE FROM [DWH].[dbo].[target]",
        "UPDATE [DWH].[dbo].[target] SET [note] = 1",
        "EXEC(N'ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL')",
        "USE [DWH]",
    ],
)
def test_target_mutation_rejects_untrusted_or_multi_statement_sql(statement: str) -> None:
    admission = MssqlTransactionAdmission(operation=_operation())
    expectation = _schema_mutation_plan().expectations[0]

    with pytest.raises(ValueError, match="target_mutation"):
        MssqlTargetMutationPlan.from_admission(admission).append(
            "schema_evolution",
            [statement],
            expectation=expectation,
        )


def test_lock_timeout_is_a_typed_session_policy_action() -> None:
    admission = MssqlTransactionAdmission(operation=_operation())
    expectation = _schema_mutation_plan().expectations[0]
    plan = MssqlTargetMutationPlan.from_admission(admission).append(
        "schema_evolution",
        ["SET LOCK_TIMEOUT 1000", "ALTER TABLE [DWH].[dbo].[target] ADD [note] int NULL"],
        expectation=expectation,
    )

    assert [action.statement_type for action in plan.actions] == [
        "session_policy",
        "target_ddl",
    ]


def test_target_catalog_before_mismatch_blocks_ddl_and_business_dml() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState()
    plan = _schema_mutation_plan()
    called = False

    def revalidate(*_args: object, boundary: str, **_kwargs: object) -> None:
        if boundary == "before":
            raise RuntimeError("mssql_transaction.target_catalog_before_mismatch:schema_columns")

    def handler(_staging: object) -> LoadResult:
        nonlocal called
        called = True
        return LoadResult(1, 0, 1)

    with pytest.raises(RuntimeError, match="target_catalog_before_mismatch"):
        _finalizer(connector, state, catalog_revalidator=revalidate).finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            handler,
            _staging(1),
            staging_rows=1,
            load_id="load-A",
            target_mutation_plan=plan,
            source_lifecycle_receipt=_source_lifecycle_receipt(),
        )

    assert called is False
    assert all(action.sql not in connector.calls for action in plan.actions)
    assert connector.rollbacks == 1
    assert state.receipt_inserts == 0


def test_target_catalog_after_mismatch_rolls_back_before_business_dml_and_receipt() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState()
    plan = _schema_mutation_plan()
    called = False

    def revalidate(*_args: object, boundary: str, **_kwargs: object) -> None:
        if boundary == "after":
            raise RuntimeError("mssql_transaction.target_catalog_after_mismatch:schema_columns")

    def handler(_staging: object) -> LoadResult:
        nonlocal called
        called = True
        return LoadResult(1, 0, 1)

    with pytest.raises(RuntimeError, match="target_catalog_after_mismatch"):
        _finalizer(connector, state, catalog_revalidator=revalidate).finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            handler,
            _staging(1),
            staging_rows=1,
            load_id="load-A",
            target_mutation_plan=plan,
            source_lifecycle_receipt=_source_lifecycle_receipt(),
        )

    assert called is False
    assert [action.sql for action in plan.actions] == connector.calls[-1:]
    assert connector.rollbacks == 1
    assert state.receipt_inserts == 0


def test_unknown_commit_never_rolls_back_and_preserves_staging_disposition() -> None:
    connector = _FinalizerConnector(commit_error=OSError("ack lost"))
    state = _FinalizerState(probe_after_commit=False)

    with pytest.raises(MssqlGenericCommitOutcomeUnknown) as raised:
        _finalizer(connector, state).finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            lambda _staging: LoadResult(inserted_rows=1, updated_rows=0, total_rows=1),
            _staging(1),
            staging_rows=1,
            load_id="load-A",
            source_lifecycle_receipt=_source_lifecycle_receipt(),
        )

    assert raised.value.cleanup_disposition.value == "preserve_staging_evidence"
    assert connector.closed == 1
    assert connector.rollbacks == 0


def test_stale_generation_rolls_back_before_business_mutation() -> None:
    connector = _FinalizerConnector()
    state = _FinalizerState(stale=True)
    called = False

    def handler(_staging: object) -> LoadResult:
        nonlocal called
        called = True
        return LoadResult(0, 0, 0)

    with pytest.raises(RuntimeError, match="stale"):
        _finalizer(connector, state).finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            handler,
            _staging(0),
            staging_rows=0,
            load_id="load-A",
            source_lifecycle_receipt=_source_lifecycle_receipt(),
        )
    assert called is False
    assert connector.rollbacks == 1
    assert state.receipt_inserts == 0


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="pg-source",
        target_conn_id="mssql-target",
        source_schema="public",
        source_table="source",
        target_database="DWH",
        target_schema="dbo",
        target_table="target",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "lineage": False,
            "postgres_source_authority_sha256": _SOURCE_AUTHORITY_SHA256,
        },
    )


def _valid_strategy_admission_config(strategy: LoadStrategy) -> LoadConfig:
    if strategy is LoadStrategy.BACKFILL:
        config = _backfill_config(chunk_index=1, owner="worker", inner_mode="incremental_merge")
        config.unique_key = ["id"]
        return config
    config = _config()
    config.load_strategy = strategy
    if strategy is LoadStrategy.INCREMENTAL_MERGE:
        config.unique_key = ["id"]
        config.merge_policy = "update_insert"
    elif strategy is LoadStrategy.REPLACE:
        config.custom_predicate = "id >= 0"
        config.options["source_type"] = "mssql"
    elif strategy is LoadStrategy.PARTITION_REPLACE:
        config.partition = {"column": "id", "native_mode": "fallback"}
    elif strategy is LoadStrategy.SNAPSHOT_DIFF:
        config.unique_key = ["id"]
        config.options["diff"] = {"delete_policy": "ignore"}
    elif strategy is LoadStrategy.SCD2:
        config.unique_key = ["id"]
        config.options["scd2"] = {"delete_policy": "ignore"}
    return config


def _backfill_config(
    *,
    chunk_index: int,
    owner: str,
    inner_mode: str = "partition_replace",
) -> LoadConfig:
    config = _config()
    config.load_strategy = LoadStrategy.BACKFILL
    config.options["backfill"] = {
        "inner_mode": inner_mode,
        "chunk": {"column": "id", "from": 0, "to": 20, "step": 10},
    }
    policy = normalize_backfill_execution_policy(config.options["backfill"])
    assert policy.chunk is not None
    chunks = plan_chunks(policy.chunk, run_key="backfill")
    chunk_config = BackfillChunkExecutor._chunk_load_config(
        config,
        chunks[chunk_index - 1],
        run_key="backfill",
        plan_hash=plan_hash(chunks, execution_policy=policy),
        owner=owner,
        lease_expires_at_utc=datetime.now(UTC) + timedelta(hours=1),
        portable_scope_campaign=CampaignPortableScopeBinding(
            columns=PortableScopeColumnContract(
                source_name="id",
                source_type="bigint",
                source_collation=None,
                target_name="id",
                target_type="bigint",
                target_collation=None,
            ),
            identity_bound=True,
        ),
    )
    assert chunk_config.portable_scope is not None
    return chunk_config


def _attempt_request(*, load_id: str, run_id: str = "scheduled-run") -> MssqlAttemptRequest:
    return MssqlAttemptRequest(
        invocation=InvocationIdentity(run_id, "process", "pipeline:task"),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id=load_id,
        target_database="DWH",
        target_schema="dbo",
        target_table="target",
        strategy="incremental_append",
    )


def _operation_request() -> MssqlOperationRequest:
    return MssqlOperationRequest(
        operation_scope_hash({"kind": "target_wide", "version": 1}),
        operation_owner_digest("owner"),
    )


def _admission_sink(state_storage: object) -> SimpleNamespace:
    return SimpleNamespace(
        connector=object(),
        state_storage=state_storage,
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
        get_target_catalog_snapshot=_matching_target_snapshot,
    )


def _certified_source() -> SimpleNamespace:
    projected = SimpleNamespace(
        source_name="id",
        source_type="bigint",
        source_collation=None,
        target_name="id",
        target_type="bigint",
        collation=None,
        nullable=True,
    )
    return SimpleNamespace(
        connector=SimpleNamespace(host="pg", port=5432, database="source"),
        mssql_transaction_checkpoint_mode=lambda _config: MssqlTransactionCheckpointMode.STATELESS,
        mssql_transaction_source_physical_identity=lambda _config: _source_physical_identity(),
        fetch_schema_projection=lambda _config: SimpleNamespace(
            relation_schema=(("id", "bigint"),),
            projected_schema=(("id", "bigint"),),
            target_projection=SimpleNamespace(columns=(projected,)),
        ),
    )


def _source_physical_identity(*, cluster: str = "pg-cluster-A") -> SourcePhysicalIdentity:
    return SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier=cluster,
        database="source",
        effective_principal="reader",
        session_principal="reader",
        server_address="192.0.2.1",
        server_port=5432,
        topology_role="standby",
        version=2,
        authority_sha256=_SOURCE_AUTHORITY_SHA256,
        timeline_id=1,
        database_oid=16384,
        effective_principal_oid=16385,
        session_principal_oid=16385,
        schema="public",
        schema_oid=2200,
        relation="source",
        relation_oid=16390,
    )


def _matching_target_snapshot(config: LoadConfig) -> MssqlSchemaCatalogSnapshot:
    columns = resolve_mssql_target_columns(config, [ColumnDef("id", "bigint")])
    unique = resolve_unique_authority_contract(
        config,
        {column.name: column.dtype for column in columns},
    )
    indexes = (
        (
            MssqlIndexState(
                name=unique.name,
                type_desc=unique.type_desc,
                unique=unique.is_unique,
                primary_key=unique.is_primary_key,
                unique_constraint=unique.is_unique_constraint,
                disabled=unique.is_disabled,
                hypothetical=unique.is_hypothetical,
                ignore_dup_key=unique.ignore_dup_key,
                filter_definition=unique.filter_definition,
                key_columns=unique.key_columns,
                included_columns=(),
                descending_keys=(False,) * len(unique.key_columns),
                data_space_type_desc="ROWS_FILEGROUP",
                partition_compression=(unique.data_compression,),
            ),
        )
        if unique is not None
        else ()
    )
    return MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        columns=tuple(
            catalog_column_from_definition(
                column,
                ordinal=index,
                database_collation="Latin1_General_100_BIN2",
            )
            for index, column in enumerate(columns, start=1)
        ),
        indexes=indexes,
    )


def _operation() -> MssqlTransactionOperation:
    attempt = MssqlTransactionAttempt(_attempt_request(load_id="load-A"), generation=3)
    request = _operation_request()
    return MssqlTransactionOperation(
        attempt=attempt,
        operation_key=request.operation_key(attempt),
        scope_hash=request.scope_hash,
        owner_digest=request.owner_digest,
        epoch=2,
    )


def _payload_commit_evidence(rows: int) -> MssqlPayloadCommitEvidence:
    return MssqlPayloadCommitEvidence(
        manifest_sha256=b"m" * 32,
        declared_rows=rows,
        actual_raw_rows=rows,
        actual_native_rows=rows,
        native_contract_sha256=b"n" * 32,
    )


def _commit_evidence_for_staging(rows: int) -> MssqlPayloadCommitEvidence:
    evidence = _staging(rows).consumed_payload_evidence.require_complete()
    assert evidence.actual_native_rows is not None
    assert evidence.native_contract_sha256 is not None
    return MssqlPayloadCommitEvidence(
        manifest_sha256=bytes.fromhex(evidence.manifest_sha256),
        declared_rows=evidence.declared_rows,
        actual_raw_rows=evidence.actual_raw_rows,
        actual_native_rows=evidence.actual_native_rows,
        native_contract_sha256=bytes.fromhex(evidence.native_contract_sha256),
    )


def _staging(rows: int) -> SimpleNamespace:
    part = ConsumedPayloadPartEvidence(
        order_key="sequential:00000000000000000000",
        artifact_sha256="a" * 64,
        artifact_size_bytes=rows,
        wire_contract_sha256="b" * 64,
        wire_schema_sha256="c" * 64,
        source_provenance_sha256="d" * 64,
        validated_schema_sha256="e" * 64,
        contract_validation_sha256=None,
        declared_rows=rows,
        actual_raw_rows=rows,
    )
    evidence = ConsumedPayloadEvidence(
        (part,),
        actual_native_rows=rows,
        native_contract_sha256="f" * 64,
    )
    return SimpleNamespace(row_count=rows, consumed_payload_evidence=evidence)


def _receipt(
    operation: MssqlTransactionOperation,
    *,
    payload_evidence: MssqlPayloadCommitEvidence | None = None,
) -> MssqlGenericCommitReceipt:
    attempt = operation.attempt
    source_lifecycle = _source_lifecycle_evidence()
    loaded_at_utc = datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC)
    mutation_plan = MssqlTargetMutationPlan.from_admission(MssqlTransactionAdmission(operation=operation))
    return MssqlGenericCommitReceipt(
        receipt_id=operation.receipt_id,
        operation_key=operation.operation_key,
        attempt_key=attempt.attempt_key,
        target_identity=attempt.target_identity,
        generation=attempt.generation,
        scope_hash=operation.scope_hash,
        operation_epoch=operation.epoch,
        owner_digest=operation.owner_digest,
        route_fingerprint=attempt.route_fingerprint,
        load_id="load-A",
        strategy=attempt.request.strategy,
        mutation_plan_sha256=mutation_plan.digest,
        target_before_sha256=mutation_plan.expected_before_sha256,
        target_after_sha256=mutation_plan.expected_after_sha256,
        loaded_at_utc=loaded_at_utc,
        committed_at_utc=datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC),
        payload_evidence=payload_evidence or _payload_commit_evidence(1),
        source_lifecycle=source_lifecycle,
        metrics=MssqlReceiptMetrics(1, 0, 1, staging_rows=1),
    )


def _source_lifecycle_evidence() -> MssqlSourceLifecycleEvidence:
    return MssqlSourceLifecycleEvidence(
        extraction_started_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
        extraction_completed_at_utc=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        clock_authority="dpone.orchestrator.utc",
        snapshot_acquired_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
        snapshot_authority="postgresql.repeatable_read",
        source_token_sha256=hashlib.sha256(b"source-token").digest(),
    )


def _source_lifecycle_receipt() -> ExtractionLifecycleReceipt:
    return ExtractionLifecycleReceipt(
        extraction_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        clock_authority="dpone.orchestrator.utc",
        snapshot_acquired_at=datetime(2026, 1, 1, tzinfo=UTC),
        snapshot_authority="postgresql.repeatable_read",
        extraction_completed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        source_token="source-token",
    )


class _AdmissionState:
    def __init__(self, captured: list[tuple[MssqlAttemptRequest, MssqlOperationRequest]]) -> None:
        self.captured = captured

    def preflight(self, _connector: object) -> None:
        return None

    def admit(self, attempt: MssqlAttemptRequest, operation: MssqlOperationRequest) -> MssqlTransactionAdmission:
        self.captured.append((attempt, operation))
        resolved_attempt = MssqlTransactionAttempt(attempt, generation=1)
        return MssqlTransactionAdmission(
            operation=MssqlTransactionOperation(
                resolved_attempt,
                operation.operation_key(resolved_attempt),
                operation.scope_hash,
                operation.owner_digest,
                1,
                operation.lease_expires_at_utc,
            )
        )


class _UnavailableSource:
    def __init__(self) -> None:
        self.calls = 0
        self.connector = SimpleNamespace(host="unavailable", port=5432, database="source")

    def get_incremental_state(self, _load_config: LoadConfig) -> object:
        self.calls += 1
        raise OSError("source unavailable")

    def extract(self, _load_config: LoadConfig, _state: object) -> object:
        self.calls += 1
        raise OSError("source unavailable")


class _UnavailableSink:
    def __init__(self) -> None:
        self.calls = 0

    def load(self, _load_config: LoadConfig, _payload: object) -> LoadResult:
        self.calls += 1
        raise AssertionError("sink load must be replay-suppressed")


class _NoopLogger:
    def log_etl_start(self, _payload: object) -> None: ...

    def log_etl_progress(self, _event: str, _payload: object) -> None: ...

    def log_etl_error(self, _message: str, _payload: object) -> None: ...

    def log_etl_end(self, _payload: object) -> None: ...

    def info(self, _message: str) -> None: ...

    def warning(self, _message: str) -> None: ...


class _FinalizerConnector:
    def __init__(self, *, commit_error: Exception | None = None) -> None:
        self.commit_error = commit_error
        self.commits = 0
        self.rollbacks = 0
        self.sqls: list[str] = []
        self.closed = 0
        self.calls: list[str] = []

    def begin(self) -> None:
        return None

    def execute_query(self, _sql: str, _params: object = None) -> int:
        self.calls.append(_sql)
        return 0

    def get_records(self, _sql: str, _params: object = None, *, as_dict: bool = False) -> list[dict[str, object]]:
        assert as_dict
        self.calls.append(_sql)
        if "@@SPID" in _sql:
            return [{"session_id": 57}]
        return [{"loaded_at_utc": datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC)}]

    def commit_transaction(self) -> None:
        self.commits += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed += 1


class _FinalizerState:
    def __init__(self, *, probe_after_commit: bool = False, stale: bool = False) -> None:
        self.probe_after_commit = probe_after_commit
        self.stale = stale
        self.receipt_inserts = 0
        self.fresh_probes = 0
        self.persisted: MssqlGenericCommitReceipt | None = None
        self.concurrent_receipt: MssqlGenericCommitReceipt | None = None
        self.lease_assertions: list[bool] = []

    def assert_current(
        self,
        _connector: object,
        operation: MssqlTransactionOperation,
        *,
        require_unexpired_lease: bool,
    ) -> None:
        self.lease_assertions.append(require_unexpired_lease)
        if self.stale:
            raise RuntimeError("mssql_transaction.stale_source_generation")
        assert operation == _operation()

    def probe_receipt(self, *_args: object, **_kwargs: object) -> MssqlGenericCommitReceipt | None:
        return self.concurrent_receipt

    def insert_receipt(
        self,
        _connector: object,
        operation: MssqlTransactionOperation,
        *,
        load_id: str,
        payload_evidence: MssqlPayloadCommitEvidence,
        source_lifecycle: MssqlSourceLifecycleEvidence,
        mutation_plan_sha256: bytes,
        target_before_sha256: bytes,
        target_after_sha256: bytes,
        loaded_at_utc: datetime,
        metrics: MssqlReceiptMetrics,
    ) -> MssqlGenericCommitReceipt:
        self.receipt_inserts += 1
        self.persisted = MssqlGenericCommitReceipt(
            receipt_id=operation.receipt_id,
            operation_key=operation.operation_key,
            attempt_key=operation.attempt.attempt_key,
            target_identity=operation.attempt.target_identity,
            generation=operation.attempt.generation,
            scope_hash=operation.scope_hash,
            operation_epoch=operation.epoch,
            owner_digest=operation.owner_digest,
            route_fingerprint=operation.attempt.route_fingerprint,
            load_id=load_id,
            strategy=operation.attempt.request.strategy,
            mutation_plan_sha256=mutation_plan_sha256,
            target_before_sha256=target_before_sha256,
            target_after_sha256=target_after_sha256,
            loaded_at_utc=loaded_at_utc,
            committed_at_utc=datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC),
            payload_evidence=payload_evidence,
            source_lifecycle=source_lifecycle,
            metrics=metrics,
        )
        return self.persisted

    def probe_receipt_fresh(self, _operation: MssqlTransactionOperation) -> MssqlGenericCommitReceipt | None:
        self.fresh_probes += 1
        return self.persisted if self.probe_after_commit else None


def _finalizer(
    connector: _FinalizerConnector,
    state: _FinalizerState,
    *,
    catalog_revalidator: object | None = None,
) -> MssqlGenericTransactionFinalizer:
    strategy = SimpleNamespace(connector=connector)
    return MssqlGenericTransactionFinalizer(
        strategy,
        object(),
        transaction_state=state,
        lock_acquirer=lambda *_args, **_kwargs: None,
        target_lock_acquirer=lambda *_args, **_kwargs: None,
        target_identity_assertion=lambda *_args, **_kwargs: None,
        catalog_revalidator=catalog_revalidator or (lambda *_args, **_kwargs: None),
        target_contract_validator=lambda *_args, **_kwargs: None,
    )


def _schema_mutation_plan() -> MssqlTargetMutationPlan:
    from dpone.readiness.schema_evolution import ColumnDef
    from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
        exact_schema_catalog_transition,
    )
    from dpone.runtime.sinks.mssql_target_catalog_model import (
        MssqlSchemaCatalogSnapshot,
        catalog_column_from_definition,
    )

    admission = MssqlTransactionAdmission(operation=_operation())
    before = MssqlSchemaCatalogSnapshot(True, "Latin1_General_100_BIN2")
    after = MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        (
            catalog_column_from_definition(
                ColumnDef("note", "nvarchar(10)"),
                ordinal=1,
                database_collation="Latin1_General_100_BIN2",
            ),
        ),
    )
    return MssqlTargetMutationPlan.from_admission(admission).append(
        "schema_evolution",
        ["ALTER TABLE [DWH].[dbo].[target] ADD [note] nvarchar(10) NULL"],
        expectation=exact_schema_catalog_transition(before, after),
    )


def _attempt_row(request: MssqlAttemptRequest, *, generation: int, current: bool) -> dict[str, object]:
    return {
        "attempt_key": request.attempt_key,
        "target_identity": request.target_identity,
        "generation": generation,
        "invocation_digest": request.invocation.invocation_digest,
        "route_fingerprint": request.route_fingerprint,
        "target_database": request.target_database,
        "target_schema": request.target_schema,
        "target_table": request.target_table,
        "strategy": request.strategy,
        "is_current_generation": current,
    }


def _operation_row(
    attempt: MssqlTransactionAttempt,
    request: MssqlOperationRequest,
) -> dict[str, object]:
    return {
        "operation_key": request.operation_key(attempt),
        "attempt_key": attempt.attempt_key,
        "scope_hash": request.scope_hash,
        "current_epoch": 1,
        "current_owner_digest": request.owner_digest,
        "lease_expires_at_utc": request.lease_expires_at_utc,
        "committed_receipt_id": None,
    }


def _receipt_row(receipt: MssqlGenericCommitReceipt) -> dict[str, object]:
    evidence = receipt.payload_evidence
    metrics = receipt.metrics
    return {
        "receipt_id": receipt.receipt_id,
        "operation_key": receipt.operation_key,
        "attempt_key": receipt.attempt_key,
        "target_identity": receipt.target_identity,
        "generation": receipt.generation,
        "scope_hash": receipt.scope_hash,
        "operation_epoch": receipt.operation_epoch,
        "owner_digest": receipt.owner_digest,
        "route_fingerprint": receipt.route_fingerprint,
        "load_id": receipt.load_id,
        "strategy": receipt.strategy,
        "mutation_plan_sha256": receipt.mutation_plan_sha256,
        "target_before_sha256": receipt.target_before_sha256,
        "target_after_sha256": receipt.target_after_sha256,
        "loaded_at_utc": receipt.loaded_at_utc.replace(tzinfo=None),
        "committed_at_utc": receipt.committed_at_utc.replace(tzinfo=None),
        "payload_manifest_sha256": evidence.manifest_sha256,
        "declared_rows": evidence.declared_rows,
        "actual_raw_rows": evidence.actual_raw_rows,
        "actual_native_rows": evidence.actual_native_rows,
        "native_contract_sha256": evidence.native_contract_sha256,
        "extraction_started_at_utc": receipt.source_lifecycle.extraction_started_at_utc.replace(tzinfo=None),
        "extraction_completed_at_utc": receipt.source_lifecycle.extraction_completed_at_utc.replace(tzinfo=None),
        "extraction_clock_authority": receipt.source_lifecycle.clock_authority,
        "snapshot_acquired_at_utc": (
            receipt.source_lifecycle.snapshot_acquired_at_utc.replace(tzinfo=None)
            if receipt.source_lifecycle.snapshot_acquired_at_utc is not None
            else None
        ),
        "snapshot_authority": receipt.source_lifecycle.snapshot_authority,
        "source_token_sha256": receipt.source_lifecycle.source_token_sha256,
        **{field: getattr(metrics, field) for field in metrics.__dataclass_fields__},
    }


class _StateSession:
    def __init__(
        self,
        name: str,
        rows: list[dict[str, object]],
        *,
        commit_error: Exception | None = None,
        records_error: Exception | None = None,
    ) -> None:
        self.name = name
        self.rows = rows
        self.commit_error = commit_error
        self.records_error = records_error
        self.closed = False
        self.commits = 0
        self.rollbacks = 0
        self.sqls: list[str] = []

    def begin(self) -> None:
        self._require_open()

    def get_records(self, _sql: str, _params: object = None, *, as_dict: bool = False) -> list[dict[str, object]]:
        self._require_open()
        assert as_dict
        self.sqls.append(_sql)
        if self.records_error is not None:
            raise self.records_error
        return list(self.rows)

    def commit_transaction(self) -> None:
        self._require_open()
        self.commits += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self._require_open()
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    def _require_open(self) -> None:
        if self.closed:
            raise AssertionError(f"closed state session reused: {self.name}")


class _FreshSessionSequence:
    def __init__(self, *sessions: _StateSession) -> None:
        self._sessions = iter(sessions)
        self.templates: list[str] = []

    def __call__(self, template: _StateSession) -> _StateSession:
        self.templates.append(template.name)
        return next(self._sessions)
