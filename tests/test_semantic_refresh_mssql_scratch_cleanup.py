from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.adapters.semantic_refresh_mssql_scratch_cleanup import (
    MssqlSemanticRefreshFailedScratchCleanupReader,
    SemanticRefreshMssqlScratchCleanupReadError,
)
from dpone.ports.semantic_refresh_clickhouse_authority import (
    clickhouse_operation_table_names,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlFailedScratchCleanupReadRequest,
    MssqlFailedScratchCleanupStateRecord,
)
from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHousePreparedReceipt,
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
    semantic_refresh_fingerprint,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    prepared_publication_documents,
)
from dpone.services.semantic_refresh_mssql_replacement import (
    SemanticRefreshMssqlFailedScratchCleanupService,
    SemanticRefreshMssqlReplacementBlocked,
)
from tests.test_semantic_refresh_mssql_authority import _bundle, _record


def _request() -> MssqlFailedScratchCleanupReadRequest:
    bundle = _bundle()
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    resource = bundle.model_resources[0]
    return MssqlFailedScratchCleanupReadRequest.build(
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
        canonical_authority_sha256=bundle.authority_sha256,
        model_unique_id=operation.model_unique_id,
        operation_id=operation.operation_id,
        operation_plan_sha256=operation.operation_plan_sha256,
        attempt_binding_sha256=attempt.attempt_binding_sha256,
        fencing_epoch=attempt.fencing_epoch,
        target_authority_id=resource.target_authority_id,
        database_name=resource.publication_database,
        target_table=resource.publication_target_table,
        target_generation=resource.target_predecessor_generation,
        target_generation_id=resource.target_predecessor_generation_id,
        target_uuid=resource.clickhouse_target_uuid,
        target_owner_operation_id=resource.target_predecessor_operation_id,
    )


class _Cursor:
    def __init__(
        self,
        *,
        execution_status: str = "FAILED_PRE_COMMIT",
        journal_status: str = "FAILED_PRE_COMMIT",
        mssql_outcome: str = "ROLLED_BACK",
        target_uuid: str | None = None,
    ) -> None:
        self.execution_status = execution_status
        self.journal_status = journal_status
        self.mssql_outcome = mssql_outcome
        self.target_uuid = target_uuid
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._row: tuple[object, ...] | None = None

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        self.executions.append((sql, tuple(parameters)))
        request = _request()
        bundle = _bundle()
        authority = _record(bundle)
        if "semantic_refresh_canonical_authorities" in sql:
            self._row = (
                authority.workflow_execution_id,
                authority.authority_sha256,
                authority.authority_json,
                authority.status,
            )
        elif "semantic_refresh_workflow_executions" in sql:
            self._row = (
                bundle.authority_sha256,
                self.execution_status,
                "sha256:" + "9" * 64,
                "{}",
            )
        elif "semantic_refresh_journals" in sql:
            self._row = (
                request.workflow_execution_id,
                request.model_unique_id,
                request.operation_plan_sha256,
                request.attempt_binding_sha256,
                request.fencing_epoch,
                request.database_name,
                request.target_table,
                self.mssql_outcome,
                "sha256:" + "8" * 64,
                None,
                None,
                None,
                None,
                None,
                self.journal_status,
            )
        elif "semantic_refresh_target_heads" in sql:
            self._row = (
                request.target_generation,
                request.target_generation_id,
                self.target_uuid or request.target_uuid,
                request.target_owner_operation_id,
            )
        else:
            self._row = None
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def close(self) -> None:
        return None


@dataclass
class _Connection:
    cursor_instance: _Cursor
    autocommit: bool = True
    commits: int = 0
    rollbacks: int = 0

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


@dataclass(frozen=True)
class _State:
    record: MssqlFailedScratchCleanupStateRecord

    def load_failed_scratch_cleanup(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedScratchCleanupStateRecord:
        assert workflow_execution_binding_sha256 == self.record.request.workflow_execution_binding_sha256
        assert operation_id == self.record.request.operation_id
        return self.record


def test_failed_scratch_reader_locks_exact_failed_state_and_unchanged_target() -> None:
    connection = _Connection(_Cursor())
    request = _request()

    record = MssqlSemanticRefreshFailedScratchCleanupReader(lambda: connection).load_failed_scratch_cleanup(
        request.workflow_execution_binding_sha256,
        request.operation_id,
    )

    assert record.request == request
    assert record.mssql_outcome == "ROLLED_BACK"
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert all("WITH (UPDLOCK, HOLDLOCK)" in sql for sql, _ in connection.cursor_instance.executions if "SELECT" in sql)


@pytest.mark.parametrize(
    ("cursor", "message"),
    [
        (_Cursor(execution_status="COMMITTING"), "not exact FAILED_PRE_COMMIT"),
        (_Cursor(journal_status="COMMITTING"), "journal conflict"),
        (_Cursor(journal_status="COMMIT_UNKNOWN"), "journal conflict"),
        (_Cursor(mssql_outcome="COMMIT_UNKNOWN"), "journal conflict"),
        (_Cursor(target_uuid="00000000-0000-0000-0000-000000000099"), "target changed"),
    ],
)
def test_failed_scratch_reader_rejects_unsafe_or_changed_state(
    cursor: _Cursor,
    message: str,
) -> None:
    connection = _Connection(cursor)
    request = _request()

    with pytest.raises(SemanticRefreshMssqlScratchCleanupReadError, match=message):
        MssqlSemanticRefreshFailedScratchCleanupReader(lambda: connection).load_failed_scratch_cleanup(
            request.workflow_execution_binding_sha256,
            request.operation_id,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_failed_scratch_service_returns_only_deterministic_non_target_relations() -> None:
    request = _request()
    record = MssqlFailedScratchCleanupStateRecord(request, "ROLLED_BACK", None, None, None, None)

    authority = SemanticRefreshMssqlFailedScratchCleanupService(_State(record)).load(
        request.workflow_execution_binding_sha256,
        request.operation_id,
    )

    staging, shadow = clickhouse_operation_table_names(request.target_table, request.operation_id)
    assert tuple((item.relation_role, item.table_name) for item in authority.relations) == (
        ("SHADOW", shadow),
        ("STAGING", staging),
    )
    assert all(item.table_name != request.target_table for item in authority.relations)
    assert all(item.observed_uuid is None for item in authority.relations)
    assert authority.protected_target_uuid == request.target_uuid


def test_failed_scratch_service_authenticates_persisted_observed_uuids() -> None:
    request = _request()
    documents = _prepared_documents(request)
    record = MssqlFailedScratchCleanupStateRecord(
        request=request,
        mssql_outcome="ROLLED_BACK",
        prepare_plan_sha256=str(documents["prepare_plan_sha256"]),
        prepare_plan_json=str(documents["prepare_plan_json"]),
        prepared_receipt_sha256=str(documents["prepared_receipt_sha256"]),
        prepared_receipt_json=str(documents["prepared_receipt_json"]),
    )

    authority = SemanticRefreshMssqlFailedScratchCleanupService(_State(record)).load(
        request.workflow_execution_binding_sha256,
        request.operation_id,
    )

    assert tuple(item.observed_uuid for item in authority.relations) == (
        "00000000-0000-0000-0000-000000000004",
        "00000000-0000-0000-0000-000000000003",
    )


def test_failed_scratch_service_rejects_caller_like_or_ambiguous_evidence() -> None:
    request = _request()
    documents = _prepared_documents(request)
    tampered = str(documents["prepare_plan_json"]).replace(request.shadow_table, request.target_table)
    record = MssqlFailedScratchCleanupStateRecord(
        request,
        "ROLLED_BACK",
        str(documents["prepare_plan_sha256"]),
        tampered,
        str(documents["prepared_receipt_sha256"]),
        str(documents["prepared_receipt_json"]),
    )
    with pytest.raises(ValueError, match="plan digest differs"):
        SemanticRefreshMssqlFailedScratchCleanupService(_State(record)).load(
            request.workflow_execution_binding_sha256,
            request.operation_id,
        )

    blocked = MssqlFailedScratchCleanupStateRecord(request, "COMMIT_UNKNOWN", None, None, None, None)
    with pytest.raises(SemanticRefreshMssqlReplacementBlocked, match="blocks"):
        SemanticRefreshMssqlFailedScratchCleanupService(_State(blocked)).load(
            request.workflow_execution_binding_sha256,
            request.operation_id,
        )


def _prepared_documents(request: MssqlFailedScratchCleanupReadRequest) -> dict[str, object]:
    bundle = _bundle()
    operation = bundle.operation_plans[0]
    resource = bundle.model_resources[0]
    business_columns = tuple(item.name for item in resource.writable_columns)
    effective_keys = tuple(
        item.name
        for item in resource.writable_columns
        if item.writable_role in {"EFFECTIVE_KEY", "EFFECTIVE_KEY_EVENT_TIME"}
    )
    plan = ClickHousePreparePlan(
        operation_id=request.operation_id,
        operation_plan_sha256=request.operation_plan_sha256,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        workflow_execution_id=request.workflow_execution_id,
        workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
        attempt_binding_sha256=request.attempt_binding_sha256,
        fence_epoch=request.fencing_epoch,
        artifact_manifest_key="semantic-refresh/test/manifest.json",
        artifact_manifest_version="v1",
        artifact_manifest_sha256="sha256:" + "7" * 64,
        target_resource_id=resource.target_resource_id,
        target_authority_id=request.target_authority_id,
        clickhouse_cluster_authority_id=resource.clickhouse_cluster_authority_id,
        database=request.database_name,
        target_table=request.target_table,
        scope_id=resource.publication_scope_id,
        scope_start=operation.scope_start,
        scope_end=operation.scope_end,
        scope_revision=operation.scope_revision,
        event_time_column=next(
            item.name for item in resource.writable_columns if item.writable_role == "EFFECTIVE_KEY_EVENT_TIME"
        ),
        staging_table=request.staging_table,
        shadow_table=request.shadow_table,
        expected_target_uuid=request.target_uuid,
        expected_schema_sha256=resource.writable_schema_sha256,
        expected_physical_sha256=resource.model_definition_proof_sha256,
        business_columns=business_columns,
        effective_key_columns=effective_keys,
        max_staging_rows=resource.resource_policy.max_after_image_rows,
        max_target_scope_rows=resource.resource_policy.max_target_scope_rows,
        max_staging_bytes=resource.resource_policy.max_clickhouse_staging_bytes,
        max_shadow_bytes=resource.resource_policy.max_clickhouse_shadow_bytes,
        max_retained_backup_bytes=resource.resource_policy.max_clickhouse_retained_backup_bytes,
        max_total_transient_bytes=resource.resource_policy.max_clickhouse_total_transient_bytes,
        shadow_equation=ShadowEquation(
            retained_target_rule="TARGET_ANTI_JOIN_STAGED_EFFECTIVE_KEY",
            append_rule="APPEND_ALL_STAGING_ROWS",
        ),
        conformance=GroupedMultisetConformance(mode="BIDIRECTIONAL_GROUPED_MULTISET"),
    )
    unsigned = {
        "operation_id": request.operation_id,
        "attempt_binding_sha256": request.attempt_binding_sha256,
        "prepare_plan_sha256": plan.sha256,
        "status": "PREPARED",
        "target_uuid": request.target_uuid,
        "staging_uuid": "00000000-0000-0000-0000-000000000003",
        "shadow_uuid": "00000000-0000-0000-0000-000000000004",
        "staging_rows": 1,
        "shadow_rows": 1,
        "desired_rows": 1,
        "forward_difference_groups": 0,
        "reverse_difference_groups": 0,
        "shadow_equation": plan.shadow_equation.to_mapping(),
        "conformance_mode": plan.conformance.mode,
        "publication_mode": "EXCHANGE",
    }
    receipt = ClickHousePreparedReceipt(
        **unsigned,
        receipt_sha256=semantic_refresh_fingerprint(unsigned),
    )
    return prepared_publication_documents(plan, receipt)
