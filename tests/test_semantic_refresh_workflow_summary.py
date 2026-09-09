from __future__ import annotations

import json
from dataclasses import replace

import pytest

from dpone.adapters.semantic_refresh_mssql_workflow_summary import (
    MssqlSemanticRefreshWorkflowSummaryState,
    SemanticRefreshWorkflowSummaryError,
    validate_workflow_summary_payload,
)
from dpone.contracts.semantic_refresh_workflow_summary import (
    SemanticRefreshDurableModelPublication,
    SemanticRefreshDurableWorkflowSummary,
)
from dpone.ports.semantic_refresh_clickhouse_resources import (
    semantic_refresh_publication_resource_allocations,
)
from dpone.ports.semantic_refresh_mssql_primitives import (
    MssqlGuardClaim,
    mssql_guard_set_sha256,
)
from tests.test_semantic_refresh_mssql_authority import _bundle, _record


class _Cursor:
    def __init__(
        self,
        *,
        dependency_guard: bool = False,
        extra_guard: bool = False,
        missing_guard: bool = False,
        empty_scope: bool = False,
        checkpoint_field: str | None = None,
        scope_field: str | None = None,
        canonical_digest_conflict: bool = False,
        reserved_resources: bool = False,
        allocation_fault: str | None = None,
        reservation_fault: str | None = None,
        complete_replay: bool = False,
        successor_advanced: bool = False,
    ) -> None:
        bundle = _bundle_with_dependency() if dependency_guard else _bundle()
        summary = _summary(bundle)
        authority = _record(bundle)
        publication = summary["publications"][0]  # type: ignore[index]
        operation_id = publication["operation_id"]
        predecessor_operation_id = "sha256:" + "f" * 64
        target_owner = predecessor_operation_id if empty_scope else operation_id
        target_mutation_outcome = "NOT_REQUIRED_EMPTY_SCOPE" if empty_scope else "TARGET_COMMITTED"
        target_uuid = "00000000-0000-0000-0000-000000000001"
        budget = bundle.resource_budget
        reservation = [
            bundle.reservation_id,
            "workflow-internal",
            bundle.execution_binding.workflow_execution_binding_sha256,
            budget.max_workflow_prepared_models,
            budget.max_workflow_sealed_extract_bytes,
            budget.max_workflow_clickhouse_staging_bytes,
            budget.max_workflow_shadow_bytes,
            budget.max_workflow_peak_bytes,
            1 if reserved_resources else 0,
            0,
            0,
            0,
            0,
            0,
            "COMPLETE" if complete_replay else "PREPARING",
        ]
        if reservation_fault == "workflow":
            reservation[1] = "other-workflow"
        elif reservation_fault == "binding":
            reservation[2] = "sha256:" + "0" * 64
        elif reservation_fault == "maximum":
            reservation[3] = int(reservation[3]) + 1
        execution = (
            "workflow-internal",
            bundle.workflow_execution_id,
            bundle.workflow_plan.workflow_plan_sha256,
            bundle.execution_binding.workflow_execution_binding_sha256,
            summary["terminal_summary_sha256"] if complete_replay else None,
            "COMPLETE" if complete_replay else "PREPARING",
            bundle.workflow_guard.resource_id,
            len(bundle.resource_guards) + 1,
            (
                json.dumps(summary, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
                if complete_replay
                else None
            ),
            ("sha256:" + "0" * 64) if canonical_digest_conflict else bundle.authority_sha256,
            mssql_guard_set_sha256(bundle.workflow_guard, bundle.resource_guards),
        )
        self._one = [
            (0,),
            execution,
            (
                authority.workflow_execution_binding_sha256,
                authority.workflow_execution_id,
                authority.authority_sha256,
                authority.authority_json,
                authority.status,
            ),
            tuple(reservation),
        ]
        if complete_replay:
            self._one.extend(((0,), (1,)))
        held = [(bundle.workflow_guard.resource_id,), *((item.resource_id,) for item in bundle.resource_guards)]
        if missing_guard:
            held.pop(0)
        if extra_guard:
            held.append(("guard://unexpected",))
        checkpoint = {
            "digest": "sha256:" + "7" * 64,
            "version": 2,
            "owner": operation_id,
        }
        if checkpoint_field == "missing":
            checkpoint = {"digest": None, "version": None, "owner": None}
        elif checkpoint_field is not None:
            checkpoint[checkpoint_field] = (
                "sha256:" + "0" * 64
                if checkpoint_field == "digest"
                else (99 if checkpoint_field == "version" else predecessor_operation_id)
            )
        scope_revision: object = publication["scope_revision"]
        scope_owner: object = operation_id
        if scope_field == "revision":
            scope_revision = 99
        elif scope_field == "owner":
            scope_owner = predecessor_operation_id
        allocations = list(_resource_allocation_rows(bundle))
        if allocation_fault == "zero":
            allocations = []
        elif allocation_fault == "missing":
            allocations.pop()
        elif allocation_fault == "extra":
            allocations.append(("sha256:" + "0" * 64, bundle.reservation_id, "prepared_models", 1, "RELEASED"))
        elif allocation_fault in {"amount", "status"}:
            changed = list(allocations[0])
            changed[3 if allocation_fault == "amount" else 4] = 99 if allocation_fault == "amount" else "RESERVED"
            allocations[0] = tuple(changed)
        durable_row = (
            publication["operation_id"],
            publication["operation_plan_sha256"],
            publication["attempt_binding_sha256"],
            "COMPLETE",
            publication["artifact_manifest_sha256"],
            publication["clickhouse_terminal_receipt_sha256"],
            publication["terminal_receipt_sha256"],
            publication["target_generation"],
            publication["scope_revision"],
            bundle.resource_guards[0].resource_id,
            publication["target_generation"],
            "sha256:" + "6" * 64,
            target_uuid.upper(),
            target_owner,
            "sha256:" + "6" * 64,
            target_uuid,
            target_mutation_outcome,
            predecessor_operation_id,
            "sha256:" + "7" * 64,
            2,
            checkpoint["digest"],
            checkpoint["version"],
            checkpoint["owner"],
            scope_revision,
            scope_owner,
        )
        if successor_advanced:
            durable_row = (
                *durable_row[:10],
                int(publication["target_generation"]) + 1,
                "sha256:" + "9" * 64,
                "00000000-0000-0000-0000-000000000009",
                "sha256:" + "9" * 64,
                *durable_row[14:20],
                "sha256:" + "9" * 64,
                3,
                "sha256:" + "9" * 64,
                int(publication["scope_revision"]) + 1,
                "sha256:" + "9" * 64,
            )
        journal_rows = (
            [
                (
                    *durable_row[:10],
                    durable_row[14],
                    durable_row[15],
                    durable_row[16],
                    durable_row[17],
                    durable_row[18],
                    durable_row[19],
                )
            ]
            if complete_replay
            else [durable_row]
        )
        self._many = [
            journal_rows,
            allocations,
            sorted(held),
        ]
        if scope_field == "missing":
            self._many[0] = []
        self.guard_count = len(held)
        self._held = sorted(held)
        self._output_rows: list[tuple[object, ...]] | None = None
        self.executions: list[str] = []

    def execute(self, sql: str, *_parameters: object) -> _Cursor:
        self.executions.append(sql)
        if "OUTPUT inserted.workflow_id" in sql:
            self._output_rows = [("workflow-internal",)]
        elif "OUTPUT inserted.resource_id" in sql:
            self._output_rows = list(self._held)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one.pop(0)

    def fetchall(self) -> list[tuple[object, ...]]:
        if self._output_rows is not None:
            rows = self._output_rows
            self._output_rows = None
            return rows
        return self._many.pop(0)

    def close(self) -> None:
        return None


class _Connection:
    autocommit = True

    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _Cursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        return None


def _bundle_with_dependency():
    bundle = _bundle()
    dependency = MssqlGuardClaim("mssql://dependency/customer", 0, 1)
    return replace(
        bundle,
        resource_guards=tuple(sorted((*bundle.resource_guards, dependency), key=lambda item: item.resource_id)),
    )


def _summary(bundle=None) -> dict[str, object]:
    bundle = _bundle() if bundle is None else bundle
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    return SemanticRefreshDurableWorkflowSummary.build(
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        expected_operation_ids=(operation.operation_id,),
        publications=(
            SemanticRefreshDurableModelPublication(
                operation_id=operation.operation_id,
                operation_plan_sha256=operation.operation_plan_sha256,
                workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                attempt_binding_sha256=attempt.attempt_binding_sha256,
                artifact_manifest_sha256="sha256:" + "1" * 64,
                clickhouse_terminal_receipt_sha256="sha256:" + "2" * 64,
                terminal_receipt_sha256="sha256:" + "d" * 64,
                target_generation=2,
                scope_revision=1,
            ),
        ),
    ).to_dict()


def _resource_allocation_rows(bundle) -> tuple[tuple[object, ...], ...]:
    resources = {item.model_unique_id: item for item in bundle.model_resources}
    allocations = []
    for operation in bundle.operation_plans:
        resource = resources[operation.model_unique_id]
        allocations.extend(
            semantic_refresh_publication_resource_allocations(
                reservation_id=bundle.reservation_id,
                operation_id=operation.operation_id,
                sealed_extract_bytes=resource.artifact_authority.max_artifact_bytes,
                clickhouse_staging_bytes=resource.resource_policy.max_clickhouse_staging_bytes,
                shadow_bytes=resource.resource_policy.max_clickhouse_shadow_bytes,
                retained_generation_bytes=resource.resource_policy.max_clickhouse_retained_backup_bytes,
            )
        )
    return tuple(
        (
            item.allocation_id,
            item.reservation_id,
            item.resource_kind,
            item.amount,
            "RELEASED",
        )
        for item in sorted(allocations)
    )


def test_workflow_summary_validation_accepts_exact_terminal_closure() -> None:
    assert validate_workflow_summary_payload(_summary()) == _summary()


def test_workflow_summary_validation_rejects_partial_or_tampered_publication() -> None:
    summary = _summary()
    publications = list(summary["publications"])  # type: ignore[arg-type]
    publications[0] = {**publications[0], "status": "COMMITTED_INCOMPLETE"}
    summary["publications"] = publications

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="invalid"):
        validate_workflow_summary_payload(summary)


def test_workflow_summary_rejects_free_workflow_locator_and_noncanonical_operation_id() -> None:
    summary = _summary()
    summary["workflow_id"] = summary.pop("workflow_execution_id")
    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="invalid"):
        validate_workflow_summary_payload(summary)

    summary = _summary()
    publications = list(summary["publications"])  # type: ignore[arg-type]
    publications[0] = {**publications[0], "operation_id": "model.orders"}
    summary["publications"] = publications
    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="invalid"):
        validate_workflow_summary_payload(summary)


def test_workflow_summary_releases_only_exact_protected_guard_set() -> None:
    connection = _Connection(_Cursor())
    acknowledgement = MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert acknowledgement["persisted"] is True
    assert connection.committed is True
    assert connection.rolled_back is False


def test_workflow_summary_releases_exact_dependency_guard_from_canonical_authority() -> None:
    bundle = _bundle_with_dependency()
    connection = _Connection(_Cursor(dependency_guard=True))

    acknowledgement = MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary(bundle))

    assert acknowledgement["persisted"] is True
    assert connection.committed is True


def test_workflow_summary_rejects_unexpected_extra_guard() -> None:
    connection = _Connection(_Cursor(extra_guard=True))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="guard closure differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_rejects_missing_canonical_dependency_guard() -> None:
    bundle = _bundle_with_dependency()
    connection = _Connection(_Cursor(dependency_guard=True, missing_guard=True))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="guard"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary(bundle))

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_rejects_tampered_canonical_guard_digest() -> None:
    connection = _Connection(_Cursor(canonical_digest_conflict=True))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="guard"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_replay_rejects_durable_terminal_field_mismatch() -> None:
    summary = _summary()
    cursor = _Cursor()
    row = list(cursor._many[0][0])
    row[5] = "sha256:" + "9" * 64
    cursor._many[0][0] = tuple(row)
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="terminal journal closure differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(summary)

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_complete_replay_survives_legitimate_successor_head_advance() -> None:
    cursor = _Cursor(complete_replay=True, successor_advanced=True)
    connection = _Connection(cursor)

    acknowledgement = MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert acknowledgement == {
        "persisted": True,
        "status": "FULLY_COMPLETE",
        "terminal_summary_sha256": _summary()["terminal_summary_sha256"],
    }
    assert connection.committed is True
    assert not any(
        table in sql
        for sql in cursor.executions
        for table in (
            "semantic_refresh_target_heads",
            "semantic_refresh_scope_heads",
            "semantic_refresh_checkpoints",
        )
    )


def test_workflow_summary_complete_replay_rejects_byte_different_summary() -> None:
    original = SemanticRefreshDurableWorkflowSummary.from_mapping(_summary())
    changed = SemanticRefreshDurableWorkflowSummary.build(
        workflow_execution_id=original.workflow_execution_id,
        workflow_plan_sha256=original.workflow_plan_sha256,
        workflow_execution_binding_sha256=original.workflow_execution_binding_sha256,
        expected_operation_ids=original.expected_operation_ids,
        publications=(
            replace(
                original.publications[0],
                terminal_receipt_sha256="sha256:" + "0" * 64,
            ),
        ),
    ).to_dict()
    connection = _Connection(_Cursor(complete_replay=True))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="replay differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(changed)

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_complete_replay_rejects_terminal_journal_mismatch() -> None:
    cursor = _Cursor(complete_replay=True)
    row = list(cursor._many[0][0])
    row[5] = "sha256:" + "9" * 64
    cursor._many[0][0] = tuple(row)
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="journal closure differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


@pytest.mark.parametrize(
    ("field_index", "wrong_value"),
    [
        (10, "not-a-generation-digest"),
        (11, "not-a-uuid"),
        (12, "UNKNOWN_OUTCOME"),
        (14, "not-a-checkpoint-digest"),
        (15, 0),
    ],
)
def test_workflow_summary_complete_replay_rejects_invalid_immutable_terminal_evidence(
    field_index: int,
    wrong_value: object,
) -> None:
    cursor = _Cursor(complete_replay=True)
    row = list(cursor._many[0][0])
    row[field_index] = wrong_value
    cursor._many[0][0] = tuple(row)
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="immutable terminal evidence differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_accepts_empty_scope_predecessor_target_lineage() -> None:
    connection = _Connection(_Cursor(empty_scope=True))

    acknowledgement = MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert acknowledgement["status"] == "FULLY_COMPLETE"
    assert connection.committed is True


def test_workflow_summary_rejects_same_target_values_owned_by_wrong_operation() -> None:
    cursor = _Cursor(empty_scope=True)
    row = list(cursor._many[0][0])
    row[13] = "sha256:" + "0" * 64
    cursor._many[0][0] = tuple(row)
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="target lineage differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


@pytest.mark.parametrize("checkpoint_field", ["missing", "digest", "version", "owner"])
def test_workflow_summary_rejects_missing_or_tampered_checkpoint_lineage(checkpoint_field: str) -> None:
    connection = _Connection(_Cursor(checkpoint_field=checkpoint_field))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="checkpoint lineage differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


@pytest.mark.parametrize("scope_field", ["revision", "owner"])
def test_workflow_summary_rejects_tampered_scope_lineage(scope_field: str) -> None:
    connection = _Connection(_Cursor(scope_field=scope_field))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="scope lineage differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


def test_workflow_summary_rejects_missing_scope_head() -> None:
    connection = _Connection(_Cursor(scope_field="missing"))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="terminal journal closure differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(reserved_resources=True),
        _Cursor(allocation_fault="status"),
    ],
)
def test_workflow_summary_rejects_unreleased_workflow_resources(cursor: _Cursor) -> None:
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="resource"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


@pytest.mark.parametrize("allocation_fault", ["zero", "missing", "extra", "amount", "status"])
def test_workflow_summary_requires_exact_released_allocation_history(allocation_fault: str) -> None:
    connection = _Connection(_Cursor(allocation_fault=allocation_fault))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="allocation closure differs"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True


@pytest.mark.parametrize("reservation_fault", ["workflow", "binding", "maximum"])
def test_workflow_summary_rejects_tampered_resource_reservation_authority(reservation_fault: str) -> None:
    connection = _Connection(_Cursor(reservation_fault=reservation_fault))

    with pytest.raises(SemanticRefreshWorkflowSummaryError, match="reservation is not fully released"):
        MssqlSemanticRefreshWorkflowSummaryState(lambda: connection).persist(_summary())

    assert connection.committed is False
    assert connection.rolled_back is True
