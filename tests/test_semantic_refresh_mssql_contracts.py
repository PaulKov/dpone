from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest

from dpone.adapters.semantic_refresh_mssql_schema import (
    SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION,
    MssqlSemanticRefreshSchemaMigration,
)
from dpone.adapters.semantic_refresh_mssql_state import (
    MssqlSemanticRefreshStateAdapter,
    SemanticRefreshMssqlStateConflict,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
)
from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    semantic_refresh_recovery_target_head_sha256,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionReceipt,
    MssqlAdmissionRequest,
    MssqlGuardClaim,
    MssqlImageKeyColumn,
    MssqlJournalPreparation,
    MssqlPrerequisiteAuthorityClaim,
    MssqlRuntimeAssuranceClaim,
    MssqlTargetOwnerClaim,
    MssqlWorkflowResourceBudget,
    MssqlWorkflowSuccessorClaim,
    mssql_guard_set_sha256,
    mssql_journal_set_sha256,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlAdmissionTargetHeadAuthority,
)
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_static_projection_identity_json,
    mssql_worker_pack_fingerprint,
)
from dpone.runtime.semantic_refresh_mssql_admission import SemanticRefreshMssqlAdmissionService
from tests.test_dbt_semantic_refresh_plan_compiler import _route_receipt, _runtime_assurances


def _claim(resource_id: str, epoch: int) -> MssqlGuardClaim:
    return MssqlGuardClaim(
        resource_id=resource_id,
        expected_predecessor_epoch=epoch - 1,
        fencing_epoch=epoch,
    )


def _budget() -> MssqlWorkflowResourceBudget:
    return MssqlWorkflowResourceBudget(4, 10_000, 20_000, 30_000, 60_000)


def _prerequisites() -> tuple[MssqlPrerequisiteAuthorityClaim, ...]:
    route = _route_receipt()
    assurances = _runtime_assurances()
    first = assurances[0].subject
    return (
        MssqlPrerequisiteAuthorityClaim(
            release_id=first.release_id,
            deployment_id=first.deployment_id,
            model_unique_id=first.model_unique_id,
            route_certification_receipt_sha256=route.route_certification_receipt_sha256,
            runtime_assurances=tuple(
                MssqlRuntimeAssuranceClaim(
                    item.subject.assurance_kind.value,
                    item.runtime_assurance_receipt_sha256,
                    json.dumps(
                        item.subject.to_dict(),
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                for item in assurances
            ),
        ),
    )


def _request() -> MssqlAdmissionRequest:
    prerequisites = _prerequisites()
    target = _claim("mssql://warehouse/dbo/events", 8)
    workflow_guard = _claim("workflow://daily-events", 4)
    resource_guards = (
        _claim("mssql://warehouse/dbo/dependency", 3),
        target,
    )
    journals = (
        MssqlJournalPreparation(
            model_unique_id=prerequisites[0].model_unique_id,
            operation_id="operation-1",
            operation_plan_sha256="sha256:" + "c" * 64,
            attempt_binding_sha256="sha256:" + "d" * 64,
            strategy_authority_json="{}",
            strategy_authority_sha256=mssql_strategy_authority_sha256("{}"),
            baseline_receipt_sha256="sha256:" + "f" * 64,
            baseline_kind="adopted_complete_relation_conformant",
            baseline_receipt_json="{}",
            baseline_status="COMPLETE",
            image_key_columns=(
                MssqlImageKeyColumn("event_id", "NATIVE"),
                MssqlImageKeyColumn("occurred_at", "NATIVE"),
            ),
            target_resource_id=target.resource_id,
            publication_database="analytics",
            publication_target_table="events",
            publication_scope_id="scope-live",
            target_predecessor_generation_id="sha256:" + "2" * 64,
            scope_predecessor_operation_id="sha256:" + "3" * 64,
            fencing_epoch=target.fencing_epoch,
        ),
    )
    return MssqlAdmissionRequest(
        workflow_id="workflow-2026-08-08",
        workflow_execution_id="workflow-2026-08-08",
        workflow_plan_sha256="sha256:" + "a" * 64,
        workflow_execution_binding_sha256="sha256:" + "b" * 64,
        canonical_authority_sha256="sha256:" + "1" * 64,
        canonical_authority_json="{}",
        controller_id="controller-1",
        owner_id="owner-1",
        reservation_id="reservation-1",
        resource_budget=_budget(),
        expected_guard_set_sha256=mssql_guard_set_sha256(workflow_guard, resource_guards),
        expected_journal_set_sha256=mssql_journal_set_sha256(journals),
        workflow_guard=workflow_guard,
        resource_guards=resource_guards,
        journals=journals,
        target_heads=(
            MssqlAdmissionTargetHeadAuthority(
                target_resource_id=target.resource_id,
                model_unique_id=prerequisites[0].model_unique_id,
                clickhouse_target_authority_id="clickhouse://analytics/events",
                target_generation=2,
                target_generation_id="sha256:" + "2" * 64,
                target_uuid="00000000-0000-0000-0000-000000000001",
                owner_operation_id="sha256:" + "9" * 64,
                terminal_receipt_sha256=None,
                head_authority_receipt_sha256="sha256:" + "f" * 64,
            ),
        ),
        target_owners=(
            MssqlTargetOwnerClaim(
                target_authority_id="clickhouse://analytics/events",
                model_unique_id=prerequisites[0].model_unique_id,
                deployment_id=prerequisites[0].deployment_id,
                owner_generation=1,
            ),
        ),
        prerequisite_authorities=prerequisites,
    )


def _replacement_request() -> MssqlAdmissionRequest:
    request = _request()
    journals = (
        replace(
            request.journals[0],
            replaces_failed_operation_id="sha256:" + "1" * 64,
        ),
    )
    return MssqlAdmissionRequest(
        workflow_id=request.workflow_id,
        workflow_execution_id=request.workflow_execution_id,
        workflow_plan_sha256=request.workflow_plan_sha256,
        workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
        canonical_authority_sha256=request.canonical_authority_sha256,
        canonical_authority_json=request.canonical_authority_json,
        controller_id=request.controller_id,
        owner_id=request.owner_id,
        reservation_id=request.reservation_id,
        resource_budget=request.resource_budget,
        expected_guard_set_sha256=request.expected_guard_set_sha256,
        expected_journal_set_sha256=mssql_journal_set_sha256(journals),
        workflow_guard=request.workflow_guard,
        resource_guards=request.resource_guards,
        journals=journals,
        target_heads=request.target_heads,
        target_owners=request.target_owners,
        prerequisite_authorities=request.prerequisite_authorities,
        successor_claim=MssqlWorkflowSuccessorClaim(
            predecessor_workflow_id="failed-workflow",
            predecessor_workflow_summary_sha256="sha256:" + "7" * 64,
            successor_workflow_id=request.workflow_id,
            replacement_plan_sha256="sha256:" + "e" * 64,
        ),
    )


def _replay_request() -> MssqlAdmissionRequest:
    request = _request()
    terminal_receipt = "sha256:" + "a" * 64
    head = replace(
        request.target_heads[0],
        terminal_receipt_sha256=terminal_receipt,
        head_authority_receipt_sha256=semantic_refresh_recovery_target_head_sha256(
            model_unique_id=request.target_heads[0].model_unique_id,
            clickhouse_target_authority_id=request.target_heads[0].clickhouse_target_authority_id,
            target_generation=request.target_heads[0].target_generation,
            target_generation_id=request.target_heads[0].target_generation_id,
            target_uuid=request.target_heads[0].target_uuid,
            owner_operation_id=request.target_heads[0].owner_operation_id,
            terminal_receipt_sha256=terminal_receipt,
        ),
    )
    return replace(request, target_heads=(head,))


def test_admission_request_rejects_an_incomplete_or_unsorted_guard_set() -> None:
    valid = _request()
    with pytest.raises(ValueError, match="sorted"):
        MssqlAdmissionRequest(
            workflow_id=valid.workflow_id,
            workflow_execution_id=valid.workflow_execution_id,
            workflow_plan_sha256=valid.workflow_plan_sha256,
            workflow_execution_binding_sha256=valid.workflow_execution_binding_sha256,
            canonical_authority_sha256=valid.canonical_authority_sha256,
            canonical_authority_json=valid.canonical_authority_json,
            controller_id=valid.controller_id,
            owner_id=valid.owner_id,
            reservation_id=valid.reservation_id,
            resource_budget=valid.resource_budget,
            expected_guard_set_sha256=valid.expected_guard_set_sha256,
            expected_journal_set_sha256=valid.expected_journal_set_sha256,
            workflow_guard=valid.workflow_guard,
            resource_guards=tuple(reversed(valid.resource_guards)),
            journals=valid.journals,
            target_heads=valid.target_heads,
            target_owners=valid.target_owners,
            prerequisite_authorities=valid.prerequisite_authorities,
        )

    with pytest.raises(ValueError, match="target resource"):
        MssqlAdmissionRequest(
            workflow_id=valid.workflow_id,
            workflow_execution_id=valid.workflow_execution_id,
            workflow_plan_sha256=valid.workflow_plan_sha256,
            workflow_execution_binding_sha256=valid.workflow_execution_binding_sha256,
            canonical_authority_sha256=valid.canonical_authority_sha256,
            canonical_authority_json=valid.canonical_authority_json,
            controller_id=valid.controller_id,
            owner_id=valid.owner_id,
            reservation_id=valid.reservation_id,
            resource_budget=valid.resource_budget,
            expected_guard_set_sha256=valid.expected_guard_set_sha256,
            expected_journal_set_sha256=valid.expected_journal_set_sha256,
            workflow_guard=valid.workflow_guard,
            resource_guards=(valid.resource_guards[0],),
            journals=valid.journals,
            target_heads=valid.target_heads,
            target_owners=valid.target_owners,
            prerequisite_authorities=valid.prerequisite_authorities,
        )

    with pytest.raises(ValueError, match="expected_guard_set_sha256 authority"):
        MssqlAdmissionRequest(
            workflow_id=valid.workflow_id,
            workflow_execution_id=valid.workflow_execution_id,
            workflow_plan_sha256=valid.workflow_plan_sha256,
            workflow_execution_binding_sha256=valid.workflow_execution_binding_sha256,
            canonical_authority_sha256=valid.canonical_authority_sha256,
            canonical_authority_json=valid.canonical_authority_json,
            controller_id=valid.controller_id,
            owner_id=valid.owner_id,
            reservation_id=valid.reservation_id,
            resource_budget=valid.resource_budget,
            expected_guard_set_sha256=valid.expected_guard_set_sha256,
            expected_journal_set_sha256=valid.expected_journal_set_sha256,
            workflow_guard=valid.workflow_guard,
            resource_guards=(valid.resource_guards[1],),
            journals=valid.journals,
            target_heads=valid.target_heads,
            target_owners=valid.target_owners,
            prerequisite_authorities=valid.prerequisite_authorities,
        )

    with pytest.raises(ValueError, match="expected_journal_set_sha256 authority"):
        MssqlAdmissionRequest(
            workflow_id=valid.workflow_id,
            workflow_execution_id=valid.workflow_execution_id,
            workflow_plan_sha256=valid.workflow_plan_sha256,
            workflow_execution_binding_sha256=valid.workflow_execution_binding_sha256,
            canonical_authority_sha256=valid.canonical_authority_sha256,
            canonical_authority_json=valid.canonical_authority_json,
            controller_id=valid.controller_id,
            owner_id=valid.owner_id,
            reservation_id=valid.reservation_id,
            resource_budget=valid.resource_budget,
            expected_guard_set_sha256=valid.expected_guard_set_sha256,
            expected_journal_set_sha256=valid.expected_journal_set_sha256,
            workflow_guard=valid.workflow_guard,
            resource_guards=valid.resource_guards,
            journals=(replace(valid.journals[0], attempt_binding_sha256="sha256:" + "f" * 64),),
            target_heads=valid.target_heads,
            target_owners=valid.target_owners,
            prerequisite_authorities=valid.prerequisite_authorities,
        )


def test_admission_delegates_the_complete_request_once() -> None:
    request = _request()
    receipt = MssqlAdmissionReceipt(
        workflow_id=request.workflow_id,
        reservation_id=request.reservation_id,
        guards=(request.workflow_guard, *request.resource_guards),
        preparing_operation_ids=("operation-1",),
    )

    @dataclass
    class RecordingAdmission:
        calls: list[MssqlAdmissionRequest] = field(default_factory=list)

        def admit(self, value: MssqlAdmissionRequest) -> MssqlAdmissionReceipt:
            self.calls.append(value)
            return receipt

    port = RecordingAdmission()
    assert SemanticRefreshMssqlAdmissionService(port).admit(request) == receipt
    assert port.calls == [request]


class _Cursor:
    def __init__(
        self,
        *,
        conflict_resource: str | None = None,
        owner_conflict: bool = False,
        baseline_conflict: bool = False,
        authority_conflict: bool = False,
        route_document_conflict: bool = False,
        activation_receipt_conflict: bool = False,
        missing_exact_activation_receipt: bool = False,
        uppercase_exact_activation_deployment: bool = False,
        malformed_activation_status: bool = False,
        tampered_activation_receipt_json: bool = False,
        tampered_activation_persisted_at: bool = False,
        projection_workflow_plan_sha256: str | None = None,
        target_generation: int = 2,
        terminal_rows: tuple[tuple[object, ...], ...] = (),
    ) -> None:
        self.conflict_resource = conflict_resource
        self.owner_conflict = owner_conflict
        self.baseline_conflict = baseline_conflict
        self.authority_conflict = authority_conflict
        self.route_document_conflict = route_document_conflict
        self.activation_receipt_conflict = activation_receipt_conflict
        self.missing_exact_activation_receipt = missing_exact_activation_receipt
        self.uppercase_exact_activation_deployment = uppercase_exact_activation_deployment
        self.malformed_activation_status = malformed_activation_status
        self.tampered_activation_receipt_json = tampered_activation_receipt_json
        self.tampered_activation_persisted_at = tampered_activation_persisted_at
        self.projection_workflow_plan_sha256 = projection_workflow_plan_sha256
        self.target_generation = target_generation
        self.terminal_rows = terminal_rows
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._fetchone: tuple[object, ...] | None = None
        self._fetchall: tuple[tuple[object, ...], ...] = ()

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        values = tuple(parameters)
        self.executions.append((sql, values))
        self._fetchone = None
        self._fetchall = ()
        if "sp_getapplock" in sql:
            self._fetchone = (0,)
        elif "FROM [dpone_control].[semantic_refresh_canonical_authorities]" in sql:
            self._fetchone = (
                "workflow-2026-08-08",
                "sha256:" + ("0" if self.authority_conflict else "1") * 64,
                "{}",
                "ACTIVE",
            )
        elif "FROM [dpone_control].[semantic_refresh_activated_packs]" in sql:
            self._fetchall = (
                _activated_pack_row(
                    projection_workflow_plan_sha256=self.projection_workflow_plan_sha256,
                ),
            )
        elif "FROM [dpone_control].[semantic_refresh_activation_authorities]" in sql:
            if "plan_bundle_sha256 COLLATE" in sql:
                if self.missing_exact_activation_receipt:
                    self._fetchall = ()
                elif "SELECT deployment_id," in sql:
                    row = list(_activation_receipt_row())
                    if self.tampered_activation_receipt_json:
                        value = json.loads(str(row[4]))
                        value["authority_store_ref"] = "mssql-control://tampered"
                        row[4] = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
                    if self.tampered_activation_persisted_at:
                        row[6] = "2026-08-08T00:00:01Z"
                    exact = tuple(row)
                    self._fetchall = (
                        (str(exact[0]).upper(), *exact[1:]) if self.uppercase_exact_activation_deployment else exact,
                    )
                else:
                    self._fetchall = (_activation_receipt_row(),)
            else:
                conflict = self.activation_receipt_conflict or (
                    self.malformed_activation_status and "status COLLATE Latin1_General_100_BIN2" in sql
                )
                self._fetchone = (2, 1) if conflict else (2, 2)
        elif "FROM [dpone_control].[semantic_refresh_route_authorities]" in sql:
            receipt = _route_receipt()
            self._fetchone = (
                receipt.route_certification_receipt_sha256,
                (
                    "{}"
                    if self.route_document_conflict
                    else json.dumps(
                        receipt.to_dict(),
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                ),
                "PASS",
            )
        elif "FROM [dpone_control].[semantic_refresh_runtime_assurance_authorities]" in sql:
            receipt = next(item for item in _runtime_assurances() if item.subject.assurance_kind.value == values[2])
            self._fetchone = (
                receipt.runtime_assurance_receipt_sha256,
                str(values[4]),
                json.dumps(receipt.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                "CERTIFIED",
            )
        elif "FROM [dpone_control].[semantic_refresh_baselines]" in sql:
            self._fetchone = (
                "model.other.events" if self.baseline_conflict else _prerequisites()[0].model_unique_id,
                "adopted_complete_relation_conformant",
                "sha256:" + "f" * 64,
                "{}",
                "COMPLETE",
                True,
            )
        elif "FROM [dpone_control].[semantic_refresh_target_owners]" in sql:
            self._fetchone = (
                "model.other.events" if self.owner_conflict else _prerequisites()[0].model_unique_id,
                _prerequisites()[0].deployment_id,
                1,
                "ACTIVE",
            )
        elif "FROM [dpone_control].[semantic_refresh_target_heads]" in sql:
            self._fetchone = (
                self.target_generation,
                "sha256:" + "2" * 64,
                "00000000-0000-0000-0000-000000000001",
                "sha256:" + "9" * 64,
            )
        elif "SELECT TOP (2) operation_id" in sql:
            self._fetchall = self.terminal_rows
        elif "FROM [dpone_control].[semantic_refresh_scope_heads]" in sql:
            self._fetchone = (3, "sha256:" + "3" * 64)
        elif "FROM [dpone_control].[semantic_refresh_checkpoints]" in sql:
            self._fetchone = ("sha256:" + "4" * 64, "sha256:" + "3" * 64, 5)
        elif "SUM(CASE WHEN ack.status" in sql:
            self._fetchone = (1, 1)
        elif "OUTPUT inserted.successor_workflow_id" in sql:
            self._fetchone = (values[0], values[1], values[3])
        elif "UPDATE [dpone_control].[semantic_refresh_guards]" in sql:
            resource_id = str(values[-2])
            self._fetchone = None if resource_id == self.conflict_resource else (resource_id,)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._fetchone

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._fetchall)

    def close(self) -> None:
        return None


def _projection_identity() -> MssqlStaticProjectionIdentity:
    request = _request()
    prerequisite = request.prerequisite_authorities[0]
    return MssqlStaticProjectionIdentity(
        dag_projection_sha256="sha256:" + "1" * 64,
        deployment_id=prerequisite.deployment_id,
        package_artifacts_sha256="sha256:" + "2" * 64,
        plan_bundle_sha256="sha256:" + "3" * 64,
        pre_release_bundle_sha256="sha256:" + "4" * 64,
        release_id=prerequisite.release_id,
        template_pack_fingerprint="sha256:" + "5" * 64,
        topology_sha256="sha256:" + "6" * 64,
        workflow_plan_sha256=request.workflow_plan_sha256,
    )


def _activated_pack_row(
    *,
    projection_workflow_plan_sha256: str | None = None,
) -> tuple[object, ...]:
    request = _request()
    projection = _projection_identity()
    if projection_workflow_plan_sha256 is not None:
        projection = replace(
            projection,
            workflow_plan_sha256=projection_workflow_plan_sha256,
        )
    activation_receipt = _activation_receipt()
    store_ref = "mssql-control://test/activation"
    run_execution = "sha256:" + "8" * 64
    guard_closure = "sha256:" + "9" * 64
    return (
        mssql_worker_pack_fingerprint(
            projection_identity=projection,
            run_execution_bundle_sha256=run_execution,
            activation_authority_receipt_sha256=(activation_receipt.activation_authority_receipt_sha256),
            authority_store_ref=store_ref,
            run_guard_closure_sha256=guard_closure,
        ),
        activation_receipt.activation_authority_receipt_sha256,
        store_ref,
        request.workflow_execution_id,
        request.workflow_plan_sha256,
        projection.plan_bundle_sha256,
        run_execution,
        guard_closure,
        mssql_static_projection_identity_json(projection),
        "ACTIVE",
    )


def _activation_receipt() -> SemanticRefreshActivationAuthorityReceipt:
    request = _request()
    projection = _projection_identity()
    prerequisite = request.prerequisite_authorities[0]
    return SemanticRefreshActivationAuthorityReceipt.build(
        release_id=projection.release_id,
        deployment_id=projection.deployment_id,
        plan_bundle_sha256=projection.plan_bundle_sha256,
        authority_store_ref="mssql-control://test/activation",
        baseline_receipts=((prerequisite.model_unique_id, request.journals[0].baseline_receipt_sha256),),
        route_certification_receipt_sha256=prerequisite.route_certification_receipt_sha256,
        runtime_assurance_receipts=tuple(
            sorted(
                (prerequisite.model_unique_id, item.assurance_kind, item.receipt_sha256)
                for item in prerequisite.runtime_assurances
            )
        ),
        persisted_at="2026-08-08T00:00:00Z",
    )


def _activation_receipt_row() -> tuple[object, ...]:
    pack = _activated_pack_row()
    projection = _projection_identity()
    receipt = _activation_receipt()
    return (
        projection.deployment_id,
        projection.release_id,
        projection.plan_bundle_sha256,
        pack[2],
        json.dumps(receipt.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        receipt.activation_authority_receipt_sha256,
        receipt.persisted_at,
        "ACTIVE",
    )


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.autocommit = True
        self.cursor_instance = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_state_adapter_acquires_every_guard_and_prepares_every_journal_atomically() -> None:
    request = _request()
    connection = _Connection(_Cursor())
    adapter = MssqlSemanticRefreshStateAdapter(lambda: connection)

    receipt = adapter.admit(request)

    executed = connection.cursor_instance.executions
    locked_resources = [
        str(parameters[0]).removeprefix("dpone:semantic-refresh:")
        for sql, parameters in executed
        if "sp_getapplock" in sql
    ]
    assert locked_resources == sorted(
        [request.workflow_guard.resource_id, *(item.resource_id for item in request.resource_guards)]
    )
    assert sum("semantic_refresh_journals" in sql for sql, _ in executed) == len(request.journals)
    activation_reads = [
        (sql, parameters) for sql, parameters in executed if "semantic_refresh_activation_authorities" in sql
    ]
    assert len(activation_reads) == len(request.prerequisite_authorities) + 1
    exact_receipt_reads = [item for item in activation_reads if "plan_bundle_sha256 COLLATE" in item[0]]
    deployment_reads = [item for item in activation_reads if "COUNT_BIG(*)" in item[0]]
    assert len(exact_receipt_reads) == 1
    assert len(deployment_reads) == len(request.prerequisite_authorities)
    assert deployment_reads[0][1] == (
        request.prerequisite_authorities[0].deployment_id,
        request.prerequisite_authorities[0].release_id,
        request.prerequisite_authorities[0].deployment_id,
    )
    assert connection.autocommit is False
    assert not any("BEGIN TRANSACTION" in sql for sql, _ in executed)
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed is True
    assert receipt.preparing_operation_ids == ("operation-1",)


def test_replacement_successor_cas_is_in_the_same_admission_transaction() -> None:
    request = _replacement_request()
    connection = _Connection(_Cursor())

    MssqlSemanticRefreshStateAdapter(lambda: connection).admit(request)

    executed = connection.cursor_instance.executions
    successor_updates = [
        sql for sql, _ in executed if "successor_workflow_id" in sql and sql.lstrip().startswith("UPDATE")
    ]
    assert len(successor_updates) == 1
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_state_adapter_rolls_back_the_whole_admission_on_guard_conflict() -> None:
    request = _request()
    conflict = request.resource_guards[1].resource_id
    connection = _Connection(_Cursor(conflict_resource=conflict))
    adapter = MssqlSemanticRefreshStateAdapter(lambda: connection)

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="guard admission conflict"):
        adapter.admit(request)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("semantic_refresh_journals" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_blocks_before_guards_when_target_owner_differs() -> None:
    request = _request()
    connection = _Connection(_Cursor(owner_conflict=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="target owner"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(request)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rechecks_replay_head_terminal_receipt_before_guards() -> None:
    request = _replay_request()
    head = request.target_heads[0]
    connection = _Connection(
        _Cursor(
            terminal_rows=(
                (
                    head.owner_operation_id,
                    "COMPLETE",
                    head.terminal_receipt_sha256,
                    head.target_generation,
                    head.target_generation_id,
                    head.target_uuid,
                    "TARGET_COMMITTED",
                    "sha256:" + "8" * 64,
                ),
            )
        )
    )

    MssqlSemanticRefreshStateAdapter(lambda: connection).admit(request)

    assert connection.commits == 1
    assert any("SELECT TOP (2) operation_id" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rejects_stale_replay_head_before_guards() -> None:
    request = _replay_request()
    connection = _Connection(_Cursor(target_generation=3))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="target predecessor head"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(request)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_replay_head_projection_rejects_tampered_receipt_digest() -> None:
    head = _replay_request().target_heads[0]

    with pytest.raises(ValueError, match="receipt differs"):
        replace(head, head_authority_receipt_sha256="sha256:" + "0" * 64)


def test_state_adapter_rechecks_active_canonical_authority_before_guards() -> None:
    connection = _Connection(_Cursor(authority_conflict=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="canonical admission authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_blocks_before_dml_when_current_baseline_differs() -> None:
    request = _request()
    connection = _Connection(_Cursor(baseline_conflict=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="baseline authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(request)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_recomputes_protected_route_receipt_before_guards() -> None:
    connection = _Connection(_Cursor(route_document_conflict=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="route certification document"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rejects_mixed_deployment_activation_receipts_before_guards() -> None:
    connection = _Connection(_Cursor(activation_receipt_conflict=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="activation authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rejects_missing_exact_plan_activation_receipt_before_guards() -> None:
    connection = _Connection(_Cursor(missing_exact_activation_receipt=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="activation authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rejects_collation_equivalent_activation_identity_before_guards() -> None:
    connection = _Connection(_Cursor(uppercase_exact_activation_deployment=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="activation authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rejects_collation_equivalent_activation_status_before_guards() -> None:
    connection = _Connection(_Cursor(malformed_activation_status=True))

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="activation authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(tampered_activation_receipt_json=True),
        _Cursor(tampered_activation_persisted_at=True),
    ],
    ids=("receipt-json", "persisted-at"),
)
def test_state_adapter_rejects_tampered_activation_receipt_document_before_guards(
    cursor: _Cursor,
) -> None:
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="activation authority"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_state_adapter_rejects_cross_wired_worker_projection_before_guards() -> None:
    connection = _Connection(
        _Cursor(projection_workflow_plan_sha256="sha256:" + "f" * 64),
    )

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="identity"):
        MssqlSemanticRefreshStateAdapter(lambda: connection).admit(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not any("sp_getapplock" in sql for sql, _ in connection.cursor_instance.executions)


def test_platform_macro_orders_the_transactional_authority_steps() -> None:
    macro_path = Path("packages/dbt-dpone/macros/semantic_refresh_scope_merge.sql")
    source = macro_path.read_text(encoding="utf-8")

    ordered_tokens = (
        "@@TRANCOUNT",
        "sp_getapplock",
        "WITH (UPDLOCK, HOLDLOCK)",
        "DPONE_BEFORE_IMAGE",
        "DPONE_SOURCE_GATES",
        "DPONE_TARGET_PRE_GATES",
        "UPDATE target",
        "INSERT INTO",
        "DPONE_TARGET_GATES",
        "DPONE_AFTER_IMAGE",
        "DPONE_BUILD_RECEIPT",
    )
    positions = [source.index(token) for token in ordered_tokens]
    assert positions == sorted(positions)
    assert "MERGE " not in source.upper()
    assert "DELETE " not in source.upper()
    assert "COMMIT" not in source.upper()
    assert "BEGIN TRANSACTION" not in source.upper()
    assert "dpone_semantic_refresh_scope_map" in source
    assert "model.unique_id" in source
    assert "DPONE_SEMANTIC_REFRESH_EVENT_TIME_KEY_REQUIRED" in source
    assert "DPONE_SEMANTIC_REFRESH_DDL_EPOCH_DRIFT" in source
    assert "@LockMode = N'Shared'" in source
    assert "@dpone_attempt_context_info varbinary(128) = HASHBYTES" in source
    assert "SET CONTEXT_INFO @dpone_attempt_context_info" in source
    assert "dpone_semantic_refresh_statement_timeout_seconds" in source
    assert "SET LOCK_TIMEOUT" in source


def test_platform_macro_gates_rows_and_bytes_before_json_allocation() -> None:
    source = Path("packages/dbt-dpone/macros/semantic_refresh_scope_merge.sql").read_text(encoding="utf-8")
    restore = Path("packages/dbt-dpone/macros/semantic_refresh_restore.sql").read_text(encoding="utf-8")

    assert "@dpone_dbt_temp_json" not in source
    assert "@dpone_current_target_json" not in source
    assert source.count("67108864") >= 8
    assert source.index("SELECT @dpone_dbt_temp_rows") < source.index("SELECT @dpone_dbt_temp_bytes")
    assert source.index("@dpone_reconciled_before_preflight_bytes") < source.index(
        "SELECT @dpone_reconciled_before_json"
    )
    assert source.index("@dpone_before_image_bytes = COALESCE") < source.index("SELECT @dpone_before_json")
    assert source.index("@dpone_after_image_bytes = COALESCE") < source.index("SELECT @dpone_after_json")
    assert "COUNT_BIG(*) AS dpone_multiplicity" in source
    assert restore.index("@dpone_restore_current_preflight_bytes") < restore.index("SELECT @dpone_current_scope_json")
    assert restore.index("@dpone_restored_scope_preflight_bytes") < restore.index("SELECT @dpone_restored_scope_json")


def test_control_schema_contains_protected_authority_baseline_and_evidence_state() -> None:
    migration = MssqlSemanticRefreshSchemaMigration(lambda: None)  # type: ignore[arg-type]
    ddl = migration.render()

    assert SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION >= 17
    assert "semantic_refresh_canonical_authorities" in ddl
    assert "semantic_refresh_activated_packs" in ddl
    assert "semantic_refresh_failed_cleanup_acks" in ddl
    assert "workflow_execution_binding_sha256 varchar(71) NOT NULL UNIQUE" in ddl
    assert "run_execution_bundle_sha256 varchar(71) NOT NULL" in ddl
    assert "semantic_refresh_baselines" in ddl
    assert "semantic_refresh_mssql_session_outcomes" in ddl
    assert "baseline_receipt_sha256" in ddl
    assert "replaces_failed_operation_id" in ddl
    assert SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION >= 22
    assert "semantic_refresh_attempt_continuations" in ddl
    assert "semantic_refresh_ddl_epoch" in ddl
    assert "dpone_semantic_refresh_ddl_epoch_guard" in ddl
    assert "@LockMode = N'Exclusive'" in ddl
    assert "@Resource = N'dpone:semantic-refresh:ddl-freeze'" in ddl
    assert "DPONE_SEMANTIC_REFRESH_DDL_FREEZE_ACTIVE" in ddl
    assert "%[_][_]dbt[_]tmp" in ddl


def test_workspace_occurrences_share_the_semantic_refresh_guard_namespace() -> None:
    ddl = MssqlSemanticRefreshSchemaMigration(lambda: None).render()  # type: ignore[arg-type]

    assert SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION >= 23
    assert "dbt_workspace_activations" in ddl
    assert "dbt_workspace_activation_guards" in ddl
    assert "semantic_refresh_guards" in ddl
    assert "PRIMARY KEY (activation_id, guard_id)" in ddl
    assert "CHECK (fencing_epoch > 0)" in ddl
    assert "dbt_workspace_guards" not in ddl


def test_workspace_attempts_fence_write_subjects_and_persist_terminal_state() -> None:
    ddl = MssqlSemanticRefreshSchemaMigration(lambda: None).render()  # type: ignore[arg-type]

    assert SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION >= 24
    assert "dbt_workspace_activation_write_subjects" in ddl
    assert "dbt_workspace_attempts" in ddl
    assert "dbt_workspace_attempt_guards" in ddl
    assert "terminal_receipt_sha256" in ddl
    assert "state IN (N'RUNNING', N'SUCCEEDED', N'FAILED', N'COMMIT_UNKNOWN')" in ddl


def test_activation_authority_schema_allows_create_only_plan_successors_per_deployment() -> None:
    ddl = MssqlSemanticRefreshSchemaMigration(lambda: None).render()  # type: ignore[arg-type]

    assert SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION >= 21
    assert "PRIMARY KEY (deployment_id, plan_bundle_sha256)" in ddl
    assert "DROP CONSTRAINT" in ddl
    assert "DECLARE @dpone_drop_activation_authority_pk_sql nvarchar(max)" in ddl
    assert "EXEC sys.sp_executesql @dpone_drop_activation_authority_pk_sql" in ddl
    assert "EXEC(N'ALTER TABLE" not in ddl
    assert "key_ordinal = 1" in ddl
    assert "key_ordinal = 2" in ddl


def test_guard_is_locked_by_primary_key_before_expected_identity_is_compared() -> None:
    source = Path("packages/dbt-dpone/macros/semantic_refresh_scope_merge.sql").read_text(encoding="utf-8")
    primary_key_lookup = source.index("WHERE resource_id =")
    identity_comparison = source.index("ISNULL(@dpone_actual_attempt_binding_sha256, '') COLLATE")
    assert primary_key_lookup < identity_comparison


def test_existing_admission_replay_normalizes_the_persisted_predecessor_uuid() -> None:
    source = Path("src/dpone/adapters/semantic_refresh_mssql_admission_journals.py").read_text(encoding="utf-8")

    assert "LOWER(CONVERT(char(36), predecessor_target_uuid))" in source


def test_mssql_runtime_compatibility_modules_do_not_depend_on_contract_documents() -> None:
    violations: list[str] = []
    for path in sorted(Path("src/dpone/runtime").glob("semantic_refresh_mssql_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("dpone.contracts"):
                violations.append(f"{path}:{node.lineno}:{node.module}")
    assert violations == []
