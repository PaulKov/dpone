from __future__ import annotations

import json
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from dpone.adapters.semantic_refresh_mssql_run_authority import (
    MssqlSemanticRefreshWorkerRunAuthority,
    SemanticRefreshMssqlWorkerRunAuthorityError,
)
from dpone.adapters.semantic_refresh_mssql_run_binding import (
    MssqlSemanticRefreshWorkerRunBindingAuthority,
    SemanticRefreshMssqlWorkerRunBindingAuthorityError,
)
from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
)
from dpone.contracts.dbt_semantic_refresh_run_guard import (
    RUN_GUARD_CLOSURE_SCHEMA,
    SemanticRefreshRunGuardClosure,
)
from dpone.ports.semantic_refresh_mssql import MssqlGuardClaim
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    MssqlStaticProjectionIdentity,
    mssql_static_projection_identity_json,
    mssql_worker_pack_fingerprint,
)
from dpone.services.semantic_refresh_mssql_activation import (
    SemanticRefreshMssqlWorkerRunAuthorityService,
)
from tests.test_semantic_refresh_mssql_authority import _bundle, _record


class _Cursor:
    def __init__(
        self,
        *,
        admitted: bool,
        pack_status: str = "ACTIVE",
        execution_status: str = "PREPARING",
        journal_status: str = "PREPARING",
        guard_status: str = "HELD",
        ambiguous_pack: bool = False,
        omit_journal: bool = False,
        reservation_status: str | None = None,
        dependency_guard_status: str = "HELD",
        omit_dependency_guard: bool = False,
        partial_registered_state: str | None = None,
        projection_plan_sha256: str | None = None,
        activation_receipt_status: str = "ACTIVE",
        tampered_pack_fingerprint: bool = False,
        stored_pack_workflow_execution_id: str | None = None,
        stored_pack_workflow_plan_sha256: str | None = None,
        stored_canonical_workflow_execution_id: str | None = None,
    ) -> None:
        self.admitted = admitted
        self.pack_status = pack_status
        self.execution_status = execution_status
        self.journal_status = journal_status
        self.guard_status = guard_status
        self.ambiguous_pack = ambiguous_pack
        self.omit_journal = omit_journal
        self.reservation_status = reservation_status
        self.dependency_guard_status = dependency_guard_status
        self.omit_dependency_guard = omit_dependency_guard
        self.partial_registered_state = partial_registered_state
        self.projection_plan_sha256 = projection_plan_sha256
        self.activation_receipt_status = activation_receipt_status
        self.tampered_pack_fingerprint = tampered_pack_fingerprint
        self.stored_pack_workflow_execution_id = stored_pack_workflow_execution_id
        self.stored_pack_workflow_plan_sha256 = stored_pack_workflow_plan_sha256
        self.stored_canonical_workflow_execution_id = stored_canonical_workflow_execution_id
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._row: tuple[object, ...] | None = None
        self._rows: tuple[tuple[object, ...], ...] = ()

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        self.executions.append((sql, tuple(parameters)))
        self._row = None
        self._rows = ()
        bundle = _worker_bundle()
        request = compose_admission(bundle)
        record = _record(bundle)
        operation = bundle.operation_plans[0]
        attempt = bundle.attempt_bindings[0]
        resource = bundle.model_resources[0]
        activation_receipt = _worker_activation_receipt(bundle)
        if "semantic_refresh_activated_packs" in sql:
            closure = _worker_guard_closure(bundle)
            projection = _worker_projection(bundle)
            if self.projection_plan_sha256 is not None:
                projection = replace(
                    projection,
                    plan_bundle_sha256=self.projection_plan_sha256,
                )
            pack = (
                mssql_worker_pack_fingerprint(
                    projection_identity=projection,
                    run_execution_bundle_sha256="sha256:" + "4" * 64,
                    activation_authority_receipt_sha256=(activation_receipt.activation_authority_receipt_sha256),
                    authority_store_ref="mssql-control://test",
                    run_guard_closure_sha256=closure.run_guard_closure_sha256,
                ),
                activation_receipt.activation_authority_receipt_sha256,
                "mssql-control://test",
                record.workflow_execution_binding_sha256,
                "sha256:" + "3" * 64,
                "sha256:" + "4" * 64,
                closure.run_guard_closure_sha256,
                closure.workflow_guard_resource_id,
                json.dumps(closure.resource_guard_ids, separators=(",", ":")),
                mssql_static_projection_identity_json(projection),
                self.pack_status,
                self.stored_pack_workflow_execution_id or bundle.workflow_execution_id,
                self.stored_pack_workflow_plan_sha256 or bundle.workflow_plan.workflow_plan_sha256,
            )
            if self.tampered_pack_fingerprint:
                pack = ("sha256:" + "f" * 64, *pack[1:])
            self._rows = (pack, pack) if self.ambiguous_pack else (pack,)
        elif "semantic_refresh_activation_authorities" in sql:
            projection = _worker_projection(bundle)
            self._rows = (
                (
                    projection.deployment_id,
                    projection.release_id,
                    projection.plan_bundle_sha256,
                    "mssql-control://test",
                    json.dumps(
                        activation_receipt.to_dict(),
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    activation_receipt.activation_authority_receipt_sha256,
                    activation_receipt.persisted_at,
                    self.activation_receipt_status,
                ),
            )
        elif "semantic_refresh_canonical_authorities" in sql:
            self._rows = (
                (
                    record.workflow_execution_binding_sha256,
                    record.authority_sha256,
                    record.authority_json,
                    record.status,
                    self.stored_canonical_workflow_execution_id or bundle.workflow_execution_id,
                ),
            )
        elif "semantic_refresh_workflow_executions" in sql:
            if self.admitted:
                self._row = (
                    bundle.workflow_plan.workflow_plan_sha256,
                    bundle.execution_binding.workflow_execution_binding_sha256,
                    bundle.authority_sha256,
                    request.guard_set_sha256,
                    request.journal_set_sha256,
                    bundle.workflow_guard.resource_id,
                    len(bundle.resource_guards) + 1,
                    bundle.owner_id,
                    self.execution_status,
                )
        elif "semantic_refresh_reservations" in sql:
            status = self.reservation_status
            if status is None and self.admitted:
                status = "PREPARING"
            if status is not None:
                budget = bundle.resource_budget
                self._rows = (
                    (
                        bundle.reservation_id,
                        bundle.workflow_execution_id,
                        bundle.execution_binding.workflow_execution_binding_sha256,
                        status,
                        budget.max_workflow_prepared_models,
                        budget.max_workflow_sealed_extract_bytes,
                        budget.max_workflow_clickhouse_staging_bytes,
                        budget.max_workflow_shadow_bytes,
                        budget.max_workflow_peak_bytes,
                    ),
                )
        elif "COUNT_BIG" in sql:
            if "semantic_refresh_guards" in sql:
                count = (
                    1
                    if self.partial_registered_state == "guard"
                    else len(bundle.resource_guards) + 1
                    if self.admitted
                    else 0
                )
            else:
                count = 1 if self.partial_registered_state == "journal" else 0
            self._row = (count,)
        elif "AS j" in sql:
            if not self.omit_journal:
                self._rows = (
                    (
                        operation.model_unique_id,
                        operation.operation_id,
                        operation.operation_plan_sha256,
                        attempt.attempt_binding_sha256,
                        attempt.fencing_epoch,
                        bundle.owner_id,
                        self.journal_status,
                        resource.target_resource_id,
                        request.journals[0].strategy_authority_sha256,
                    ),
                )
        elif "semantic_refresh_guards" in sql:
            resource_id = str(parameters[0])
            claim = next(
                item for item in (bundle.workflow_guard, *bundle.resource_guards) if item.resource_id == resource_id
            )
            operation = next(
                (
                    item
                    for item in bundle.operation_plans
                    if bundle.model_resources[0].target_resource_id == resource_id
                ),
                None,
            )
            attempt = None if operation is None else bundle.attempt_bindings[0]
            if resource_id == "mssql://warehouse/dbo/dependency" and self.omit_dependency_guard:
                self._row = None
            else:
                self._row = (
                    claim.fencing_epoch,
                    bundle.owner_id,
                    bundle.workflow_execution_id,
                    None if operation is None else operation.operation_id,
                    None if operation is None else operation.operation_plan_sha256,
                    None if attempt is None else attempt.attempt_binding_sha256,
                    None if operation is None else request.journals[0].strategy_authority_sha256,
                    (
                        self.dependency_guard_status
                        if resource_id == "mssql://warehouse/dbo/dependency"
                        else self.guard_status
                    ),
                )
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self._rows

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


class _MixedJournalCursor:
    def __init__(self, rows: tuple[tuple[object, ...], ...]) -> None:
        self.rows = rows

    def execute(self, _sql: str, *_parameters: object) -> _MixedJournalCursor:
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return None

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.rows

    def close(self) -> None:
        return None


def _load(cursor: _Cursor):
    bundle = _worker_bundle()
    connection = _Connection(cursor)
    result = SemanticRefreshMssqlWorkerRunAuthorityService(
        MssqlSemanticRefreshWorkerRunAuthority(lambda: connection)
    ).load(
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
        workflow_execution_id=bundle.workflow_execution_id,
    )
    return result, connection


def _worker_bundle():
    bundle = _bundle()
    target = bundle.resource_guards[0]
    dependency = MssqlGuardClaim(
        resource_id="mssql://warehouse/dbo/dependency",
        expected_predecessor_epoch=2,
        fencing_epoch=3,
    )
    return replace(
        bundle,
        resource_guards=tuple(sorted((dependency, target), key=lambda item: item.resource_id)),
    )


def _worker_guard_closure(bundle) -> SemanticRefreshRunGuardClosure:
    resources = tuple(item.resource_id for item in bundle.resource_guards)
    payload = {
        "resource_guard_ids": list(resources),
        "schema": RUN_GUARD_CLOSURE_SCHEMA,
        "workflow_guard_resource_id": bundle.workflow_guard.resource_id,
    }
    return SemanticRefreshRunGuardClosure(
        workflow_guard_resource_id=bundle.workflow_guard.resource_id,
        resource_guard_ids=resources,
        run_guard_closure_sha256=canonical_fingerprint(payload),
    )


def _worker_projection(bundle) -> MssqlStaticProjectionIdentity:
    return MssqlStaticProjectionIdentity(
        dag_projection_sha256="sha256:" + "5" * 64,
        deployment_id="sha256:" + "6" * 64,
        package_artifacts_sha256="sha256:" + "7" * 64,
        plan_bundle_sha256="sha256:" + "3" * 64,
        pre_release_bundle_sha256="sha256:" + "8" * 64,
        release_id="sha256:" + "9" * 64,
        template_pack_fingerprint="sha256:" + "a" * 64,
        topology_sha256="sha256:" + "b" * 64,
        workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
    )


def _worker_activation_receipt(bundle) -> SemanticRefreshActivationAuthorityReceipt:
    projection = _worker_projection(bundle)
    resource = bundle.model_resources[0]
    return SemanticRefreshActivationAuthorityReceipt.build(
        release_id=projection.release_id,
        deployment_id=projection.deployment_id,
        plan_bundle_sha256=projection.plan_bundle_sha256,
        authority_store_ref="mssql-control://test",
        baseline_receipts=((resource.model_unique_id, resource.baseline_receipt_sha256),),
        route_certification_receipt_sha256=resource.route_certification_receipt_sha256,
        runtime_assurance_receipts=(
            (
                resource.model_unique_id,
                "ddl_freeze",
                resource.ddl_freeze_assurance_receipt_sha256,
            ),
            (
                resource.model_unique_id,
                "writer_exclusivity",
                resource.writer_exclusivity_assurance_receipt_sha256,
            ),
        ),
        persisted_at="2026-08-08T00:00:00Z",
    )


def test_worker_run_locator_returns_binding_without_attempts_before_admission() -> None:
    result, connection = _load(_Cursor(admitted=False))

    assert result.admission_status == "REGISTERED"
    assert result.attempts == ()
    assert result.record.workflow_execution_binding_sha256 == (
        _worker_bundle().execution_binding.workflow_execution_binding_sha256
    )
    assert connection.commits == 1
    activation_reads = [
        parameters
        for sql, parameters in connection.cursor_instance.executions
        if "semantic_refresh_activation_authorities" in sql
    ]
    projection = _worker_projection(_worker_bundle())
    assert activation_reads == [(projection.deployment_id, projection.plan_bundle_sha256)]
    assert all("WITH (UPDLOCK, HOLDLOCK)" in sql for sql, _ in connection.cursor_instance.executions if "SELECT" in sql)


def test_worker_run_locator_returns_exact_attempt_and_fence_after_admission() -> None:
    bundle = _worker_bundle()
    result, connection = _load(_Cursor(admitted=True))

    assert result.admission_status == "ADMITTED"
    assert len(result.attempts) == 1
    assert result.attempts[0].attempt_binding_sha256 == bundle.attempt_bindings[0].attempt_binding_sha256
    assert result.attempts[0].fencing_epoch == bundle.attempt_bindings[0].fencing_epoch
    assert result.attempts[0].task_id == bundle.attempt_bindings[0].task_id
    assert connection.commits == 1


def test_worker_run_locator_keeps_attempt_authority_after_model_completion() -> None:
    result, connection = _load(_Cursor(admitted=True, journal_status="COMPLETE"))

    assert result.admission_status == "ADMITTED"
    assert result.attempts[0].journal_status == "COMPLETE"
    assert connection.commits == 1


def test_worker_run_binding_locator_survives_terminal_workflow_for_summary_replay() -> None:
    bundle = _worker_bundle()
    cursor = _Cursor(admitted=True, execution_status="COMPLETE", journal_status="COMPLETE")
    connection = _Connection(cursor)

    result = MssqlSemanticRefreshWorkerRunBindingAuthority(lambda: connection).locate_binding(
        bundle.workflow_plan.workflow_plan_sha256,
        bundle.workflow_execution_id,
    )

    assert result.record.workflow_execution_binding_sha256 == (
        bundle.execution_binding.workflow_execution_binding_sha256
    )
    assert result.plan_bundle_sha256 == result.projection_identity.plan_bundle_sha256
    assert connection.commits == 1
    assert not any("semantic_refresh_workflow_executions" in sql for sql, _ in cursor.executions)


def test_worker_run_binding_locator_rejects_tampered_pack_fingerprint() -> None:
    bundle = _worker_bundle()
    cursor = _Cursor(
        admitted=True,
        execution_status="COMPLETE",
        journal_status="COMPLETE",
        tampered_pack_fingerprint=True,
    )
    connection = _Connection(cursor)

    with pytest.raises(
        SemanticRefreshMssqlWorkerRunBindingAuthorityError,
        match="identity",
    ):
        MssqlSemanticRefreshWorkerRunBindingAuthority(lambda: connection).locate_binding(
            bundle.workflow_plan.workflow_plan_sha256,
            bundle.workflow_execution_id,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(
            admitted=True,
            stored_pack_workflow_execution_id=_worker_bundle().workflow_execution_id.upper(),
        ),
        _Cursor(
            admitted=True,
            stored_pack_workflow_plan_sha256=(_worker_bundle().workflow_plan.workflow_plan_sha256.upper()),
        ),
        _Cursor(
            admitted=True,
            stored_canonical_workflow_execution_id=_worker_bundle().workflow_execution_id.upper(),
        ),
    ],
)
def test_worker_run_binding_locator_rejects_collation_equivalent_stored_identity(
    cursor: _Cursor,
) -> None:
    bundle = _worker_bundle()
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshMssqlWorkerRunBindingAuthorityError):
        MssqlSemanticRefreshWorkerRunBindingAuthority(lambda: connection).locate_binding(
            bundle.workflow_plan.workflow_plan_sha256,
            bundle.workflow_execution_id,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_worker_run_binding_locator_uses_binary_exact_locator_queries() -> None:
    bundle = _worker_bundle()
    cursor = _Cursor(admitted=True)
    connection = _Connection(cursor)

    MssqlSemanticRefreshWorkerRunBindingAuthority(lambda: connection).locate_binding(
        bundle.workflow_plan.workflow_plan_sha256,
        bundle.workflow_execution_id,
    )

    locator_queries = [
        sql
        for sql, _ in cursor.executions
        if "semantic_refresh_activated_packs" in sql or "semantic_refresh_canonical_authorities" in sql
    ]
    assert len(locator_queries) == 2
    assert all("COLLATE Latin1_General_100_BIN2" in sql for sql in locator_queries)


def test_two_model_attempt_closure_allows_commit_one_then_resolve_commit_two() -> None:
    operation_ids = ("sha256:" + "1" * 64, "sha256:" + "2" * 64)
    plan_ids = ("sha256:" + "3" * 64, "sha256:" + "4" * 64)
    attempt_ids = ("sha256:" + "5" * 64, "sha256:" + "6" * 64)
    strategy_ids = ("sha256:" + "7" * 64, "sha256:" + "8" * 64)
    models = ("model.analytics.first", "model.analytics.second")
    resources = ("mssql://first", "mssql://second")
    owner = "worker-owner"
    operations = tuple(
        SimpleNamespace(
            model_unique_id=model,
            operation_id=operation_id,
            operation_plan_sha256=plan_id,
        )
        for model, operation_id, plan_id in zip(models, operation_ids, plan_ids, strict=True)
    )
    attempts = tuple(
        SimpleNamespace(
            operation_id=operation_id,
            attempt_binding_sha256=attempt_id,
            fencing_epoch=index,
            task_id="semantic_refresh__dbt_build_test",
            try_number=1,
            pod_uid=f"00000000-0000-0000-0000-{index:012d}",
        )
        for index, (operation_id, attempt_id) in enumerate(zip(operation_ids, attempt_ids, strict=True), start=1)
    )
    model_resources = tuple(
        SimpleNamespace(model_unique_id=model, target_resource_id=resource)
        for model, resource in zip(models, resources, strict=True)
    )
    journals = tuple(
        SimpleNamespace(
            operation_id=operation_id,
            target_resource_id=resource,
            strategy_authority_sha256=strategy,
        )
        for operation_id, resource, strategy in zip(operation_ids, resources, strategy_ids, strict=True)
    )
    rows = tuple(
        (
            model,
            operation_id,
            plan_id,
            attempt_id,
            index,
            owner,
            status,
            resource,
            strategy,
        )
        for index, (model, operation_id, plan_id, attempt_id, status, resource, strategy) in enumerate(
            zip(
                models,
                operation_ids,
                plan_ids,
                attempt_ids,
                ("COMPLETE", "PREPARING"),
                resources,
                strategy_ids,
                strict=True,
            ),
            start=1,
        )
    )
    cursor = _MixedJournalCursor(rows)
    bundle = SimpleNamespace(
        workflow_execution_id="daily/two-model",
        operation_plans=operations,
        attempt_bindings=attempts,
        model_resources=model_resources,
        resource_guards=tuple(SimpleNamespace(resource_id=item) for item in resources),
        owner_id=owner,
    )
    request = SimpleNamespace(journals=journals)
    adapter = MssqlSemanticRefreshWorkerRunAuthority(lambda: None)  # type: ignore[arg-type,return-value]

    resolved = adapter._journal_attempts(cursor, bundle, request)

    assert tuple(item.operation_id for item in resolved) == operation_ids
    assert tuple(item.journal_status for item in resolved) == ("COMPLETE", "PREPARING")


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(admitted=False, pack_status="INACTIVE"),
        _Cursor(admitted=False, ambiguous_pack=True),
        _Cursor(admitted=True, execution_status="COMPLETE"),
        _Cursor(admitted=True, omit_journal=True),
        _Cursor(admitted=True, guard_status="RELEASED"),
        _Cursor(admitted=True, dependency_guard_status="RELEASED"),
        _Cursor(admitted=True, omit_dependency_guard=True),
        _Cursor(admitted=True, reservation_status="COMMITTING"),
        _Cursor(admitted=False, reservation_status="PREPARING"),
        _Cursor(admitted=False, partial_registered_state="guard"),
        _Cursor(admitted=False, partial_registered_state="journal"),
        _Cursor(admitted=True, journal_status="FAILED_PRE_COMMIT"),
        _Cursor(admitted=False, projection_plan_sha256="sha256:" + "f" * 64),
        _Cursor(admitted=False, activation_receipt_status="INACTIVE"),
    ],
)
def test_worker_run_locator_rejects_inactive_ambiguous_partial_or_terminal_state(
    cursor: _Cursor,
) -> None:
    connection = _Connection(cursor)
    bundle = _worker_bundle()

    with pytest.raises(SemanticRefreshMssqlWorkerRunAuthorityError):
        MssqlSemanticRefreshWorkerRunAuthority(lambda: connection).locate(
            bundle.workflow_plan.workflow_plan_sha256,
            bundle.workflow_execution_id,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_worker_run_locator_rejects_wrong_plan_identity() -> None:
    connection = _Connection(_Cursor(admitted=False))
    bundle = _bundle()

    with pytest.raises(SemanticRefreshMssqlWorkerRunAuthorityError, match="differs"):
        MssqlSemanticRefreshWorkerRunAuthority(lambda: connection).locate(
            "sha256:" + "f" * 64,
            bundle.workflow_execution_id,
        )
