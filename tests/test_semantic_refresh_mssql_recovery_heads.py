from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.adapters.semantic_refresh_mssql_recovery_heads import (
    MssqlSemanticRefreshRecoveryHeadReader,
    SemanticRefreshMssqlRecoveryHeadReadError,
)
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlDurableRecoveryAuthority,
    MssqlDurableRecoveryPublication,
    MssqlDurableRecoveryTargetHead,
    MssqlRecoveryHeadLocator,
    MssqlRecoveryHeadReadRequest,
)
from dpone.services.semantic_refresh_mssql_recovery_heads import SemanticRefreshMssqlRecoveryHeadService
from tests.test_semantic_refresh_mssql_authority import _bundle, _record
from tests.test_semantic_refresh_mssql_run_authority import (
    _Cursor as _WorkerBindingCursor,
)
from tests.test_semantic_refresh_mssql_run_authority import _worker_bundle

_WORKER_BUNDLE = _worker_bundle()
_EXECUTION_ID = _WORKER_BUNDLE.workflow_execution_id
_BINDING = _WORKER_BUNDLE.execution_binding.workflow_execution_binding_sha256
_PLAN = _WORKER_BUNDLE.workflow_plan.workflow_plan_sha256
_AUTHORITY = _WORKER_BUNDLE.authority_sha256
_PACK = "sha256:" + "3" * 64
_OPERATION = "sha256:" + "4" * 64
_TERMINAL = "sha256:" + "5" * 64
_GENERATION_ID = "sha256:" + "6" * 64
_UUID = "00000000-0000-0000-0000-000000000007"
_OPERATION_PLAN = "sha256:" + "a" * 64
_ATTEMPT = "sha256:" + "b" * 64


def _request() -> MssqlRecoveryHeadReadRequest:
    return MssqlRecoveryHeadReadRequest(
        workflow_execution_id=_EXECUTION_ID,
        workflow_execution_binding_sha256=_BINDING,
        workflow_plan_sha256=_PLAN,
        canonical_authority_sha256=_AUTHORITY,
        locators=(
            MssqlRecoveryHeadLocator(
                model_unique_id="model.analytics.events",
                clickhouse_target_authority_id="clickhouse://cluster/analytics/events",
                database_name="analytics",
                target_table="events",
                predecessor_operation_id=_OPERATION,
            ),
        ),
    )


class _Cursor(_WorkerBindingCursor):
    def __init__(
        self,
        *,
        head_generation: int = 2,
        terminal_generation: int | None = None,
        head_owner: str = _OPERATION,
        mutation_outcome: str = "TARGET_COMMITTED",
        terminal_predecessor_owner: str = "sha256:" + "7" * 64,
        tampered_pack_fingerprint: bool = False,
        projection_plan_sha256: str | None = None,
        activation_receipt_status: str = "ACTIVE",
        stored_terminal_workflow_execution_id: str | None = None,
        stored_terminal_workflow_execution_binding_sha256: str | None = None,
    ) -> None:
        super().__init__(
            admitted=True,
            execution_status="COMPLETE",
            journal_status="COMPLETE",
            tampered_pack_fingerprint=tampered_pack_fingerprint,
            projection_plan_sha256=projection_plan_sha256,
            activation_receipt_status=activation_receipt_status,
        )
        self.head_generation = head_generation
        self.terminal_generation = terminal_generation or head_generation
        self.head_owner = head_owner
        self.mutation_outcome = mutation_outcome
        self.terminal_predecessor_owner = terminal_predecessor_owner
        self.stored_terminal_workflow_execution_id = stored_terminal_workflow_execution_id
        self.stored_terminal_workflow_execution_binding_sha256 = stored_terminal_workflow_execution_binding_sha256

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        if "semantic_refresh_workflow_executions" in sql:
            self.executions.append((sql, tuple(parameters)))
            self._rows = ()
            self._row = (
                _PLAN,
                _AUTHORITY,
                "COMPLETE",
                "sha256:" + "8" * 64,
                "{}",
                self.stored_terminal_workflow_execution_id or _EXECUTION_ID,
                self.stored_terminal_workflow_execution_binding_sha256 or _BINDING,
            )
        elif "semantic_refresh_journals" in sql:
            self.executions.append((sql, tuple(parameters)))
            self._rows = ()
            requested_operation = str(parameters[0])
            if self.mutation_outcome == "NOT_REQUIRED_EMPTY_SCOPE" and requested_operation == self.head_owner:
                self._row = None
            else:
                self._row = (
                    requested_operation,
                    "model.analytics.events",
                    "analytics",
                    "events",
                    "COMPLETE",
                    _TERMINAL,
                    self.terminal_generation,
                    _GENERATION_ID,
                    _UUID,
                    self.mutation_outcome,
                    self.terminal_predecessor_owner,
                    _EXECUTION_ID,
                    _OPERATION_PLAN,
                    _ATTEMPT,
                    "sha256:" + "c" * 64,
                    "sha256:" + "d" * 64,
                    1,
                )
        elif "semantic_refresh_target_heads" in sql:
            self.executions.append((sql, tuple(parameters)))
            self._rows = ()
            self._row = (self.head_generation, _GENERATION_ID, _UUID, self.head_owner)
        else:
            super().execute(sql, *parameters)
        return self


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


def test_recovery_head_reader_locks_complete_journal_and_current_head() -> None:
    connection = _Connection(_Cursor())

    result = MssqlSemanticRefreshRecoveryHeadReader(lambda: connection).load_recovery_authority(_request())

    assert result.terminal_summary_sha256 == "sha256:" + "8" * 64
    assert result.plan_bundle_sha256 == _PACK
    assert result.predecessor_publications[0].operation_id == _OPERATION
    assert result.target_heads[0].target_generation == 2
    assert result.target_heads[0].target_generation_id == _GENERATION_ID
    assert result.target_heads[0].target_uuid == _UUID
    assert result.target_heads[0].owner_operation_id == _OPERATION
    assert result.target_heads[0].terminal_receipt_sha256 == _TERMINAL
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert all("WITH (UPDLOCK, HOLDLOCK)" in sql for sql, _ in connection.cursor_instance.executions if "SELECT" in sql)


def test_recovery_head_reader_preserves_empty_scope_target_owner() -> None:
    existing_owner = "sha256:" + "7" * 64
    connection = _Connection(
        _Cursor(
            head_owner=existing_owner,
            mutation_outcome="NOT_REQUIRED_EMPTY_SCOPE",
            terminal_predecessor_owner=existing_owner,
        )
    )

    result = MssqlSemanticRefreshRecoveryHeadReader(lambda: connection).load_recovery_authority(_request())

    assert result.target_heads[0].owner_operation_id == existing_owner


def test_recovery_head_reader_authenticates_an_interleaved_current_owner() -> None:
    current_owner = "sha256:" + "c" * 64
    connection = _Connection(_Cursor(head_generation=3, head_owner=current_owner))

    result = MssqlSemanticRefreshRecoveryHeadReader(lambda: connection).load_recovery_authority(_request())

    assert result.target_heads[0].target_generation == 3
    assert result.target_heads[0].owner_operation_id == current_owner
    journal_reads = [parameters[0] for sql, parameters in connection.cursor_instance.executions if "journals" in sql]
    assert journal_reads == [_OPERATION, current_owner]


def test_recovery_head_reader_rejects_stale_current_generation() -> None:
    connection = _Connection(_Cursor(head_generation=3, terminal_generation=2))

    with pytest.raises(SemanticRefreshMssqlRecoveryHeadReadError, match="differs"):
        MssqlSemanticRefreshRecoveryHeadReader(lambda: connection).load_recovery_authority(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(tampered_pack_fingerprint=True),
        _Cursor(projection_plan_sha256="sha256:" + "f" * 64),
        _Cursor(activation_receipt_status="INACTIVE"),
    ],
)
def test_recovery_head_reader_rejects_incomplete_activated_pack_authority(
    cursor: _Cursor,
) -> None:
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshMssqlRecoveryHeadReadError, match="protected"):
        MssqlSemanticRefreshRecoveryHeadReader(lambda: connection).load_recovery_authority(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "cursor",
    [
        _Cursor(stored_terminal_workflow_execution_id=_EXECUTION_ID.upper()),
        _Cursor(stored_terminal_workflow_execution_binding_sha256=_BINDING.upper()),
    ],
)
def test_recovery_head_reader_rejects_collation_equivalent_terminal_execution(
    cursor: _Cursor,
) -> None:
    connection = _Connection(cursor)

    with pytest.raises(SemanticRefreshMssqlRecoveryHeadReadError, match="execution"):
        MssqlSemanticRefreshRecoveryHeadReader(lambda: connection).load_recovery_authority(_request())

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_recovery_head_service_derives_locators_from_canonical_authority() -> None:
    bundle = _bundle()
    resource = bundle.model_resources[0]
    operation = bundle.operation_plans[0]

    @dataclass(frozen=True)
    class _Authority:
        def load(self, workflow_execution_binding_sha256: str):
            assert workflow_execution_binding_sha256 == (bundle.execution_binding.workflow_execution_binding_sha256)
            return _record(bundle)

    @dataclass
    class _State:
        request: MssqlRecoveryHeadReadRequest | None = None

        def load_recovery_authority(
            self,
            request: MssqlRecoveryHeadReadRequest,
        ) -> MssqlDurableRecoveryAuthority:
            self.request = request
            return MssqlDurableRecoveryAuthority(
                workflow_execution_id=bundle.workflow_execution_id,
                workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
                plan_bundle_sha256="sha256:" + "9" * 64,
                canonical_authority_sha256=bundle.authority_sha256,
                terminal_summary_sha256="sha256:" + "8" * 64,
                terminal_summary_json="{}",
                predecessor_publications=(
                    MssqlDurableRecoveryPublication(
                        model_unique_id=resource.model_unique_id,
                        operation_id=operation.operation_id,
                        operation_plan_sha256=operation.operation_plan_sha256,
                        workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                        attempt_binding_sha256=bundle.attempt_bindings[0].attempt_binding_sha256,
                        artifact_manifest_sha256="sha256:" + "c" * 64,
                        clickhouse_terminal_receipt_sha256="sha256:" + "d" * 64,
                        terminal_receipt_sha256=_TERMINAL,
                        target_generation=2,
                        scope_revision=1,
                    ),
                ),
                target_heads=(
                    MssqlDurableRecoveryTargetHead(
                        model_unique_id=resource.model_unique_id,
                        clickhouse_target_authority_id=resource.target_authority_id,
                        target_generation=2,
                        target_generation_id=_GENERATION_ID,
                        target_uuid=_UUID,
                        owner_operation_id=operation.operation_id,
                        terminal_receipt_sha256=_TERMINAL,
                    ),
                ),
            )

    state = _State()
    result = SemanticRefreshMssqlRecoveryHeadService(_Authority(), state).load(
        bundle.execution_binding.workflow_execution_binding_sha256
    )

    assert state.request is not None
    assert state.request.canonical_authority_sha256 == bundle.authority_sha256
    assert state.request.locators[0].predecessor_operation_id == operation.operation_id
    assert result.target_heads[0].target_uuid == _UUID
    assert result.target_heads[0].terminal_receipt_sha256 == _TERMINAL
