"""Protected continuation receipts for later Airflow task tries."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.adapters.semantic_refresh_mssql_attempt_continuation import (
    MssqlSemanticRefreshAttemptContinuationStore,
    SemanticRefreshMssqlAttemptContinuationError,
)
from dpone.contracts.semantic_refresh_attempt_continuation import (
    SemanticRefreshAttemptContinuationReceipt,
    semantic_refresh_engine_quiescence_sha256,
)
from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshContainerTermination,
    SemanticRefreshTrustedAttemptTerminationReceipt,
)
from dpone.ports.semantic_refresh_attempt_quiescence import (
    ClickHouseAttemptQuiescenceProof,
)
from dpone.ports.semantic_refresh_mssql_worker_admission import (
    MssqlTrustedAttemptCoordinate,
    compose_worker_admission_bundle,
)
from tests.test_dbt_semantic_refresh_plan_compiler import _plan_bundle, _run_bundle
from tests.test_semantic_refresh_mssql_worker_admission import _coordinates, _epochs

_BUILD = "sha256:" + "b" * 64
_AFTER = "sha256:" + "a" * 64


class _Cursor:
    def __init__(
        self,
        bundle,
        *,
        observed: str = _AFTER,
        observed_rows: int = 1,
        observed_bytes: int = 1_024,
        existing=None,
        termination=True,
        lock_result: int = 0,
        active_sessions: int = 0,
    ) -> None:
        self.connection = type("Connection", (), {"timeout": 30})()
        self.bundle = bundle
        self.observed = observed
        self.observed_rows = observed_rows
        self.observed_bytes = observed_bytes
        self.existing = existing
        self.termination = _termination(bundle) if termination is True else termination
        self.lock_result = lock_result
        self.active_sessions = active_sessions
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self._row = None

    def execute(self, sql: str, *parameters: object):
        self.executions.append((sql, tuple(parameters)))
        self._row = None
        attempt = self.bundle.attempt_bindings[0]
        operation = self.bundle.operation_plans[0]
        resource = self.bundle.model_resources[0]
        if "FROM [dpone_control].[semantic_refresh_attempt_terminations]" in sql:
            if self.termination is not None:
                receipt = self.termination
                self._row = (
                    receipt.workflow_execution_id,
                    receipt.workflow_execution_binding_sha256,
                    json.dumps(list(receipt.operation_ids), separators=(",", ":")),
                    receipt.operation_set_sha256,
                    receipt.attempt_binding_sha256,
                    receipt.dag_id,
                    receipt.run_id,
                    receipt.task_id,
                    receipt.map_index,
                    receipt.try_number,
                    receipt.cluster_id,
                    receipt.namespace,
                    receipt.pod_name,
                    receipt.pod_uid,
                    receipt.pod_resource_version,
                    receipt.terminal_phase,
                    json.dumps(
                        [item.to_dict() for item in receipt.container_terminations],
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    datetime.fromisoformat(receipt.observed_at.replace("Z", "+00:00")).replace(tzinfo=None),
                    receipt.observer_authority,
                    receipt.observer_policy_sha256,
                    receipt.observer_attestation_sha256,
                    receipt.observer_signature_sha256,
                    receipt.verification_status,
                    receipt.termination_receipt_sha256,
                )
        elif "@dpone_continuation_lock_result" in sql:
            self._row = (self.lock_result, self.active_sessions)
        elif "FROM [dpone_control].[semantic_refresh_journals] AS journal" in sql:
            self._row = (
                operation.operation_id,
                operation.operation_plan_sha256,
                attempt.attempt_binding_sha256,
                attempt.fencing_epoch,
                attempt.owner_id,
                "PREPARING",
                resource.target_resource_id,
                attempt.fencing_epoch,
                attempt.owner_id,
                "HELD",
                _BUILD,
                _AFTER,
            )
        elif "@dpone_continuation_target_preflight_bytes" in sql:
            self._row = (self.observed_rows, self.observed_bytes)
        elif "@dpone_continuation_target_json" in sql:
            self._row = (self.observed,)
        elif "FROM [dpone_control].[semantic_refresh_attempt_continuations]" in sql:
            if self.existing is not None:
                self._row = (
                    json.dumps(self.existing.to_dict(), separators=(",", ":"), sort_keys=True),
                    self.existing.continuation_receipt_sha256,
                )
        return self

    def fetchone(self):
        row = self._row
        self._row = None
        return row

    def fetchall(self):
        return ()


def _bundle():
    plan = _plan_bundle()
    return compose_worker_admission_bundle(
        plan_bundle=plan,
        run_execution=_run_bundle(plan),
        coordinates=_coordinates(plan),
        guard_epochs=_epochs(plan),
    )


def _continued(bundle, *, try_number: int = 2) -> tuple[MssqlTrustedAttemptCoordinate, ...]:
    original = bundle.attempt_bindings[0]
    return (
        MssqlTrustedAttemptCoordinate(
            operation_id=original.operation_id,
            task_id=original.task_id,
            try_number=try_number,
            pod_uid="00000000-0000-0000-0000-000000000888",
        ),
    )


def _termination(bundle) -> SemanticRefreshTrustedAttemptTerminationReceipt:
    original = bundle.attempt_bindings[0]
    return SemanticRefreshTrustedAttemptTerminationReceipt.build(
        workflow_execution_id=bundle.workflow_execution_id,
        workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
        operation_ids=(original.operation_id,),
        attempt_binding_sha256=original.attempt_binding_sha256,
        dag_id="semantic_refresh",
        run_id=original.dag_run_id,
        task_id=original.task_id,
        map_index=-1,
        try_number=original.try_number,
        cluster_id="local-airflow",
        namespace="dpone",
        pod_name="semantic-refresh-original",
        pod_uid=original.pod_uid,
        pod_resource_version="10",
        terminal_phase="Failed",
        container_terminations=(
            SemanticRefreshContainerTermination(
                name="base",
                container_id="containerd://original",
                reason="Error",
                finished_at="2026-08-09T23:59:59.000000Z",
                exit_code=1,
            ),
        ),
        observed_at="2026-08-09T23:59:59.500000Z",
        observer_authority="kubernetes-observer/local",
        observer_policy_sha256="sha256:" + "1" * 64,
        observer_attestation_sha256="sha256:" + "2" * 64,
        observer_signature_sha256="sha256:" + "3" * 64,
    )


class _Quiescence:
    def __init__(self, *, active: bool = False) -> None:
        self.active = active

    def prove_quiescent(self, **values):
        if self.active:
            raise RuntimeError("ClickHouse operation query is still active")
        return ClickHouseAttemptQuiescenceProof.build(
            workflow_execution_binding_sha256=values["workflow_execution_binding_sha256"],
            operation_id=values["operation_id"],
            original_attempt_binding_sha256=values["original_attempt_binding_sha256"],
            clickhouse_cluster_authority_id=values["clickhouse_cluster_authority_id"],
            query_id_prefix="dpone-semref-aaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbb-",
            observed_at=values["observed_at"],
        )


def _store(*, quiescence=None, day: int = 10):
    return MssqlSemanticRefreshAttemptContinuationStore(
        control_schema="dpone_control",
        clock=lambda: datetime(2026, 8, day, tzinfo=UTC),
        clickhouse_quiescence=quiescence or _Quiescence(),
    )


def test_continuation_binds_new_try_only_after_current_target_matches_after_image() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)
    store = _store()

    receipts = store.reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt.original_attempt_binding_sha256 == bundle.attempt_bindings[0].attempt_binding_sha256
    assert receipt.after_image_sha256 == _AFTER
    assert receipt.try_number == 2
    assert receipt.status == "VERIFIED"
    assert receipt.original_attempt_termination_receipt_sha256 == _termination(bundle).termination_receipt_sha256
    assert receipt.mssql_active_session_count == 0
    assert receipt.mssql_guard_lock_status == "EXCLUSIVE_ACQUIRED"
    quiescence_sql = next(
        sql for sql, _ in cursor.executions if "DPONE_SEMANTIC_REFRESH_SESSION_QUIESCENCE_UNVERIFIED" in sql
    )
    assert "HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER STATE')" in quiescence_sql
    assert "HAS_PERMS_BY_NAME(NULL, N'SERVER'" not in quiescence_sql
    assert any(
        "INSERT INTO [dpone_control].[semantic_refresh_attempt_continuations]" in sql for sql, _ in cursor.executions
    )


def test_continuation_rejects_current_target_drift_before_persistence() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle, observed="sha256:" + "f" * 64)
    store = _store()

    with pytest.raises(SemanticRefreshMssqlAttemptContinuationError, match="current target scope differs"):
        store.reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)


def test_continuation_rejects_same_or_older_try_with_changed_pod() -> None:
    bundle = _bundle()
    store = _store()

    with pytest.raises(SemanticRefreshMssqlAttemptContinuationError, match="does not advance"):
        store.reconcile(_Cursor(bundle), bundle=bundle, coordinates=_continued(bundle, try_number=1))


def test_continuation_v1_rejects_more_than_one_successor_try() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="DPONE_SEMANTIC_REFRESH_CONTINUATION_CHAIN_UNSUPPORTED",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle, try_number=3))

    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)
    assert not any("@dpone_continuation_target_json" in sql for sql, _ in cursor.executions)


def test_continuation_v1_rejects_later_try_even_when_pod_uid_matches() -> None:
    bundle = _bundle()
    original = bundle.attempt_bindings[0]
    cursor = _Cursor(bundle)
    coordinates = (
        MssqlTrustedAttemptCoordinate(
            operation_id=original.operation_id,
            task_id=original.task_id,
            try_number=original.try_number + 2,
            pod_uid=original.pod_uid,
        ),
    )

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="DPONE_SEMANTIC_REFRESH_CONTINUATION_CHAIN_UNSUPPORTED",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=coordinates)

    assert cursor.executions == []


def test_continuation_preflights_rows_and_bytes_before_target_json() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)

    _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    statements = [sql for sql, _ in cursor.executions]
    preflight_index = next(
        index for index, sql in enumerate(statements) if "@dpone_continuation_target_preflight_bytes" in sql
    )
    json_index = next(index for index, sql in enumerate(statements) if "@dpone_continuation_target_json" in sql)
    assert preflight_index < json_index
    assert "DATALENGTH([event_id])" in statements[preflight_index]
    assert "DATALENGTH([event_date])" in statements[preflight_index]
    assert "* 12" in statements[preflight_index]


def test_continuation_requires_protected_query_timeout_before_sql() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)
    for timeout in (0, 601):
        cursor = _Cursor(bundle)
        cursor.connection.timeout = timeout
        with pytest.raises(
            SemanticRefreshMssqlAttemptContinuationError,
            match="DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED",
        ):
            _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))
        assert cursor.executions == []


def test_continuation_rejects_cursor_without_query_timeout_connection() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)
    del cursor.connection

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert cursor.executions == []


def test_continuation_wraps_driver_timeout_error_before_sql() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)

    class _BrokenTimeout:
        @property
        def timeout(self) -> int:
            raise RuntimeError("driver timeout unavailable")

    cursor.connection = _BrokenTimeout()

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_UNVERIFIED",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert cursor.executions == []


def test_continuation_rejects_hard_byte_cap_before_target_json() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle, observed_bytes=67_108_865)

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert not any("@dpone_continuation_target_json" in sql for sql, _ in cursor.executions)
    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)


def test_continuation_rejects_row_budget_before_target_json() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle, observed_rows=1_000_001)

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="DPONE_SEMANTIC_REFRESH_RESOURCE_BUDGET_EXCEEDED",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert not any("@dpone_continuation_target_json" in sql for sql, _ in cursor.executions)
    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)


def test_continuation_exact_replay_returns_original_timestamp_and_digest() -> None:
    bundle = _bundle()
    first = _store().reconcile(_Cursor(bundle), bundle=bundle, coordinates=_continued(bundle))[0]
    cursor = _Cursor(bundle, existing=first)
    replay = _store(day=11).reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))[0]

    assert replay == first
    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)


def test_continuation_uses_canonical_uuid_ordering_for_target_digest() -> None:
    bundle = _bundle()
    resource = bundle.model_resources[0]
    uuid_columns = tuple(
        replace(item, source_type="uniqueidentifier") if item.name == "event_id" else item
        for item in resource.writable_columns
    )
    bundle = replace(bundle, model_resources=(replace(resource, writable_columns=uuid_columns),))
    cursor = _Cursor(bundle)

    _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    target_sql = next(sql for sql, _ in cursor.executions if "@dpone_continuation_target_json" in sql)
    assert "CONVERT(char(36), [event_id])" in target_sql


def test_changed_try_with_same_pod_still_requires_takeover_evidence() -> None:
    bundle = _bundle()
    original = bundle.attempt_bindings[0]
    coordinates = (
        MssqlTrustedAttemptCoordinate(
            operation_id=original.operation_id,
            task_id=original.task_id,
            try_number=original.try_number + 1,
            pod_uid=original.pod_uid,
        ),
    )
    cursor = _Cursor(bundle, termination=None)

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="termination receipt is absent",
    ):
        _store(quiescence=_Quiescence(active=True)).reconcile(
            cursor,
            bundle=bundle,
            coordinates=coordinates,
        )

    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)
    assert not any("@dpone_continuation_target_json" in sql for sql, _ in cursor.executions)


def test_new_pod_rejects_absent_termination_before_reconciliation() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle, termination=None)

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="termination receipt is absent",
    ):
        _store().reconcile(cursor, bundle=bundle, coordinates=_continued(bundle))

    assert not any("semantic_refresh_journals" in sql for sql, _ in cursor.executions)


@pytest.mark.parametrize(("lock_result", "active_sessions"), [(-1, 0), (0, 1)])
def test_new_pod_rejects_nonquiescent_mssql_executor(
    lock_result: int,
    active_sessions: int,
) -> None:
    bundle = _bundle()

    with pytest.raises(
        SemanticRefreshMssqlAttemptContinuationError,
        match="MSSQL session, transaction, or app lock",
    ):
        _store().reconcile(
            _Cursor(
                bundle,
                lock_result=lock_result,
                active_sessions=active_sessions,
            ),
            bundle=bundle,
            coordinates=_continued(bundle),
        )


def test_new_pod_rejects_active_clickhouse_operation_query() -> None:
    bundle = _bundle()
    cursor = _Cursor(bundle)

    with pytest.raises(RuntimeError, match="ClickHouse operation query is still active"):
        _store(quiescence=_Quiescence(active=True)).reconcile(
            cursor,
            bundle=bundle,
            coordinates=_continued(bundle),
        )

    # Target reconciliation and receipt persistence happen only after engine quiescence.
    assert not any("INSERT INTO" in sql for sql, _ in cursor.executions)


def test_continuation_receipt_golden_is_stable() -> None:
    def digest(character: str) -> str:
        return "sha256:" + character * 64

    workflow_binding = digest("a")
    operation_id = digest("b")
    original_attempt = digest("d")
    termination = digest("1")
    verified_at = "2026-08-10T08:00:00.000000Z"
    clickhouse = ClickHouseAttemptQuiescenceProof.build(
        workflow_execution_binding_sha256=workflow_binding,
        operation_id=operation_id,
        original_attempt_binding_sha256=original_attempt,
        clickhouse_cluster_authority_id="clickhouse-local",
        query_id_prefix="dpone-semref-aaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbbbbbb-",
        observed_at=verified_at,
    )
    receipt = SemanticRefreshAttemptContinuationReceipt.build(
        workflow_execution_binding_sha256=workflow_binding,
        operation_id=operation_id,
        operation_plan_sha256=digest("c"),
        original_attempt_binding_sha256=original_attempt,
        fencing_epoch=7,
        guard_resource_id="DWH.mart.events",
        build_receipt_sha256=digest("e"),
        after_image_sha256=digest("f"),
        original_attempt_termination_receipt_sha256=termination,
        clickhouse_cluster_authority_id="clickhouse-local",
        clickhouse_query_id_prefix=clickhouse.query_id_prefix,
        clickhouse_quiescence_observation_sha256=(clickhouse.quiescence_observation_sha256),
        mssql_active_session_count=0,
        mssql_guard_lock_status="EXCLUSIVE_ACQUIRED",
        engine_quiescence_receipt_sha256=semantic_refresh_engine_quiescence_sha256(
            workflow_execution_binding_sha256=workflow_binding,
            operation_id=operation_id,
            original_attempt_binding_sha256=original_attempt,
            original_attempt_termination_receipt_sha256=termination,
            clickhouse_quiescence_observation_sha256=(clickhouse.quiescence_observation_sha256),
            observed_at=verified_at,
        ),
        task_id="dbt_build_and_tests",
        try_number=2,
        pod_uid="00000000-0000-0000-0000-000000000888",
        verified_at=verified_at,
    )
    expected = json.loads(
        Path("tests/fixtures/semantic-refresh-v2/contracts/attempt-continuation-golden-v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert receipt.to_dict() == expected
    assert SemanticRefreshAttemptContinuationReceipt.from_mapping(expected) == receipt


def test_continuation_v1_golden_stays_parseable_and_rejects_v2_holder_fields() -> None:
    expected = json.loads(
        Path("tests/fixtures/semantic-refresh-v2/contracts/attempt-continuation-golden-v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert SemanticRefreshAttemptContinuationReceipt.from_mapping(expected).schema.endswith("receipt.v1")
    with pytest.raises(ValueError, match="unknown=.+previous_holder_attempt_binding_sha256"):
        SemanticRefreshAttemptContinuationReceipt.from_mapping(
            {
                **expected,
                "previous_holder_attempt_binding_sha256": "sha256:" + "9" * 64,
            }
        )
