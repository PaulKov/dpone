from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig
from dpone.contracts.repair_authority import (
    ExpectedCheckpoint,
    RepairAllowance,
    RepairAuthority,
    RepairAuthorityError,
    TargetAuthorityTransfer,
    repair_authority_digest,
)
from dpone.ports.source_state_storage import MssqlStateLocation, SourceStateKey
from dpone.runtime.state.mssql_repair_authority import MssqlRepairAuthorityService
from dpone.runtime.state.xmin_storage import XMinState
from dpone.services.interval_context import IntervalContextService
from dpone.services.run_invocation_context import REPAIR_AUTHORITY_REF_ENV, RunInvocationContextService


def _key() -> SourceStateKey:
    return SourceStateKey(
        environment="prod",
        process="platform.sample_metrics.metrics_value",
        source_connection="postgres_sample_metrics_source",
        source_database="sample-metrics",
        source_schema="public",
        source_table="metrics_value",
        target_database="dwh_example",
        target_schema="sample_metrics",
        target_table="metrics_value",
        target_identity=b"t" * 32,
        unique_key=("guid",),
        schema_hash="sha256:" + "1" * 64,
        scope_hash="sha256:" + "2" * 64,
    )


def _authority_row(
    *,
    checkpoint: XMinState | None,
    full_baseline: bool = False,
    max_delete_rows: int | None = None,
    max_delete_ratio: float | None = None,
    expires_delta: timedelta = timedelta(hours=1),
    consumed: bool = False,
    transfer_from: TargetAuthorityTransfer | None = None,
) -> dict[str, object]:
    key = _key()
    expected = ExpectedCheckpoint(
        absent=checkpoint is None,
        xmin=checkpoint.xmin_value if checkpoint else None,
        revision=checkpoint.revision if checkpoint else None,
    )
    allow = RepairAllowance(full_baseline, max_delete_rows, max_delete_ratio)
    expires = datetime(2026, 1, 1, 13, tzinfo=UTC) + expires_delta
    authority_id = "repair-work-item-001"
    reason = "Approved one-shot work-item repair"
    digest = repair_authority_digest(
        authority_id=authority_id,
        state_key=key.digest,
        expected_checkpoint=expected,
        scope_hash=key.scope_hash,
        reason=reason,
        expires_at_utc=expires,
        allow=allow,
        transfer_from=transfer_from,
    )
    return {
        "authority_id": authority_id,
        "authority_digest": digest,
        "state_key": key.digest,
        "transfer_from_state_key": transfer_from.state_key if transfer_from else None,
        "transfer_from_xmin": transfer_from.xmin if transfer_from else None,
        "transfer_from_revision": transfer_from.revision if transfer_from else None,
        "expected_checkpoint_absent": expected.absent,
        "expected_xmin": expected.xmin,
        "expected_revision": expected.revision,
        "scope_hash": key.scope_hash,
        "reason": reason,
        "expires_at_utc": expires.replace(tzinfo=None),
        "allow_full_baseline": full_baseline,
        "allow_max_delete_rows": max_delete_rows,
        "allow_max_delete_ratio": max_delete_ratio,
        "current_utc": datetime(2026, 1, 1, 13),
        "already_consumed": consumed,
    }


class _Executor:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.row = row
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def get_records(self, sql, params=None, as_dict=False):
        del as_dict
        self.calls.append((str(sql), tuple(params or ())))
        return [self.row] if self.row is not None else []

    def execute_query(self, sql, params=None):
        self.calls.append((str(sql), tuple(params or ())))


def _service() -> MssqlRepairAuthorityService:
    return MssqlRepairAuthorityService(
        MssqlStateLocation(
            "Example_System",
            "dbo",
            "dpone_source_state",
            "dpone_commit_receipt",
        )
    )


def test_full_baseline_authority_is_checkpoint_scope_and_state_bound() -> None:
    previous = XMinState(100, datetime.now(UTC), revision=7)
    executor = _Executor(_authority_row(checkpoint=previous, full_baseline=True))

    authority = _service().admit(
        executor=executor,
        authority_ref="repair-work-item-001",
        key=_key(),
        expected_checkpoint=previous,
        scope_hash=_key().scope_hash,
        require_full_baseline=True,
        missing_rows=0,
        missing_ratio=0.0,
        configured_max_delete_rows=100_000,
        configured_max_delete_ratio=0.05,
    )

    assert isinstance(authority, RepairAuthority)
    assert authority.expected_checkpoint.matches(previous)
    assert "UPDLOCK, HOLDLOCK" in executor.calls[0][0]


def test_delete_guard_authority_does_not_need_full_baseline_permission() -> None:
    previous = XMinState(100, datetime.now(UTC), revision=7)
    executor = _Executor(
        _authority_row(
            checkpoint=previous,
            full_baseline=False,
            max_delete_rows=300_000,
            max_delete_ratio=0.5,
        )
    )

    authority = _service().admit(
        executor=executor,
        authority_ref="repair-work-item-001",
        key=_key(),
        expected_checkpoint=previous,
        scope_hash=_key().scope_hash,
        require_full_baseline=False,
        missing_rows=200_000,
        missing_ratio=0.2,
        configured_max_delete_rows=100_000,
        configured_max_delete_ratio=0.05,
    )

    assert authority.allow.full_baseline is False


def test_target_authority_transfer_is_digest_and_old_checkpoint_bound() -> None:
    old_key = _key().digest[::-1]
    transfer = TargetAuthorityTransfer(state_key=old_key, xmin=91, revision=4)
    row = _authority_row(checkpoint=None, full_baseline=True, transfer_from=transfer)

    authority = _service().preview(
        executor=_Executor(row),
        authority_ref="repair-work-item-001",
        key=_key(),
        expected_checkpoint=None,
        scope_hash=_key().scope_hash,
    )

    assert authority.transfer_from == transfer
    changed = dict(row)
    changed["transfer_from_revision"] = 5
    with pytest.raises(RepairAuthorityError, match="digest_mismatch"):
        RepairAuthority.from_record(changed)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(transfer_from_revision=None),
        lambda row: row.update(expected_checkpoint_absent=False, expected_xmin=1, expected_revision=1),
        lambda row: row.update(allow_full_baseline=False),
        lambda row: row.update(transfer_from_state_key=_key().digest),
    ],
)
def test_target_authority_transfer_rejects_partial_or_unsafe_binding(mutation) -> None:
    transfer = TargetAuthorityTransfer(state_key=_key().digest[::-1], xmin=91, revision=4)
    row = _authority_row(checkpoint=None, full_baseline=True, transfer_from=transfer)
    mutation(row)

    with pytest.raises(RepairAuthorityError):
        RepairAuthority.from_record(row)


def test_consumed_target_authority_transfer_cannot_be_reused() -> None:
    transfer = TargetAuthorityTransfer(state_key=_key().digest[::-1], xmin=91, revision=4)
    row = _authority_row(
        checkpoint=None,
        full_baseline=True,
        transfer_from=transfer,
        consumed=True,
    )

    with pytest.raises(RepairAuthorityError, match="already_consumed"):
        _service().preview(
            executor=_Executor(row),
            authority_ref="repair-work-item-001",
            key=_key(),
            expected_checkpoint=None,
            scope_hash=_key().scope_hash,
        )


def test_empty_snapshot_all_delete_requires_explicit_bounds_covering_rows_and_ratio() -> None:
    previous = XMinState(100, datetime.now(UTC), revision=7)
    sufficient = _Executor(
        _authority_row(
            checkpoint=previous,
            max_delete_rows=3,
            max_delete_ratio=1.0,
        )
    )

    authority = _service().admit(
        executor=sufficient,
        authority_ref="repair-work-item-001",
        key=_key(),
        expected_checkpoint=previous,
        scope_hash=_key().scope_hash,
        require_full_baseline=False,
        require_empty_snapshot_override=True,
        missing_rows=3,
        missing_ratio=1.0,
        configured_max_delete_rows=100_000,
        configured_max_delete_ratio=1.0,
    )

    assert authority.allow.max_delete_rows == 3
    assert authority.allow.max_delete_ratio == 1.0

    for row_bound, ratio_bound in ((2, 1.0), (3, 0.99), (None, 1.0), (3, None)):
        executor = _Executor(
            _authority_row(
                checkpoint=previous,
                max_delete_rows=row_bound,
                max_delete_ratio=ratio_bound,
            )
        )
        with pytest.raises(RepairAuthorityError, match="empty_snapshot_delete_bounds_denied"):
            _service().admit(
                executor=executor,
                authority_ref="repair-work-item-001",
                key=_key(),
                expected_checkpoint=previous,
                scope_hash=_key().scope_hash,
                require_full_baseline=False,
                require_empty_snapshot_override=True,
                missing_rows=3,
                missing_ratio=1.0,
                configured_max_delete_rows=100_000,
                configured_max_delete_ratio=1.0,
            )


def test_empty_target_baseline_requires_full_baseline_allowance() -> None:
    denied = _Executor(_authority_row(checkpoint=None, full_baseline=False))
    with pytest.raises(RepairAuthorityError, match="full_baseline_denied"):
        _service().admit(
            executor=denied,
            authority_ref="repair-work-item-001",
            key=_key(),
            expected_checkpoint=None,
            scope_hash=_key().scope_hash,
            require_full_baseline=True,
            require_empty_snapshot_override=True,
            missing_rows=0,
            missing_ratio=0.0,
            configured_max_delete_rows=100_000,
            configured_max_delete_ratio=0.05,
        )

    admitted = _service().admit(
        executor=_Executor(_authority_row(checkpoint=None, full_baseline=True)),
        authority_ref="repair-work-item-001",
        key=_key(),
        expected_checkpoint=None,
        scope_hash=_key().scope_hash,
        require_full_baseline=True,
        require_empty_snapshot_override=True,
        missing_rows=0,
        missing_ratio=0.0,
        configured_max_delete_rows=100_000,
        configured_max_delete_ratio=0.05,
    )

    assert admitted.allow.full_baseline is True


def test_consumed_empty_snapshot_authority_cannot_be_reused() -> None:
    previous = XMinState(100, datetime.now(UTC), revision=7)
    executor = _Executor(
        _authority_row(
            checkpoint=previous,
            max_delete_rows=3,
            max_delete_ratio=1.0,
            consumed=True,
        )
    )

    with pytest.raises(RepairAuthorityError, match="already_consumed"):
        _service().admit(
            executor=executor,
            authority_ref="repair-work-item-001",
            key=_key(),
            expected_checkpoint=previous,
            scope_hash=_key().scope_hash,
            require_full_baseline=False,
            require_empty_snapshot_override=True,
            missing_rows=3,
            missing_ratio=1.0,
            configured_max_delete_rows=100_000,
            configured_max_delete_ratio=1.0,
        )


@pytest.mark.parametrize(
    ("row_mutation", "code"),
    [
        (lambda row: row.update(already_consumed=True), "already_consumed"),
        (
            lambda row: row.update(current_utc=datetime(2026, 1, 2)),
            "expired",
        ),
        (lambda row: row.update(scope_hash="sha256:" + "9" * 64), "digest_mismatch"),
    ],
)
def test_authority_reuse_expiry_and_mutation_fail_closed(row_mutation, code: str) -> None:
    previous = XMinState(100, datetime.now(UTC), revision=7)
    row = _authority_row(checkpoint=previous, full_baseline=True)
    row_mutation(row)

    with pytest.raises(RepairAuthorityError, match=code):
        _service().preview(
            executor=_Executor(row),
            authority_ref="repair-work-item-001",
            key=_key(),
            expected_checkpoint=previous,
            scope_hash=_key().scope_hash,
        )


def test_consumption_is_unique_and_bound_to_state_load_and_receipt() -> None:
    previous = XMinState(100, datetime.now(UTC), revision=7)
    executor = _Executor(_authority_row(checkpoint=previous, full_baseline=True))
    authority = _service().preview(
        executor=executor,
        authority_ref="repair-work-item-001",
        key=_key(),
        expected_checkpoint=previous,
        scope_hash=_key().scope_hash,
    )

    _service().consume(
        executor=executor,
        authority=authority,
        key=_key(),
        load_id="load-1",
        receipt_id="receipt-1",
        used_full_baseline=True,
        observed_delete_rows=42,
        observed_delete_ratio=0.01,
    )

    sql, params = executor.calls[-1]
    assert "DPONE_REPAIR_AUTHORITY_ALREADY_CONSUMED" in sql
    assert "state_key = ?" in sql
    assert "load_id = ? AND receipt_id = ? AND state_key = ?" in sql
    assert "SYSUTCDATETIME()" in sql
    assert sql.count("?") == len(params)


def test_invocation_ref_is_runtime_only_and_validated() -> None:
    service = RunInvocationContextService(
        mapping_context_service=SimpleNamespace(from_environ=lambda _env: {}),
        interval_context_factory=IntervalContextService,
    )
    invocation = service.resolve(environ={REPAIR_AUTHORITY_REF_ENV: "repair-work-item-001"})
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="metrics_value",
        target_schema="sample_metrics",
        target_table="metrics_value",
    )

    assert invocation.load_config_mutator(config).repair_authority_ref == "repair-work-item-001"
    with pytest.raises(Exception) as error:
        service.resolve(environ={REPAIR_AUTHORITY_REF_ENV: "../../standing-permission"})
    assert getattr(error.value, "code", None) == "DPONE_REPAIR_AUTHORITY_REF_INVALID"
