"""Caller-transaction admission and consumption of MSSQL repair authority."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from dpone.contracts.repair_authority import (
    RepairAuthority,
    RepairAuthorityError,
    RepairAuthorityUse,
)
from dpone.ports.source_state_storage import MssqlStateLocation, SourceStateKey

_UTC = timezone.utc  # noqa: UP017 - mypy baseline supports Python 3.10 stubs.


class MssqlRepairAuthorityService:
    """Admit only the exact immutable approval selected by this invocation."""

    def __init__(self, location: MssqlStateLocation) -> None:
        self._location = location

    def admit(
        self,
        *,
        executor: Any,
        authority_ref: str | None,
        key: SourceStateKey,
        expected_checkpoint: Any | None,
        scope_hash: str,
        require_full_baseline: bool,
        require_empty_snapshot_override: bool = False,
        missing_rows: int,
        missing_ratio: float,
        configured_max_delete_rows: int,
        configured_max_delete_ratio: float,
    ) -> RepairAuthority | None:
        guard_exceeded = missing_rows > configured_max_delete_rows or missing_ratio > configured_max_delete_ratio
        required = require_full_baseline or require_empty_snapshot_override or guard_exceeded
        if not authority_ref:
            if required:
                raise RepairAuthorityError("repair_authority.required")
            return None
        if not required:
            raise RepairAuthorityError("repair_authority.not_required")

        authority = self._load_bound(
            executor=executor,
            authority_ref=authority_ref,
            key=key,
            expected_checkpoint=expected_checkpoint,
            scope_hash=scope_hash,
            lock=True,
        )
        if require_full_baseline and not authority.allow.full_baseline:
            raise RepairAuthorityError("repair_authority.full_baseline_denied")
        if require_empty_snapshot_override:
            if missing_rows == 0:
                if not require_full_baseline:
                    raise RepairAuthorityError("repair_authority.empty_snapshot_full_baseline_required")
            elif (
                missing_ratio != 1.0
                or authority.allow.max_delete_rows is None
                or authority.allow.max_delete_ratio is None
                or missing_rows > authority.allow.max_delete_rows
                or missing_ratio > authority.allow.max_delete_ratio
            ):
                raise RepairAuthorityError("repair_authority.empty_snapshot_delete_bounds_denied")
        if guard_exceeded and not authority.allow.permits_delete_override(
            missing_rows=missing_rows,
            missing_ratio=missing_ratio,
            configured_max_rows=configured_max_delete_rows,
            configured_max_ratio=configured_max_delete_ratio,
        ):
            raise RepairAuthorityError("repair_authority.delete_guard_denied")
        return authority

    def preview(
        self,
        *,
        executor: Any,
        authority_ref: str,
        key: SourceStateKey,
        expected_checkpoint: Any | None,
        scope_hash: str,
    ) -> RepairAuthority:
        """Validate immutable bindings before source extraction; consume nothing."""

        return self._load_bound(
            executor=executor,
            authority_ref=authority_ref,
            key=key,
            expected_checkpoint=expected_checkpoint,
            scope_hash=scope_hash,
            lock=False,
        )

    def _load_bound(
        self,
        *,
        executor: Any,
        authority_ref: str,
        key: SourceStateKey,
        expected_checkpoint: Any | None,
        scope_hash: str,
        lock: bool,
    ) -> RepairAuthority:
        rows = executor.get_records(self._authority_sql(lock=lock), (authority_ref,), as_dict=True)
        if not rows:
            raise RepairAuthorityError("repair_authority.not_found")
        row = rows[0]
        if bool(row.get("already_consumed")):
            raise RepairAuthorityError("repair_authority.already_consumed")
        authority = RepairAuthority.from_record(row)
        current_utc = _utc_datetime(row.get("current_utc"), "repair_authority.clock_invalid")
        if authority.expires_at_utc <= current_utc:
            raise RepairAuthorityError("repair_authority.expired")
        if authority.state_key != key.digest:
            raise RepairAuthorityError("repair_authority.state_key_mismatch")
        if authority.scope_hash != scope_hash or scope_hash != key.scope_hash:
            raise RepairAuthorityError("repair_authority.scope_hash_mismatch")
        if not authority.expected_checkpoint.matches(expected_checkpoint):
            raise RepairAuthorityError("repair_authority.checkpoint_mismatch")
        return authority

    def consume(
        self,
        *,
        executor: Any,
        authority: RepairAuthority,
        key: SourceStateKey,
        load_id: str,
        receipt_id: str,
        used_full_baseline: bool,
        observed_delete_rows: int,
        observed_delete_ratio: float,
    ) -> None:
        """Bind a unique authority consumption to the exact committed receipt."""

        use = RepairAuthorityUse(
            authority=authority,
            load_id=load_id,
            receipt_id=receipt_id,
            used_full_baseline=used_full_baseline,
            observed_delete_rows=observed_delete_rows,
            observed_delete_ratio=observed_delete_ratio,
        )
        executor.execute_query(
            self._consumption_sql(),
            (
                use.authority.authority_id,
                use.authority.authority_digest,
                key.digest,
                use.load_id,
                use.receipt_id,
                key.digest,
                use.authority.authority_id,
                use.authority.authority_id,
                use.authority.authority_digest,
                key.digest,
                use.load_id,
                use.receipt_id,
                int(use.used_full_baseline),
                use.observed_delete_rows,
                use.observed_delete_ratio,
            ),
        )

    def _authority_sql(self, *, lock: bool) -> str:
        authority = self._location.repair_authority_table_name
        consumption = self._location.repair_consumption_table_name
        authority_hint = " WITH (UPDLOCK, HOLDLOCK)" if lock else ""
        consumption_hint = " WITH (UPDLOCK, HOLDLOCK)" if lock else ""
        return f"""
            SELECT TOP (1)
                a.authority_id, a.authority_digest, a.state_key,
                a.transfer_from_state_key, a.transfer_from_xmin, a.transfer_from_revision,
                a.expected_checkpoint_absent, a.expected_xmin, a.expected_revision,
                a.scope_hash, a.reason, a.expires_at_utc, a.allow_full_baseline,
                a.allow_max_delete_rows, a.allow_max_delete_ratio,
                SYSUTCDATETIME() AS current_utc,
                CONVERT(bit, CASE WHEN c.authority_id IS NULL THEN 0 ELSE 1 END) AS already_consumed
            FROM {authority} AS a{authority_hint}
            LEFT JOIN {consumption} AS c{consumption_hint}
              ON c.authority_id = a.authority_id
            WHERE a.authority_id = ?
        """

    def _consumption_sql(self) -> str:
        authority = self._location.repair_authority_table_name
        consumption = self._location.repair_consumption_table_name
        receipt = self._location.receipt_table_name
        return f"""
            IF NOT EXISTS (
                SELECT 1 FROM {authority} WITH (UPDLOCK, HOLDLOCK)
                WHERE authority_id = ? AND authority_digest = ? AND state_key = ?
            ) THROW 51000, 'DPONE_REPAIR_AUTHORITY_CHANGED', 1;
            IF NOT EXISTS (
                SELECT 1 FROM {receipt} WITH (UPDLOCK, HOLDLOCK)
                WHERE load_id = ? AND receipt_id = ? AND state_key = ?
            ) THROW 51000, 'DPONE_REPAIR_AUTHORITY_RECEIPT_MISMATCH', 1;
            IF EXISTS (
                SELECT 1 FROM {consumption} WITH (UPDLOCK, HOLDLOCK)
                WHERE authority_id = ?
            ) THROW 51000, 'DPONE_REPAIR_AUTHORITY_ALREADY_CONSUMED', 1;
            INSERT INTO {consumption} (
                authority_id, authority_digest, state_key, load_id, receipt_id,
                used_full_baseline, observed_delete_rows, observed_delete_ratio,
                consumed_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """


def _utc_datetime(value: Any, code: str) -> datetime:
    if not isinstance(value, datetime):
        raise RepairAuthorityError(code)
    if value.tzinfo is None:
        value = value.replace(tzinfo=_UTC)
    return value.astimezone(_UTC)


__all__ = ["MssqlRepairAuthorityService"]
