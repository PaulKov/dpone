"""Transactional ownership guard for one physical MSSQL snapshot target."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.contracts.repair_authority import RepairAuthority
    from dpone.ports.source_state_storage import MssqlStateLocation, SourceStateKey


class MssqlTargetAuthorityService:
    """Assert one active owner or supersede it under an approved baseline."""

    def __init__(self, location: MssqlStateLocation) -> None:
        self._location = location

    def assert_or_transfer(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        authority: RepairAuthority | None,
    ) -> None:
        """Keep normal ownership strict and make an approved transfer one-shot."""

        transfer = authority.transfer_from if authority is not None else None
        if transfer is None:
            self.assert_current(executor=executor, key=key)
            return
        if authority is None or authority.state_key != key.digest:
            raise RuntimeError("repair_authority.transfer_binding_invalid")
        executor.execute_query(
            self._transfer_sql(),
            (
                key.target_identity,
                key.digest,
                transfer.state_key,
                transfer.xmin,
                transfer.revision,
            ),
        )

    def assert_current(self, *, executor: Any, key: SourceStateKey) -> None:
        """Reject any other non-superseded owner of the physical target."""

        executor.execute_query(
            f"""
            IF EXISTS (
                SELECT 1
                FROM {self._location.state_table_name} WITH (UPDLOCK, HOLDLOCK)
                WHERE target_identity = ?
                  AND superseded_at_utc IS NULL
                  AND state_key <> ?
            )
                THROW 51000, 'DPONE_XMIN_TARGET_AUTHORITY_CONFLICT', 1;
            """,
            (key.target_identity, key.digest),
        )

    def _transfer_sql(self) -> str:
        state = self._location.state_table_name
        return f"""
            SET NOCOUNT ON;
            DECLARE @target_identity binary(32) = ?;
            DECLARE @new_state_key binary(32) = ?;
            DECLARE @old_state_key binary(32) = ?;
            DECLARE @old_xmin bigint = ?;
            DECLARE @old_revision bigint = ?;
            DECLARE @active_owner_count bigint;

            IF EXISTS (
                SELECT 1 FROM {state} WITH (UPDLOCK, HOLDLOCK)
                WHERE state_key = @new_state_key
            )
                THROW 51000, 'DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_DESTINATION_EXISTS', 1;

            SELECT @active_owner_count = COUNT_BIG(*)
            FROM {state} WITH (UPDLOCK, HOLDLOCK)
            WHERE target_identity = @target_identity
              AND superseded_at_utc IS NULL;

            IF @active_owner_count = 0
                THROW 51000, 'DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_NOT_REQUIRED', 1;
            IF @active_owner_count <> 1
                THROW 51000, 'DPONE_XMIN_TARGET_AUTHORITY_CONFLICT', 1;
            IF NOT EXISTS (
                SELECT 1 FROM {state} WITH (UPDLOCK, HOLDLOCK)
                WHERE state_key = @old_state_key
                  AND target_identity = @target_identity
                  AND xmin_value = @old_xmin
                  AND state_revision = @old_revision
                  AND superseded_at_utc IS NULL
            )
                THROW 51000, 'DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_BINDING_MISMATCH', 1;

            UPDATE {state} WITH (UPDLOCK, HOLDLOCK)
            SET superseded_at_utc = SYSUTCDATETIME(),
                superseded_by_state_key = @new_state_key,
                __dpone__updated_at = SYSUTCDATETIME()
            WHERE state_key = @old_state_key
              AND target_identity = @target_identity
              AND xmin_value = @old_xmin
              AND state_revision = @old_revision
              AND superseded_at_utc IS NULL;
            IF @@ROWCOUNT <> 1
                THROW 51000, 'DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_RACE', 1;
        """


def require_active_target_authority_index(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
) -> None:
    """Require the filtered unique index that physically enforces one owner."""

    from dpone.runtime.state.mssql_catalog_integrity import (
        MssqlIndexContract,
        require_table_index_integrity,
    )

    try:
        require_table_index_integrity(
            connector,
            database=database,
            schema=schema,
            table=table,
            indexes=(
                MssqlIndexContract(
                    kind="unique_index",
                    columns=("target_identity",),
                    clustered=False,
                    filter_expression="[superseded_at_utc] IS NULL",
                ),
            ),
        )
    except RuntimeError as exc:
        label = f"{database}.{schema}.{table}"
        raise RuntimeError(f"mssql_external_active_target_authority_index:{label}") from exc


__all__ = ["MssqlTargetAuthorityService", "require_active_target_authority_index"]
