"""Create-only MSSQL store for full semantic-refresh activation authorities."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_activation_identity import (
    activation_deployment_identity_conflicts,
    activation_receipt_from_row,
    activation_receipt_query,
)
from dpone.adapters.semantic_refresh_mssql_activation_identity import (
    canonical_activation_json as _canonical_json,
)
from dpone.adapters.semantic_refresh_mssql_activation_identity import (
    normalized_activation_value as _normalized,
)
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
    SemanticRefreshActivationAuthoritySet,
)


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlActivationAuthorityError(RuntimeError):
    """Raised when a protected authority set is absent or differs on replay."""


class MssqlSemanticRefreshActivationAuthorityStore:
    """Persist the complete typed activation authority in one transaction."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        authority_store_ref: str,
        control_schema: str = "dpone_control",
    ) -> None:
        if not control_schema.replace("_", "a").isalnum() or not control_schema[0].isalpha():
            raise ValueError("control_schema must be a simple SQL identifier")
        if not isinstance(authority_store_ref, str) or not authority_store_ref.strip():
            raise ValueError("authority_store_ref must be a protected non-empty identifier")
        self._connection_factory = connection_factory
        self._authority_store_ref = authority_store_ref
        self._control_schema = control_schema

    def persist_exact(
        self,
        authority: SemanticRefreshActivationAuthoritySet,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Create all full documents or acknowledge an exact durable replay."""

        if not isinstance(authority, SemanticRefreshActivationAuthoritySet):
            raise TypeError("authority must be a typed activation authority set")
        receipt = SemanticRefreshActivationAuthorityReceipt.build(
            release_id=authority.release_id,
            deployment_id=authority.deployment_id,
            plan_bundle_sha256=authority.plan_bundle_sha256,
            authority_store_ref=self._authority_store_ref,
            baseline_receipts=authority.baseline_receipts,
            route_certification_receipt_sha256=(authority.route_certification.route_certification_receipt_sha256),
            runtime_assurance_receipts=authority.runtime_assurance_receipts,
            persisted_at=authority.persisted_at,
        )
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            self._lock(cursor, authority.deployment_id)
            self._require_deployment_identity(cursor, authority)
            self._persist_documents(cursor, authority)
            self._persist_receipt(cursor, receipt)
            connection.commit()
            return receipt
        except SemanticRefreshMssqlActivationAuthorityError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlActivationAuthorityError(
                "semantic-refresh activation authority persistence failed"
            ) from exc
        finally:
            _close(cursor)
            _close(connection)

    def load_exact(
        self,
        *,
        release_id: str,
        deployment_id: str,
        plan_bundle_sha256: str,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Load and authenticate one create-only deployment receipt."""

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            receipt = self.load_exact_in_transaction(
                cursor,
                release_id=release_id,
                deployment_id=deployment_id,
                plan_bundle_sha256=plan_bundle_sha256,
            )
            connection.commit()
            return receipt
        except SemanticRefreshMssqlActivationAuthorityError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlActivationAuthorityError(
                "semantic-refresh activation authority load failed"
            ) from exc
        finally:
            _close(cursor)
            _close(connection)

    def load_exact_in_transaction(
        self,
        cursor: _Cursor,
        *,
        release_id: str,
        deployment_id: str,
        plan_bundle_sha256: str,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Authenticate one receipt while its caller owns the transaction."""

        cursor.execute(
            activation_receipt_query(self._table("semantic_refresh_activation_authorities")),
            deployment_id,
            plan_bundle_sha256,
        )
        row = cursor.fetchone()
        if row is None or cursor.fetchone() is not None:
            raise SemanticRefreshMssqlActivationAuthorityError(
                "ACTIVE deployment authority receipt is absent or ambiguous"
            )
        try:
            return activation_receipt_from_row(
                row,
                deployment_id=deployment_id,
                release_id=release_id,
                plan_bundle_sha256=plan_bundle_sha256,
                authority_store_ref=self._authority_store_ref,
            )
        except (TypeError, ValueError) as exc:
            raise SemanticRefreshMssqlActivationAuthorityError(
                "deployment authority receipt differs from protected storage"
            ) from exc

    def _persist_documents(self, cursor: _Cursor, authority: SemanticRefreshActivationAuthoritySet) -> None:
        for baseline in authority.baselines:
            self._insert_absent_exact(
                cursor,
                table="semantic_refresh_baselines",
                key_sql="target_resource_id = ?",
                key=(baseline.mssql_relation_id,),
                columns=(
                    "model_unique_id",
                    "baseline_kind",
                    "baseline_receipt_sha256",
                    "baseline_receipt_json",
                    "status",
                    "is_current",
                ),
                expected=(
                    baseline.model_unique_id,
                    baseline.baseline_kind.value,
                    baseline.baseline_adoption_receipt_sha256,
                    _canonical_json(baseline.to_dict()),
                    "COMPLETE",
                    True,
                ),
            )
        route = authority.route_certification
        self._insert_absent_exact(
            cursor,
            table="semantic_refresh_route_authorities",
            key_sql="deployment_id = ?",
            key=(authority.deployment_id,),
            columns=(
                "route_certification_receipt_sha256",
                "receipt_json",
                "status",
                "effective_from",
                "expires_at",
                "revoked_receipt_sha256",
            ),
            expected=(
                route.route_certification_receipt_sha256,
                _canonical_json(route.to_dict()),
                route.status.value,
                route.effective_from,
                route.expires_at,
                route.revoked_receipt_sha256,
            ),
        )
        for item in authority.runtime_assurances:
            self._insert_absent_exact(
                cursor,
                table="semantic_refresh_runtime_assurance_authorities",
                key_sql="deployment_id = ? AND model_unique_id = ? AND assurance_kind = ?",
                key=(
                    authority.deployment_id,
                    item.subject.model_unique_id,
                    item.subject.assurance_kind.value,
                ),
                columns=(
                    "runtime_assurance_receipt_sha256",
                    "subject_json",
                    "receipt_json",
                    "status",
                    "effective_from",
                    "expires_at",
                    "revoked_receipt_sha256",
                ),
                expected=(
                    item.runtime_assurance_receipt_sha256,
                    _canonical_json(item.subject.to_dict()),
                    _canonical_json(item.to_dict()),
                    item.status.value,
                    item.effective_from,
                    item.expires_at,
                    item.revoked_receipt_sha256,
                ),
            )

    def _require_deployment_identity(
        self,
        cursor: _Cursor,
        authority: SemanticRefreshActivationAuthoritySet,
    ) -> None:
        if activation_deployment_identity_conflicts(
            cursor,
            activation_authority_table=self._table("semantic_refresh_activation_authorities"),
            deployment_id=authority.deployment_id,
            release_id=authority.release_id,
            authority_store_ref=self._authority_store_ref,
        ):
            raise SemanticRefreshMssqlActivationAuthorityError(
                "deployment identity differs across activation authority receipts"
            )

    def _persist_receipt(
        self,
        cursor: _Cursor,
        receipt: SemanticRefreshActivationAuthorityReceipt,
    ) -> None:
        self._insert_absent_exact(
            cursor,
            table="semantic_refresh_activation_authorities",
            key_sql=(
                "deployment_id COLLATE Latin1_General_100_BIN2 "
                "= CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2 AND "
                "plan_bundle_sha256 COLLATE Latin1_General_100_BIN2 "
                "= CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2"
            ),
            key=(receipt.deployment_id, receipt.plan_bundle_sha256),
            columns=(
                "release_id",
                "authority_store_ref",
                "authority_receipt_json",
                "activation_authority_receipt_sha256",
                "persisted_at",
                "status",
            ),
            expected=(
                receipt.release_id,
                receipt.authority_store_ref,
                _canonical_json(receipt.to_dict()),
                receipt.activation_authority_receipt_sha256,
                receipt.persisted_at,
                "ACTIVE",
            ),
        )

    def _insert_absent_exact(
        self,
        cursor: _Cursor,
        *,
        table: str,
        key_sql: str,
        key: tuple[object, ...],
        columns: tuple[str, ...],
        expected: tuple[object, ...],
    ) -> None:
        projection = ", ".join(columns)
        cursor.execute(
            f"SELECT {projection} FROM {self._table(table)} WITH (UPDLOCK, HOLDLOCK) WHERE {key_sql};",
            *key,
        )
        row = cursor.fetchone()
        if row is not None:
            if tuple(_normalized(value) for value in row) != tuple(_normalized(value) for value in expected):
                raise SemanticRefreshMssqlActivationAuthorityError(f"{table} replay differs")
            return
        insert_columns = _key_columns(key_sql) + columns
        placeholders = ", ".join("?" for _ in insert_columns)
        cursor.execute(
            f"INSERT INTO {self._table(table)} ({', '.join(insert_columns)}) VALUES ({placeholders});",
            *key,
            *expected,
        )

    def _lock(self, cursor: _Cursor, deployment_id: str) -> None:
        cursor.execute(
            """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
            f"dpone:semantic-refresh:activation-authority:{deployment_id}",
        )
        row = cursor.fetchone()
        if row is None or isinstance(row[0], bool) or not isinstance(row[0], int) or row[0] < 0:
            raise SemanticRefreshMssqlActivationAuthorityError("activation authority lock was not acquired")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _key_columns(key_sql: str) -> tuple[str, ...]:
    return tuple(part.strip().split(" ", 1)[0] for part in key_sql.split("AND"))


def _rollback(connection: _Connection | None) -> None:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


def _close(resource: object | None) -> None:
    if resource is not None:
        try:
            resource.close()  # type: ignore[attr-defined]
        except Exception:
            pass


__all__ = [
    "MssqlSemanticRefreshActivationAuthorityStore",
    "SemanticRefreshMssqlActivationAuthorityError",
]
