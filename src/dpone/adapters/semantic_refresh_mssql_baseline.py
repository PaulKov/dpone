"""Create-only MSSQL store for canonical complete baseline receipts."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

from dpone.contracts.semantic_refresh_baseline_receipt import (
    SemanticRefreshBaselineAdoptionReceipt,
)
from dpone.ports.semantic_refresh_mssql_baseline import (
    validate_mssql_baseline_receipt_identity,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_T = TypeVar("_T")


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


class SemanticRefreshMssqlBaselineReceiptError(RuntimeError):
    """Raised when a complete baseline receipt is absent, stale, or conflicting."""


class MssqlSemanticRefreshBaselineReceiptStore:
    """Persist canonical receipts in the existing baseline control table."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def persist_exact(
        self,
        receipt: SemanticRefreshBaselineAdoptionReceipt,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        """Create or exact-replay one typed COMPLETE receipt."""

        validate_mssql_baseline_receipt_identity(receipt)
        return self._transaction(
            lambda cursor: self._persist(cursor, receipt),
            "baseline receipt persistence failed",
        )

    def load_exact(
        self,
        *,
        mssql_connection_authority_id: str,
        mssql_relation_id: str,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        """Load one current COMPLETE receipt and recompute its canonical digest."""

        _text(mssql_connection_authority_id, "mssql_connection_authority_id")
        _text(mssql_relation_id, "mssql_relation_id")
        return self._transaction(
            lambda cursor: self._load(
                cursor,
                mssql_connection_authority_id=mssql_connection_authority_id,
                mssql_relation_id=mssql_relation_id,
            ),
            "baseline receipt load failed",
        )

    def _persist(
        self,
        cursor: _Cursor,
        receipt: SemanticRefreshBaselineAdoptionReceipt,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        existing = self._select(cursor, receipt.mssql_relation_id)
        expected = _row(receipt)
        if existing is not None:
            if existing != expected:
                raise SemanticRefreshMssqlBaselineReceiptError("baseline receipt replay differs")
            return receipt
        cursor.execute(
            f"""
INSERT INTO {self._table()} (
    target_resource_id, model_unique_id, baseline_kind,
    baseline_receipt_sha256, baseline_receipt_json, status, is_current
)
OUTPUT inserted.baseline_receipt_sha256
VALUES (?, ?, ?, ?, ?, N'COMPLETE', 1);
""".strip(),
            receipt.mssql_relation_id,
            *_row(receipt)[:4],
        )
        inserted = cursor.fetchone()
        if inserted is None or tuple(inserted) != (receipt.baseline_adoption_receipt_sha256,):
            raise SemanticRefreshMssqlBaselineReceiptError("baseline receipt insert was not exact")
        return receipt

    def _load(
        self,
        cursor: _Cursor,
        *,
        mssql_connection_authority_id: str,
        mssql_relation_id: str,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        row = self._select(cursor, mssql_relation_id)
        if row is None:
            raise SemanticRefreshMssqlBaselineReceiptError("baseline receipt is absent")
        try:
            receipt = SemanticRefreshBaselineAdoptionReceipt.from_mapping(json.loads(str(row[3])))
            validate_mssql_baseline_receipt_identity(receipt)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SemanticRefreshMssqlBaselineReceiptError("baseline receipt document is invalid") from exc
        if (
            _row(receipt) != row
            or receipt.mssql_relation_id != mssql_relation_id
            or receipt.mssql_connection_authority_id != mssql_connection_authority_id
        ):
            raise SemanticRefreshMssqlBaselineReceiptError("baseline receipt durable authority differs")
        return receipt

    def _select(self, cursor: _Cursor, target_resource_id: str) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT model_unique_id, baseline_kind, baseline_receipt_sha256,
       baseline_receipt_json, status, is_current
FROM {self._table()} WITH (UPDLOCK, HOLDLOCK)
WHERE target_resource_id = ?;
""".strip(),
            target_resource_id,
        )
        value = cursor.fetchone()
        return None if value is None else tuple(value)

    def _transaction(self, action: Callable[[_Cursor], _T], label: str) -> _T:
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            result = action(cursor)
            connection.commit()
            return result
        except SemanticRefreshMssqlBaselineReceiptError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlBaselineReceiptError(label) from exc
        finally:
            _close(cursor)
            _close(connection)

    def _table(self) -> str:
        return f"[{self._control_schema}].[semantic_refresh_baselines]"


def _row(receipt: SemanticRefreshBaselineAdoptionReceipt) -> tuple[object, ...]:
    return (
        receipt.model_unique_id,
        receipt.baseline_kind.value,
        receipt.baseline_adoption_receipt_sha256,
        json.dumps(receipt.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        "COMPLETE",
        True,
    )


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty text")
    return value


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
    "MssqlSemanticRefreshBaselineReceiptStore",
    "SemanticRefreshMssqlBaselineReceiptError",
]
