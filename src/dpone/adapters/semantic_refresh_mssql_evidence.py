"""Thin transactionally consistent reader for MSSQL producer evidence."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.ports.semantic_refresh_mssql_evidence import (
    MssqlBuildReceiptEvidence,
    MssqlImageEvidence,
    MssqlOperationEvidence,
    MssqlTransactionDisposition,
    mssql_session_evidence_sha256,
)
from dpone.ports.semantic_refresh_mssql_primitives import MssqlImageKeyColumn

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlEvidenceReadError(RuntimeError):
    """Raised when admitted evidence identity cannot be read consistently."""


class MssqlSemanticRefreshEvidenceReader:
    """Read journal, receipt, image presence and session outcome in one snapshot."""

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

    def read_operation_evidence(
        self,
        *,
        operation_id: str,
        attempt_binding_sha256: str,
    ) -> MssqlOperationEvidence:
        """Return durable evidence without inferring a terminal model outcome."""

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            plan_sha256, fencing_epoch, image_key_columns = self._journal_identity(
                cursor,
                operation_id,
                attempt_binding_sha256,
            )
            receipt_row = self._receipt(cursor, operation_id)
            receipt, before, after = self._artifacts(cursor, receipt_row, image_key_columns)
            disposition, not_invoked = self._session_outcome(
                cursor,
                operation_id,
                plan_sha256,
                attempt_binding_sha256,
                fencing_epoch,
            )
            connection.commit()
            return MssqlOperationEvidence(
                operation_id=operation_id,
                operation_plan_sha256=plan_sha256,
                attempt_binding_sha256=attempt_binding_sha256,
                fencing_epoch=fencing_epoch,
                database_available=True,
                controller_proves_not_invoked=not_invoked,
                transaction_disposition=disposition,
                receipt=receipt,
                before_image=before,
                after_image=after,
            )
        except Exception as exc:
            if connection is not None:
                connection.rollback()
            if isinstance(exc, SemanticRefreshMssqlEvidenceReadError):
                raise
            raise SemanticRefreshMssqlEvidenceReadError("MSSQL operation evidence read failed") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def _journal_identity(
        self,
        cursor: _Cursor,
        operation_id: str,
        attempt_binding_sha256: str,
    ) -> tuple[str, int, tuple[MssqlImageKeyColumn, ...]]:
        cursor.execute(
            f"""
SELECT operation_plan_sha256, fencing_epoch, image_key_columns_json
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ? AND attempt_binding_sha256 = ?;
""".strip(),
            operation_id,
            attempt_binding_sha256,
        )
        row = cursor.fetchone()
        if row is None:
            raise SemanticRefreshMssqlEvidenceReadError("admitted operation evidence identity is absent")
        plan_sha256, fencing_epoch, image_key_columns_json = tuple(row)
        if not isinstance(plan_sha256, str) or isinstance(fencing_epoch, bool) or not isinstance(fencing_epoch, int):
            raise SemanticRefreshMssqlEvidenceReadError("admitted operation evidence identity is invalid")
        image_key_columns = _parse_image_key_columns(image_key_columns_json)
        return plan_sha256, fencing_epoch, image_key_columns

    def _receipt(self, cursor: _Cursor, operation_id: str) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT receipt.operation_id, receipt.operation_plan_sha256,
       receipt.attempt_binding_sha256, receipt.fencing_epoch,
       receipt.before_image_relation, receipt.before_image_sha256,
       receipt.after_image_relation, receipt.after_image_sha256,
       receipt.inserted_count, receipt.updated_count, receipt.build_receipt_sha256,
       journal.model_unique_id, journal.strategy_authority_sha256
FROM {self._table("semantic_refresh_receipts")} AS receipt WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
  ON journal.operation_id = receipt.operation_id
 AND journal.operation_plan_sha256 = receipt.operation_plan_sha256
 AND journal.attempt_binding_sha256 = receipt.attempt_binding_sha256
 AND journal.fencing_epoch = receipt.fencing_epoch
WHERE receipt.operation_id = ?;
""".strip(),
            operation_id,
        )
        row = cursor.fetchone()
        return None if row is None else tuple(row)

    def _artifacts(
        self,
        cursor: _Cursor,
        row: tuple[Any, ...] | None,
        image_key_columns: tuple[MssqlImageKeyColumn, ...],
    ) -> tuple[MssqlBuildReceiptEvidence | None, MssqlImageEvidence | None, MssqlImageEvidence | None]:
        if row is None:
            return None, None, None
        (
            operation_id,
            plan,
            attempt,
            fence,
            before_relation,
            before_sha,
            after_relation,
            after_sha,
            inserted,
            updated,
            build_receipt_sha256,
            model_unique_id,
            strategy_authority_sha256,
        ) = row
        receipt = MssqlBuildReceiptEvidence(
            operation_id=str(operation_id),
            operation_plan_sha256=str(plan),
            attempt_binding_sha256=str(attempt),
            fencing_epoch=int(fence),
            before_image_sha256=str(before_sha),
            after_image_sha256=str(after_sha),
            inserted_count=int(inserted),
            updated_count=int(updated),
            model_unique_id=str(model_unique_id),
            strategy_authority_sha256=str(strategy_authority_sha256),
            before_image_relation=str(before_relation),
            after_image_relation=str(after_relation),
            build_receipt_sha256=str(build_receipt_sha256),
        )
        before = self._verified_image(
            cursor,
            receipt,
            role="BEFORE",
            relation=str(before_relation),
            expected_sha256=str(before_sha),
            image_key_columns=image_key_columns,
        )
        after = self._verified_image(
            cursor,
            receipt,
            role="AFTER",
            relation=str(after_relation),
            expected_sha256=str(after_sha),
            image_key_columns=image_key_columns,
        )
        return receipt, before, after

    def _verified_image(
        self,
        cursor: _Cursor,
        receipt: MssqlBuildReceiptEvidence,
        *,
        role: str,
        relation: str,
        expected_sha256: str,
        image_key_columns: tuple[MssqlImageKeyColumn, ...],
    ) -> MssqlImageEvidence | None:
        quoted_relation, object_name, catalog_columns = _relation_names(relation)
        cursor.execute(
            f"SELECT name FROM {catalog_columns} WHERE object_id = OBJECT_ID(?, N'U') ORDER BY column_id;",
            object_name,
        )
        columns = tuple(str(row[0]) for row in cursor.fetchall())
        if not columns:
            return None
        if not {item.name for item in image_key_columns}.issubset(columns):
            raise SemanticRefreshMssqlEvidenceReadError("image key columns differ from admitted authority")
        column_sql = ", ".join(_quoted_identifier(column) for column in columns)
        key_sql = ", ".join(_image_order_expression(column) for column in image_key_columns)
        cursor.execute(
            f"""
DECLARE @dpone_image_json nvarchar(max);
SELECT @dpone_image_json = (
    SELECT {column_sql} FROM {quoted_relation}
    ORDER BY {key_sql} FOR JSON PATH, INCLUDE_NULL_VALUES
);
SELECT 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES(
           'SHA2_256', COALESCE(@dpone_image_json, N'[]')
       ), 2)),
       (SELECT COUNT_BIG(*) FROM {quoted_relation});
""".strip()
        )
        row = cursor.fetchone()
        if row is None or row[0] != expected_sha256:
            raise SemanticRefreshMssqlEvidenceReadError("durable image digest differs")
        row_count = row[1]
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
            raise SemanticRefreshMssqlEvidenceReadError("durable image row count is invalid")
        return _image(receipt, role, str(row[0]), row_count)

    def _session_outcome(
        self,
        cursor: _Cursor,
        operation_id: str,
        plan_sha256: str,
        attempt_binding_sha256: str,
        fencing_epoch: int,
    ) -> tuple[MssqlTransactionDisposition, bool]:
        cursor.execute(
            f"""
SELECT operation_plan_sha256, attempt_binding_sha256, fencing_epoch,
       transaction_disposition, controller_proves_not_invoked, evidence_sha256
FROM {self._table("semantic_refresh_mssql_session_outcomes")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            operation_id,
        )
        row = cursor.fetchone()
        if row is None:
            return MssqlTransactionDisposition.UNKNOWN, False
        if tuple(row[:3]) != (plan_sha256, attempt_binding_sha256, fencing_epoch):
            return MssqlTransactionDisposition.UNKNOWN, False
        try:
            disposition = MssqlTransactionDisposition(str(row[3]))
        except ValueError:
            return MssqlTransactionDisposition.UNKNOWN, False
        if row[4] not in {False, True, 0, 1}:
            raise SemanticRefreshMssqlEvidenceReadError("session evidence boolean is invalid")
        not_invoked = bool(row[4])
        expected_sha256 = mssql_session_evidence_sha256(
            operation_id=operation_id,
            operation_plan_sha256=plan_sha256,
            attempt_binding_sha256=attempt_binding_sha256,
            fencing_epoch=fencing_epoch,
            transaction_disposition=disposition,
            controller_proves_not_invoked=not_invoked,
        )
        if row[5] != expected_sha256:
            raise SemanticRefreshMssqlEvidenceReadError("session evidence digest differs")
        return disposition, not_invoked

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _image(
    receipt: MssqlBuildReceiptEvidence,
    role: str,
    digest: str,
    row_count: int,
) -> MssqlImageEvidence:
    return MssqlImageEvidence(
        operation_id=receipt.operation_id,
        operation_plan_sha256=receipt.operation_plan_sha256,
        attempt_binding_sha256=receipt.attempt_binding_sha256,
        fencing_epoch=receipt.fencing_epoch,
        image_role=role,
        image_sha256=digest,
        row_count=row_count,
        committed=True,
    )


def _parse_image_key_columns(value: object) -> tuple[MssqlImageKeyColumn, ...]:
    if not isinstance(value, str):
        raise SemanticRefreshMssqlEvidenceReadError("journal image key columns are invalid")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise SemanticRefreshMssqlEvidenceReadError("journal image key columns are invalid") from exc
    if not isinstance(parsed, list) or not parsed:
        raise SemanticRefreshMssqlEvidenceReadError("journal image key columns are invalid")
    try:
        columns = tuple(
            MssqlImageKeyColumn(
                name=item["name"],
                order_encoding=item["order_encoding"],
            )
            for item in parsed
            if isinstance(item, dict) and set(item) == {"name", "order_encoding"}
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SemanticRefreshMssqlEvidenceReadError("journal image key columns are invalid") from exc
    if len(columns) != len(parsed) or len({item.name for item in columns}) != len(columns):
        raise SemanticRefreshMssqlEvidenceReadError("journal image key columns are invalid")
    return columns


def _relation_names(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str) or not value.strip():
        raise SemanticRefreshMssqlEvidenceReadError("image relation is invalid")
    parts = tuple(_unquote_identifier(item.strip()) for item in value.split("."))
    if len(parts) not in {2, 3} or any(_IDENTIFIER.fullmatch(item) is None for item in parts):
        raise SemanticRefreshMssqlEvidenceReadError("image relation is invalid")
    catalog_columns = f"{_quoted_identifier(parts[0])}.sys.columns" if len(parts) == 3 else "sys.columns"
    return (
        ".".join(_quoted_identifier(item) for item in parts),
        ".".join(parts),
        catalog_columns,
    )


def _unquote_identifier(value: str) -> str:
    if len(value) >= 2 and ((value[0], value[-1]) in {("[", "]"), ('"', '"')}):
        return value[1:-1]
    return value


def _quoted_identifier(value: str) -> str:
    return f"[{value.replace(']', ']]')}]"


def _image_order_expression(column: MssqlImageKeyColumn) -> str:
    quoted = _quoted_identifier(column.name)
    if column.order_encoding == "UUID_TEXT":
        return f"CONVERT(char(36), {quoted})"
    return quoted


__all__ = [
    "MssqlSemanticRefreshEvidenceReader",
    "SemanticRefreshMssqlEvidenceReadError",
]
