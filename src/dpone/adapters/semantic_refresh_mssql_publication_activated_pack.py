"""Read-only MSSQL authority for one run-bound activated Airflow pack."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Protocol

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class MssqlActivatedPackAuthorityError(RuntimeError):
    """Raised when the exact active pack/run tuple cannot be authenticated."""


class MssqlSemanticRefreshActivatedPackAuthority:
    """Authenticate an Airflow pack against its create-only MSSQL row."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if _IDENTIFIER_RE.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def assert_authorized(self, identity: object) -> None:
        """Require the full fingerprint, activation, plan, and logical-run tuple."""

        expected = (*_identity_values(identity), "ACTIVE")
        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                f"""
SELECT pack_fingerprint, activation_authority_receipt_sha256, authority_store_ref,
       workflow_execution_id, workflow_execution_binding_sha256,
       workflow_plan_sha256, plan_bundle_sha256,
       run_execution_bundle_sha256, status
FROM [{self._control_schema}].[semantic_refresh_activated_packs] WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
                expected[4],
            )
            row = cursor.fetchone()
            if row is None or tuple(row) != expected:
                raise MssqlActivatedPackAuthorityError("activated semantic-refresh pack authority differs or is absent")
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, MssqlActivatedPackAuthorityError):
                raise
            raise MssqlActivatedPackAuthorityError("activated semantic-refresh pack authority is unavailable") from exc
        finally:
            cursor.close()
            connection.close()


def _identity_values(identity: object) -> tuple[str, ...]:
    fields = (
        "pack_fingerprint",
        "activation_authority_receipt_sha256",
        "authority_store_ref",
        "workflow_execution_id",
        "workflow_execution_binding_sha256",
        "workflow_plan_sha256",
        "plan_bundle_sha256",
        "run_execution_bundle_sha256",
    )
    try:
        values = tuple(getattr(identity, field) for field in fields)
    except AttributeError as exc:
        raise MssqlActivatedPackAuthorityError("activated pack identity fields are incomplete") from exc
    if any(not isinstance(value, str) or not value for value in values):
        raise MssqlActivatedPackAuthorityError("activated pack identity fields are invalid")
    return values


__all__ = [
    "MssqlActivatedPackAuthorityError",
    "MssqlSemanticRefreshActivatedPackAuthority",
]
