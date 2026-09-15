"""Immutable native-original bindings in an administrator-installed SQL ledger."""

from __future__ import annotations

import re
from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_BYTES
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeOriginalBinding,
    NativeOriginalKind,
    NativeOriginalSubject,
    decode_native_original_binding,
    encode_native_original_binding,
    encode_native_original_subject,
    require_native_original_kind,
)
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class NativeOriginalBindingError(RuntimeError):
    """The protected ledger did not prove the complete requested binding."""


def native_control_schema(value: str) -> str:
    """Validate an interpolated SQL identifier; values always use parameters."""
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value) is None:
        raise ValueError("control_schema must be a simple SQL identifier of at most 128 characters")
    return value


class MssqlNativeOriginalBindings:
    """Create once, then resolve through a fresh independently authenticated connection.

    The injected factory must select the authenticated control database and a
    least-privilege registered principal, using finite connect/statement timeouts.
    It returns a fresh dedicated connection per call. This adapter never installs
    its own schema or authority and never repeats an ambiguous mutation.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        control_schema: str,
        control_authority: OriginalRef,
        max_binding_bytes: int,
    ) -> None:
        if type(control_authority) is not OriginalRef:
            raise TypeError("control_authority must be an authenticated OriginalRef")
        if type(max_binding_bytes) is not int or not 0 < max_binding_bytes <= MAX_NATIVE_JSON_BYTES:
            raise ValueError("max_binding_bytes must be a positive integer within the native JSON limit")
        self._schema = native_control_schema(control_schema)
        self._authority = OriginalRef(control_authority.locator, control_authority.sha256)
        self._connect = connection_factory
        self._max_bytes = max_binding_bytes

    def bind(self, binding: NativeOriginalBinding) -> OriginalRef:
        """Persist the complete tuple or prove its exact immutable replay."""
        expected = encode_native_original_binding(binding)
        if len(expected) > self._max_bytes:
            raise ValueError("canonical binding exceeds max_binding_bytes")
        frozen = decode_native_original_binding(expected)
        reference = OriginalRef(frozen.locator, frozen.payload_sha256)
        parameters = self._parameters(reference, frozen.subject, frozen.kind)
        failure: Exception | None = None
        try:
            observed = self._execute("bind", (*parameters, expected))
            if observed != expected:
                raise NativeOriginalBindingError("bind response differs from the complete requested tuple")
        except Exception as exc:
            # Execute/commit may have succeeded. Cleanup alone cannot prove absence.
            failure = exc
        try:
            resolved = self.resolve(reference, expected_subject=frozen.subject, expected_kind=frozen.kind)
            if encode_native_original_binding(resolved) != expected:
                raise NativeOriginalBindingError("independent binding differs from the complete requested tuple")
        except Exception as exc:
            if failure is not None:
                raise NativeOriginalBindingError("binding outcome could not be independently reconciled") from failure
            raise NativeOriginalBindingError("independent binding verification failed") from exc
        return reference

    def resolve(
        self, reference: OriginalRef, *, expected_subject: NativeOriginalSubject, expected_kind: NativeOriginalKind
    ) -> NativeOriginalBinding:
        """Resolve one retained tuple; authenticate every expected coordinate."""
        parameters = self._parameters(reference, expected_subject, expected_kind)
        payload = self._execute("resolve", parameters)
        value = decode_native_original_binding(payload)
        if (
            value.locator.encode("utf-8") != parameters[2]
            or value.payload_sha256.encode("ascii") != parameters[3]
            or encode_native_original_subject(value.subject) != parameters[4]
            or value.kind.encode("ascii") != parameters[5]
        ):
            raise NativeOriginalBindingError("resolved tuple differs from requested identity")
        return value

    def _parameters(
        self, reference: OriginalRef, subject: NativeOriginalSubject, kind: NativeOriginalKind
    ) -> tuple[object, ...]:
        if type(reference) is not OriginalRef:
            raise TypeError("reference must be an OriginalRef")
        frozen = OriginalRef(reference.locator, reference.sha256)
        require_native_original_kind(kind)
        return (
            self._authority.locator.encode("utf-8"),
            self._authority.sha256.encode("ascii"),
            frozen.locator.encode("utf-8"),
            frozen.sha256.encode("ascii"),
            encode_native_original_subject(subject),
            kind.encode("ascii"),
            self._max_bytes,
        )

    def _execute(self, operation: str, parameters: tuple[object, ...]) -> bytes:
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            placeholders = ", ".join("?" for _ in parameters)
            cursor.execute(f"EXEC [{self._schema}].[native_original_{operation}_v1] {placeholders}", *parameters)
            row = dbapi_lifecycle.row(cursor)
            if row is None or len(row) != 1 or type(row[0]) is not bytes:
                raise NativeOriginalBindingError("ledger must return exactly one canonical binding bytes row")
            payload = row[0]
            if len(payload) > self._max_bytes or dbapi_lifecycle.row(cursor) is not None:
                raise NativeOriginalBindingError("ledger response exceeds its bounded single-row contract")
            if encode_native_original_binding(decode_native_original_binding(payload)) != payload:
                raise NativeOriginalBindingError("ledger returned noncanonical binding bytes")
            connection.commit()
            return payload
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
