"""Pinned optional Microsoft bulk-copy adapter with independent input authority.

Successful return confirms consumed input and reported SDK count only. SQL object,
content and publication verification remain coordinator responsibilities. The SDK
uses a hidden bulk connection and suppresses some teardown failures: the enclosing
worker must exit and be reaped before any containment claim or retry.
"""

from __future__ import annotations

import importlib
from importlib import metadata
from typing import Any

from dpone.contracts.mssql_tds_api import TdsInputReceipt
from dpone.ports.mssql_tds_input import TdsBulkInput


def admit_tds_sdk(*, input_mode: str) -> None:
    """Check exact optional versions and native APIs without opening SQL clients."""
    if type(input_mode) is not str or input_mode not in {"rows", "arrow"}:
        raise ValueError("mssql_native.tds_invalid_input_mode")
    try:
        if metadata.version("mssql-python") != "1.13.0":
            raise ValueError
        if input_mode == "arrow" and metadata.version("pyarrow") != "25.0.1":
            raise ValueError
        cursor = importlib.import_module("mssql_python.cursor").Cursor
        core = importlib.import_module("mssql_py_core")
        method = "bulkcopy" if input_mode == "rows" else "bulkcopy_arrow"
        if not callable(getattr(cursor, method, None)) or not callable(getattr(core.PyCoreCursor, method, None)):
            raise ValueError
        if not callable(getattr(core, "PyCoreConnection", None)):
            raise ValueError
        if input_mode == "arrow":
            arrow = importlib.import_module("pyarrow")
            if not callable(getattr(arrow, "array", None)) or not callable(getattr(arrow, "RecordBatch", None)):
                raise ValueError
    except Exception:
        raise ValueError("mssql_native.tds_sdk_unavailable_or_unadmitted") from None


def copy_owned_stage(
    connection: Any,
    target: str,
    source: TdsBulkInput,
    *,
    input_mode: str,
    columns: tuple[str, ...],
    expected: TdsInputReceipt,
    batch_rows: int,
    timeout_seconds: int,
) -> TdsInputReceipt:
    """Copy exactly one input using explicit safe options; never coerce or retry.

    The caller supplies an already-authorized owned target and restricted writer
    connection. This function owns its cursor, but not the supplied connection.
    ``expected`` binds rows, encoded bytes and digest, independently of SDK output.
    Empty input still establishes complete EOF but opens no SQL cursor.
    Unclassified copy failures are permanent here. A caller must not infer
    retryability from ``tds_sdk_copy_failed`` or retry before proven retirement.
    """
    _validate_options(target, columns, expected, batch_rows, timeout_seconds)
    admit_tds_sdk(input_mode=input_mode)
    iterator: Any = None
    cursor: Any = None
    failed = False
    try:
        try:
            iterator = source.iter_rows() if input_mode == "rows" else source.iter_arrow_batches()
        except Exception:
            raise ValueError("mssql_native.tds_input_incomplete") from None
        if expected.rows == 0:
            try:
                for _ in iterator:
                    raise ValueError
            except Exception:
                raise ValueError("mssql_native.tds_input_incomplete") from None
        else:
            try:
                cursor = connection.cursor()
                method = cursor.bulkcopy if input_mode == "rows" else cursor.bulkcopy_arrow
                result = method(
                    target,
                    iterator,
                    batch_size=batch_rows,
                    timeout=timeout_seconds,
                    column_mappings=list(columns),
                    keep_identity=False,
                    check_constraints=True,
                    table_lock=True,
                    keep_nulls=True,
                    fire_triggers=True,
                    use_internal_transaction=True,
                )
            except Exception:
                raise ValueError("mssql_native.tds_sdk_copy_failed") from None
            receipt = _complete(source, expected)
            if (
                type(result) is not dict
                or type(result.get("rows_copied")) is not int
                or result["rows_copied"] != expected.rows
            ):
                raise ValueError("mssql_native.tds_sdk_result_mismatch")
            return receipt
        return _complete(source, expected)
    except BaseException:
        failed = True
        raise
    finally:
        # Preserve cancellation and the original failure if cleanup also fails.
        cleanup_failed = False
        cleanup_interrupt: BaseException | None = None
        for resource in (iterator, cursor):
            if resource is not None:
                try:
                    close = getattr(resource, "close", None)
                    if callable(close):
                        close()
                except BaseException as error:
                    cleanup_failed = True
                    if not isinstance(error, Exception) and cleanup_interrupt is None:
                        cleanup_interrupt = error
        if cleanup_interrupt is not None and not failed:
            raise cleanup_interrupt
        if cleanup_failed and not failed:
            raise ValueError("mssql_native.tds_sdk_cleanup_failed") from None


def _complete(source: TdsBulkInput, expected: TdsInputReceipt) -> TdsInputReceipt:
    try:
        receipt = source.require_complete()
    except Exception:
        raise ValueError("mssql_native.tds_input_incomplete") from None
    if type(receipt) is not TdsInputReceipt or receipt != expected:
        raise ValueError("mssql_native.tds_input_identity_mismatch")
    return receipt


def _validate_options(
    target: str, columns: tuple[str, ...], expected: TdsInputReceipt, batch_rows: int, timeout_seconds: int
) -> None:
    if type(target) is not str or not target or len(target) > 776 or "\0" in target:
        raise ValueError("mssql_native.tds_invalid_target")
    if (
        type(columns) is not tuple
        or not columns
        or len(columns) > 1024
        or any(type(c) is not str or not c or len(c) > 128 or "\0" in c for c in columns)
        or len(set(columns)) != len(columns)
    ):
        raise ValueError("mssql_native.tds_invalid_columns")
    if type(expected) is not TdsInputReceipt:
        raise ValueError("mssql_native.tds_invalid_expected_receipt")
    if type(batch_rows) is not int or not 1 <= batch_rows <= 65536:
        raise ValueError("mssql_native.tds_invalid_batch_rows")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError("mssql_native.tds_invalid_timeout")
