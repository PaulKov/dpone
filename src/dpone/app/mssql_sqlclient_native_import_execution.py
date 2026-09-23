"""One-shot application boundary for a fresh SqlClient native chunk."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters.mssql_sqlclient_native_importer import SqlClientInputCustody
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.services.mssql_tds_writer_settlement import (
    SqlClientWriterVerified,
    project_sqlclient_native_chunk,
)


class SqlClientNativeImportExecution:
    """Retain input, run the exact P7-P10f chain and consume its terminal once."""

    def __init__(
        self,
        execute_terminal: Callable[
            [NativeChunkPlan, EncodedNativeFile, str, object, SqlClientInputCustody], SqlClientWriterVerified
        ],
    ) -> None:
        if not callable(execute_terminal):
            raise ValueError("mssql_native.sqlclient_import_execution_invalid")
        self._execute_terminal = execute_terminal

    def execute(
        self,
        plan: NativeChunkPlan,
        file: EncodedNativeFile,
        attempt_id: str,
        lease: object,
        custody: SqlClientInputCustody,
    ) -> SqlClientNativeChunkProjection:
        """Project only the opaque terminal returned by the injected exact chain."""
        if type(custody) is not SqlClientInputCustody:
            raise ValueError("mssql_native.sqlclient_import_execution_invalid")
        terminal = self._execute_terminal(plan, file, attempt_id, lease, custody)
        if not isinstance(terminal, SqlClientWriterVerified):
            raise ValueError("mssql_native.sqlclient_import_execution_invalid")
        return project_sqlclient_native_chunk(terminal)


__all__ = ("SqlClientNativeImportExecution",)
