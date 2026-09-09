"""Native SQL Server bulk helpers.

The helpers in this module are intentionally independent from ``pyodbc`` so
unit tests and lightweight CLI imports can validate bcp command construction
without requiring Microsoft ODBC drivers to be installed.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import replace

from dpone.runtime.connectors.mssql_bcp_dsn import (
    BcpDsnLease,
    BcpDsnOptions,
    issue_bcp_dsn,
)
from dpone.runtime.connectors.mssql_bcp_injected import run_injected_bcp_command
from dpone.runtime.connectors.mssql_bcp_process import (
    BcpCredentials,
    BcpOptions,
    BcpProcess,
    BcpProcessInput,
    BcpResult,
    BcpTimeoutError,
    parse_bcp_rows_copied,
    start_bcp_process,
)
from dpone.runtime.support.mssql_bcp_values import (
    DelimitedBulkFile,
    MssqlSpoolCapacityError,
    UnsafeBulkValueError,
)

# ODBC Driver 18 encrypts by default; TLS cannot handle bcp -a above ~16 KiB on Linux/macOS.
MSSQL_BCP_MAX_PACKET_SIZE_ODBC18 = 16_384
DEFAULT_MSSQL_BCP_TIMEOUT_SECONDS = 3600


def effective_mssql_bcp_packet_size(packet_size: int) -> int:
    """Return a network packet size safe for encrypted ODBC Driver 18 bcp sessions."""
    return min(int(packet_size), MSSQL_BCP_MAX_PACKET_SIZE_ODBC18)


class BcpRunner:
    """Small, injectable wrapper around Microsoft ``bcp``.

    Synchronous production commands use bounded ``Popen.communicate``; only
    asynchronous queryout uses concurrent pipe readers. A
    ``subprocess.run``-compatible callable remains injectable for command-only
    unit tests. Passwords are never exposed in ``BcpResult.redacted_command``
    or raised error messages.
    """

    def __init__(
        self,
        credentials: BcpCredentials,
        options: BcpOptions | None = None,
        run: Callable[..., object] | None = None,
    ) -> None:
        self.credentials = credentials
        requested = options or BcpOptions()
        if requested.timeout_seconds is None:
            requested = replace(requested, timeout_seconds=DEFAULT_MSSQL_BCP_TIMEOUT_SECONDS)
        if (
            isinstance(requested.timeout_seconds, bool)
            or not isinstance(requested.timeout_seconds, int)
            or requested.timeout_seconds < 1
        ):
            raise ValueError("MSSQL BCP timeout_seconds must be a positive integer")
        self.options = requested
        self._run = run

    def queryout(self, query: str, output_path: str) -> BcpResult:
        command = self.build_queryout_command(query, output_path)
        return self._execute(command)

    def queryout_process(
        self,
        query: str,
        output_path: str,
        *,
        progress_callback: Callable[[str], None] | None = None,
    ) -> BcpProcess:
        command = self.build_queryout_command(query, output_path)
        return self._start_process(
            command,
            progress_callback=progress_callback,
            drain_output=True,
        )

    def _start_process(
        self,
        command: list[str],
        *,
        progress_callback: Callable[[str], None] | None = None,
        process_input: BcpProcessInput | None = None,
        drain_output: bool,
    ) -> BcpProcess:
        """Start one drained BCP process and retain its DSN until completion."""

        stdin = self._stdin_password()
        lease = self._issue_connection_dsn()
        redact = process_input.redact if process_input is not None else str
        return start_bcp_process(
            command,
            redacted_command=tuple(redact(value) for value in self.redact_command(command)),
            stdin_text=stdin,
            timeout_seconds=int(self.options.timeout_seconds),
            environment=lease.environment if lease is not None else None,
            cleanup_callback=lease.close if lease is not None else None,
            progress_callback=progress_callback,
            process_input=process_input,
            drain_output=drain_output,
            popen=subprocess.Popen,
        )

    def import_file(self, qualified_table: str, input_path: str) -> BcpResult:
        process_input = BcpProcessInput.resolve(input_path, self.options.input_file_authority)
        command = [self.options.bcp_path, qualified_table, "in", process_input.path, self._file_format_flag()]
        command += self._connection_options(
            include_database=not bcp_qualified_table_includes_database(qualified_table)
        ) + self._format_options(include_table_lock=True, include_keep_nulls=True)
        return self._execute(
            command,
            process_input=process_input,
        )

    def import_format_file(self, qualified_table: str, input_path: str, format_path: str) -> BcpResult:
        """Bulk-import an explicit length-prefixed host format.

        The format file owns field framing, so delimiter flags are deliberately
        absent.  Connection, TLS, batching, reject-file and table-lock options
        remain identical to ordinary BCP imports.
        """

        command = [self.options.bcp_path, qualified_table, "in", input_path, "-f", format_path]
        command += self._connection_options(
            include_database=not bcp_qualified_table_includes_database(qualified_table)
        ) + self._format_options(
            include_table_lock=True,
            include_keep_nulls=True,
            include_delimiters=False,
        )
        return self._execute(command)

    def build_queryout_command(self, query: str, output_path: str, *, options: BcpOptions | None = None) -> list[str]:
        original_options = self.options
        if options is not None:
            object.__setattr__(self, "options", options)
        try:
            command = [self.options.bcp_path, query, "queryout", output_path, self._file_format_flag()]
            command += self._connection_options() + self._format_options(
                include_table_lock=False,
                include_keep_nulls=False,
            )
            return command
        finally:
            if options is not None:
                object.__setattr__(self, "options", original_options)

    def _connection_options(self, *, include_database: bool = True) -> list[str]:
        command = (
            ["-D", "-S", "dpone_bcp_connection"]
            if self.options.connection_dsn is not None
            else ["-S", self.credentials.server]
        )
        if include_database:
            command += ["-d", self.credentials.database]
        if self.credentials.trusted_connection:
            command += ["-T"]
        else:
            command += ["-U", self.credentials.user or ""]
            if self.options.password_transport == "argument":
                command += ["-P", self.credentials.password or ""]
        return command

    def _format_options(
        self,
        *,
        include_table_lock: bool,
        include_keep_nulls: bool,
        include_delimiters: bool = True,
    ) -> list[str]:
        options = []
        if self.options.file_format == "character":
            options += ["-C", self.options.code_page]
            if include_delimiters:
                options += [
                    "-t",
                    self._bcp_terminator(self.options.field_terminator),
                    "-r",
                    self._bcp_terminator(self.options.row_terminator),
                ]
        options += [
            "-b",
            str(self.options.batch_size),
            "-a",
            str(effective_mssql_bcp_packet_size(self.options.packet_size)),
        ]
        if self.options.timeout_seconds:
            options += ["-l", str(self.options.timeout_seconds)]
        if self.options.error_file:
            options += ["-e", self.options.error_file]
        if self.options.trust_server_certificate:
            options.append("-u")
        if include_table_lock and self.options.table_lock:
            options += ["-h", "TABLOCK"]
        if include_keep_nulls and self.options.keep_nulls:
            options.append("-k")
        return options

    def _execute(
        self,
        command: list[str],
        *,
        process_input: BcpProcessInput | None = None,
    ) -> BcpResult:
        if self._run is None:
            return self._start_process(
                command,
                process_input=process_input,
                drain_output=False,
            ).wait()

        stdin = self._stdin_password()
        lease = self._issue_connection_dsn()
        process_options = process_input.subprocess_options() if process_input is not None else {}
        redact = process_input.redact if process_input is not None else str
        assert self._run is not None
        return run_injected_bcp_command(
            command,
            redacted_command=tuple(redact(value) for value in self.redact_command(command)),
            stdin_text=stdin,
            timeout_seconds=int(self.options.timeout_seconds),
            environment=lease.environment if lease is not None else None,
            cleanup_callback=lease.close if lease is not None else None,
            run=self._run,
            process_options=process_options,
            redact_output=redact,
        )

    def _issue_connection_dsn(self) -> BcpDsnLease | None:
        options = self.options.connection_dsn
        if options is None:
            return None
        return issue_bcp_dsn(
            host=self.credentials.host,
            port=self.credentials.port,
            options=options,
        )

    def _stdin_password(self) -> str | None:
        if (
            not self.credentials.trusted_connection
            and self.options.password_transport == "stdin"
            and self.credentials.password is not None
        ):
            return f"{self.credentials.password}\n"
        return None

    @staticmethod
    def redact_command(command: Sequence[str]) -> list[str]:
        redacted: list[str] = []
        hide_next = False
        for token in command:
            if hide_next:
                redacted.append("***")
                hide_next = False
                continue
            redacted.append(token)
            if token == "-P":
                hide_next = True
        return redacted

    parse_rows_copied = staticmethod(parse_bcp_rows_copied)

    @staticmethod
    def _bcp_terminator(value: str) -> str:
        if value == "\t":
            return "\\t"
        if value == "\n":
            return "\\n"
        if value == "\r\n":
            return "\\r\\n"
        return value

    def _file_format_flag(self) -> str:
        raw_format = self.options.file_format.strip()
        normalized = raw_format.lower().replace("_", "-")
        if normalized in {"character", "char", "c"}:
            return "-c"
        if normalized in {"wide-native", "widenative", "nchar-native"} or raw_format == "N":
            if sys.platform != "win32":
                raise ValueError("bcp_unicode_native_requires_windows")
            return "-N"
        if normalized in {"native", "n"}:
            return "-n"
        raise ValueError(f"Unsupported bcp file_format: {self.options.file_format}")


def bcp_qualified_table_includes_database(qualified_table: str) -> bool:
    """Return true when ``qualified_table`` is a three-part ``database.schema.table`` target.

    Microsoft ``bcp`` rejects ``-d <database>`` together with a three-part table
    argument; callers must omit the connection database flag in that case.
    """

    parts = re.findall(r"\[[^\]]+\]", str(qualified_table or ""))
    return len(parts) >= 3


def is_mssql_character_bulk_unsafe_type(dtype: str) -> bool:
    """Return true for SQL Server types that should not use character bcp by default."""

    normalized = dtype.lower().strip()
    if normalized in {"timestamp", "rowversion"}:
        return True
    if normalized.startswith("timestamp ") or normalized.startswith("timestamp("):
        return False
    unsafe_tokens = (
        "binary",
        "varbinary",
        "image",
        "rowversion",
        "sql_variant",
        "hierarchyid",
        "geometry",
        "geography",
    )
    return any(token in normalized for token in unsafe_tokens)


__all__ = [
    "DEFAULT_MSSQL_BCP_TIMEOUT_SECONDS",
    "BcpCredentials",
    "BcpDsnOptions",
    "BcpOptions",
    "BcpRunner",
    "BcpTimeoutError",
    "DelimitedBulkFile",
    "MssqlSpoolCapacityError",
    "UnsafeBulkValueError",
    "bcp_qualified_table_includes_database",
    "effective_mssql_bcp_packet_size",
    "is_mssql_character_bulk_unsafe_type",
]
