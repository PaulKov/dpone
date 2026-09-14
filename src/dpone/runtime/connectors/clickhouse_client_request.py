"""Pure ClickHouse client credentials, wire options and request construction.

No process is created here. Legacy transports retain their own dispatch points;
controlled adapters consume these builders without constructing a legacy runner.
"""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from dataclasses import dataclass, field
from types import FunctionType
from typing import Any


@dataclass(frozen=True)
class ClickHouseClientCredentials:
    """Connection settings used by ``clickhouse-client``."""

    host: str
    port: int
    database: str
    user: str
    password: str = ""
    secure: bool = False


@dataclass(frozen=True)
class ClickHouseClientOptions:
    """Runtime options for native ClickHouse file ingest."""

    client_command: str = "clickhouse-client"
    input_format: str = "TabSeparated"
    timeout_seconds: int | None = None
    max_insert_block_size: int | None = None
    settings: dict[str, Any] = field(default_factory=dict)
    query_id: str | None = None
    insert_deduplication_token: str | None = None

    @classmethod
    def from_bulk_wire_contract(
        cls,
        contract: Any,
        *,
        base: ClickHouseClientOptions | None = None,
    ) -> ClickHouseClientOptions:
        """Return options with input format/settings required by a typed wire contract."""

        base_options = base or cls()
        return cls(
            client_command=base_options.client_command,
            input_format=str(getattr(contract, "input_format", base_options.input_format)),
            timeout_seconds=base_options.timeout_seconds,
            max_insert_block_size=base_options.max_insert_block_size,
            settings={**base_options.settings, **dict(contract.delimiter_profile.clickhouse_settings)},
            query_id=base_options.query_id,
            insert_deduplication_token=base_options.insert_deduplication_token,
        )


def build_base_command(credentials: ClickHouseClientCredentials, options: ClickHouseClientOptions) -> list[str]:
    command = shlex.split(options.client_command)
    command += [
        "--host",
        credentials.host,
        "--port",
        str(credentials.port),
        "--database",
        credentials.database,
        "--user",
        credentials.user,
        "--password",
        credentials.password,
    ]
    if credentials.secure:
        command.append("--secure")
    if options.max_insert_block_size:
        command += ["--max_insert_block_size", str(options.max_insert_block_size)]
    for key, value in options.settings.items():
        command += [f"--{key}", str(value)]
    if options.query_id:
        command += ["--query_id", options.query_id]
    if options.insert_deduplication_token:
        command += ["--insert_deduplication_token", options.insert_deduplication_token]
    return command


def build_insert_query(table: str, columns: Sequence[str], input_format: str) -> str:
    column_sql = ", ".join(f"`{column}`" for column in columns)
    return f"INSERT INTO {table} ({column_sql}) FORMAT {input_format}"


def build_query_command(
    credentials: ClickHouseClientCredentials, options: ClickHouseClientOptions, sql: str
) -> list[str]:
    return build_base_command(credentials, options) + ["--query", sql]


def redact_command(command: Sequence[str]) -> list[str]:
    redacted: list[str] = []
    hide_next = False
    for token in command:
        if hide_next:
            redacted.append("***")
            hide_next = False
            continue
        redacted.append(token)
        if token == "--password":
            hide_next = True
    return redacted


# Historical globals and generated dataclass methods remain pickle/introspection compatible.
for _model in (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
):
    _model.__module__ = "dpone.runtime.connectors.clickhouse_bulk"
    for _member in vars(_model).values():
        if isinstance(_member, classmethod):
            _member = _member.__func__
        if isinstance(_member, FunctionType) and _member.__module__ == __name__:
            _member.__module__ = "dpone.runtime.connectors.clickhouse_bulk"
