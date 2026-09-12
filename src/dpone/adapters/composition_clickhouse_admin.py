"""Concrete bounded HTTP I/O for the closed ClickHouse 24.8 principal surface.

Only the trusted supervisor constructs this client with sealed administrator
credentials. Command SQL travels in the POST body, never the URI. Neither
transport completion nor this syntax allowlist supplies gate/ownership authority.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from ipaddress import ip_address
from uuid import uuid4

from dpone.adapters.composition_clickhouse_http import (
    BoundedClickHouseHttp,
    ClickHouseHttpError,
    ClickHouseTransportCredentials,
    clickhouse_http_path,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import strict_json_object

_USER = r"dpone_ch_[0-9a-f]{64}"
_NAME = r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*"
_CREATE = re.compile(
    rf"CREATE USER `({_USER})` IDENTIFIED WITH sha256_hash BY '([0-9a-f]{{64}})' HOST NONE DEFAULT ROLE NONE GRANTEES NONE"
)
_GRANT = re.compile(
    rf"GRANT (CREATE TABLE|INSERT|SELECT|DROP TABLE) ON `([A-Za-z_][A-Za-z0-9_]*)`.`({_NAME})` TO `({_USER})`"
)
_ENABLE = re.compile(rf"ALTER USER `({_USER})` HOST IP '([0-9a-f:.]+)'")
_LOCAL = re.compile(rf"ALTER USER `{_USER}` HOST LOCAL")
_DISABLE = re.compile(rf"ALTER USER `{_USER}` HOST NONE")
_REVOKE = re.compile(rf"REVOKE ALL ON \*\.\* FROM `{_USER}`")
_USER_COLUMNS = (
    "toString(id)",
    "toString(auth_type)",
    "host_ip",
    "host_names",
    "host_names_regexp",
    "host_names_like",
    "default_roles_all",
    "default_roles_list",
    "default_roles_except",
    "grantees_any",
    "grantees_list",
    "grantees_except",
)
_USER_TYPES = (
    "String",
    "String",
    "Array(String)",
    "Array(String)",
    "Array(String)",
    "Array(String)",
    "UInt8",
    "Array(String)",
    "Array(String)",
    "UInt8",
    "Array(String)",
    "Array(String)",
)
# Exact queries consumed by ClickHousePrincipalAdmin; no worker SQL or settings.
_QUERIES: dict[str, tuple[str | None, tuple[str, ...], tuple[str, ...]]] = {
    "SELECT version(), toString(serverUUID()), toString(uuid), engine FROM system.databases WHERE name={database:String} LIMIT 2": (
        "database",
        ("version()", "toString(serverUUID())", "toString(uuid)", "engine"),
        ("String",) * 4,
    ),
    "SELECT toString(id) FROM system.users WHERE name={user:String} LIMIT 2": ("user", ("toString(id)",), ("String",)),
    "SELECT " + ", ".join(_USER_COLUMNS) + " FROM system.users WHERE name={user:String} LIMIT 2": (
        "user",
        _USER_COLUMNS,
        _USER_TYPES,
    ),
    "SELECT granted_role_name FROM system.role_grants WHERE user_name={user:String} LIMIT 2": (
        "user",
        ("granted_role_name",),
        ("String",),
    ),
    "SELECT toString(access_type), database, table, column, is_partial_revoke, grant_option FROM system.grants WHERE user_name={user:String} LIMIT 65": (
        "user",
        ("toString(access_type)", "database", "table", "column", "is_partial_revoke", "grant_option"),
        ("String", "Nullable(String)", "Nullable(String)", "Nullable(String)", "UInt8", "UInt8"),
    ),
    "SELECT count() FROM system.processes WHERE user={user:String} OR initial_user={user:String}": (
        "user",
        ("count()",),
        ("UInt64",),
    ),
    **{
        f"SELECT count() FROM system.{table}": (None, ("count()",), ("UInt64",))
        for table in ("transactions", "asynchronous_inserts", "mutations WHERE NOT is_done", "distribution_queue")
    },
}


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("clickhouse_admin_closed_surface")


def _command(statement: str, parameters: Mapping[str, object], redactions: tuple[str, ...]) -> None:
    _require(type(statement) is str and len(statement) <= 2048 and isinstance(parameters, Mapping) and not parameters)
    _require(
        type(redactions) is tuple
        and len(redactions) <= 4
        and all(type(value) is str and 0 < len(value) <= 4096 for value in redactions)
    )
    if match := _CREATE.fullmatch(statement):
        _require(match[2] in redactions)
    elif match := _GRANT.fullmatch(statement):
        _require(len(match[2]) <= 128 and len(match[3]) <= 128)
    elif match := _ENABLE.fullmatch(statement):
        try:
            address = ip_address(match[2])
        except ValueError:
            raise CompositionAdmissionError("clickhouse_admin_closed_surface") from None
        _require(str(address) == match[2] and not address.is_unspecified)
    else:
        _require(
            _DISABLE.fullmatch(statement) is not None
            or _REVOKE.fullmatch(statement) is not None
            or _LOCAL.fullmatch(statement) is not None
        )


def _cell(value: object, kind: str) -> object:
    if kind == "Nullable(String)":
        return None if value is None else _cell(value, "String")
    if kind == "String":
        _require(type(value) is str and len(value) <= 4096)
        return value
    if kind == "Array(String)":
        _require(type(value) is list and len(value) <= 64)
        assert isinstance(value, list)
        return tuple(_cell(item, "String") for item in value)
    if kind == "UInt64" and type(value) is str:
        _require(re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is not None)
        value = int(value)
    _require(type(value) is int and 0 <= value <= (255 if kind == "UInt8" else 2**64 - 1))
    return value


def _rows(
    body: bytes, columns: tuple[str, ...], types: tuple[str, ...], maximum: int
) -> tuple[tuple[object, ...], ...]:
    value = strict_json_object(body)
    _require(
        {"meta", "data", "rows"} <= set(value)
        and set(value) <= {"meta", "data", "rows", "statistics", "rows_before_limit_at_least"}
    )
    _require(value["meta"] == [{"name": name, "type": kind} for name, kind in zip(columns, types, strict=True)])
    data = value["data"]
    _require(type(data) is list and len(data) <= maximum and type(value["rows"]) is int and value["rows"] == len(data))
    if "rows_before_limit_at_least" in value:
        _require(
            type(value["rows_before_limit_at_least"]) is int
            and len(data) <= value["rows_before_limit_at_least"] <= maximum
        )
    if "statistics" in value:
        statistics = value["statistics"]
        _require(type(statistics) is dict and set(statistics) == {"elapsed", "rows_read", "bytes_read"})
        _require(
            type(statistics["elapsed"]) in {int, float}
            and statistics["elapsed"] >= 0
            and all(type(statistics[key]) is int and statistics[key] >= 0 for key in ("rows_read", "bytes_read"))
        )
    _require(all(type(row) is list and len(row) == len(columns) for row in data))
    return tuple(tuple(_cell(cell, kind) for cell, kind in zip(row, types, strict=True)) for row in data)


class ClickHousePrincipalHttpClient:
    """Actual one-shot I/O; construction provides no gate or dispatch authority."""

    def __init__(
        self,
        *,
        endpoint: str,
        credentials: ClickHouseTransportCredentials,
        timeout_seconds: float,
        max_response_bytes: int = 1024 * 1024,
        ca_file: str | None = None,
    ) -> None:
        self._http = BoundedClickHouseHttp(
            endpoint=endpoint,
            credentials=credentials,
            timeout_seconds=timeout_seconds,
            max_payload_bytes=16384,
            max_response_bytes=max_response_bytes,
            ca_file=ca_file,
        )

    def command(self, statement: str, *, parameters: Mapping[str, object], redactions: tuple[str, ...]) -> None:
        """Complete the fixed principal command once; uncertain ACK never retries."""
        _command(statement, parameters, redactions)
        query_id = "dpone-admin-" + uuid4().hex
        try:
            observed = self._http.request(
                path=clickhouse_http_path(query_id=query_id), payload=statement.encode(), query_id=query_id
            )
            if observed.body:
                raise ClickHouseHttpError(observed.request_body_bytes)
        except Exception:
            raise CompositionAdmissionError("clickhouse_admin_command_unknown") from None

    def query(
        self, statement: str, *, parameters: Mapping[str, object], max_rows: int
    ) -> tuple[tuple[object, ...], ...]:
        """Decode complete strict JSONCompact; max_rows+1 is a rejection sentinel."""
        _require(type(statement) is str and statement in _QUERIES and type(max_rows) is int and 1 <= max_rows <= 64)
        key, columns, types = _QUERIES[statement]
        _require(isinstance(parameters, Mapping) and set(parameters) == (set() if key is None else {key}))
        values: dict[str, str] = {}
        if key is not None:
            parameter = parameters[key]
            _require(
                type(parameter) is str
                and 1 <= len(parameter) <= 128
                and re.fullmatch(_USER if key == "user" else r"[A-Za-z_][A-Za-z0-9_]*", parameter) is not None
            )
            assert isinstance(parameter, str)
            values[key] = parameter
        query_id = "dpone-admin-" + uuid4().hex
        try:
            observed = self._http.request(
                path=clickhouse_http_path(query_id=query_id, parameters=values, result_rows=max_rows + 1),
                payload=f"SELECT * FROM ({statement}) LIMIT {max_rows + 1} FORMAT JSONCompact".encode(),
                query_id=query_id,
            )
            return _rows(observed.body, columns, types, max_rows)
        except Exception:
            raise CompositionAdmissionError("clickhouse_admin_query_unknown") from None
