"""Protected physical ClickHouse connection and topology authority."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CLUSTER_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SemanticRefreshClickHouseHttpClientPort(Protocol):
    """Minimal protected HTTP capability required by publication."""

    @property
    def endpoint_authority_id(self) -> str:
        """Return the normalized non-secret endpoint used by every request."""

    def execute(self, statement: str) -> Any:
        """Execute one HTTP-backed ClickHouse query or command."""

    def execute_operation(self, statement: str, *, query_id: str) -> Any:
        """Execute one operation-bound query with an observable query ID."""

    def insert_parquet(self, statement: str, content: bytes) -> Any:
        """Execute one bounded Parquet insert with exact supplied bytes."""

    def insert_parquet_operation(self, statement: str, content: bytes, *, query_id: str) -> Any:
        """Execute one operation-bound Parquet insert with an observable query ID."""


@dataclass(frozen=True, slots=True)
class ClickHouseClusterConnectionAuthority:
    """Exact endpoint and single-node topology bound to one protected cluster ID."""

    clickhouse_cluster_authority_id: str
    endpoint_authority_id: str
    cluster_name: str
    host_names: tuple[str, ...]
    topology_sha256: str

    def __post_init__(self) -> None:
        if not self.clickhouse_cluster_authority_id.strip():
            raise ValueError("ClickHouse connection cluster authority ID is empty")
        if normalize_clickhouse_endpoint_authority(self.endpoint_authority_id) != self.endpoint_authority_id:
            raise ValueError("ClickHouse connection endpoint authority is not canonical")
        if _CLUSTER_NAME_RE.fullmatch(self.cluster_name) is None:
            raise ValueError("ClickHouse connection cluster name is invalid")
        if (
            len(self.host_names) != 1
            or any(not isinstance(host, str) or not host.strip() for host in self.host_names)
            or tuple(sorted(set(self.host_names))) != self.host_names
        ):
            raise ValueError("ClickHouse connection host set must contain one canonical host")
        if _DIGEST_RE.fullmatch(self.topology_sha256) is None:
            raise ValueError("ClickHouse connection topology digest is invalid")


def normalize_clickhouse_endpoint_authority(value: str) -> str:
    """Return the canonical HTTP(S) endpoint authority without credentials or paths."""

    if not isinstance(value, str):
        raise TypeError("ClickHouse endpoint authority must be text")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError("ClickHouse endpoint authority is invalid")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("ClickHouse endpoint authority port is invalid") from exc
    host = parsed.hostname.lower()
    rendered_host = f"[{host}]" if ":" in host else host
    default_port = (parsed.scheme == "https" and port == 443) or (parsed.scheme == "http" and port == 80)
    port_suffix = "" if port is None or default_port else f":{port}"
    return f"{parsed.scheme}://{rendered_host}{port_suffix}/"


def clickhouse_cluster_topology_sha256(rows: tuple[tuple[object, ...], ...]) -> str:
    """Digest ordered `(host_name, port, shard_num, replica_num)` observations."""

    try:
        raw = json.dumps(rows, allow_nan=False, ensure_ascii=True, separators=(",", ":")).encode()
    except (TypeError, ValueError) as exc:
        raise ValueError("ClickHouse cluster topology rows are not canonical JSON") from exc
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def assert_clickhouse_cluster_topology(
    authority: ClickHouseClusterConnectionAuthority,
    rows: tuple[tuple[object, ...], ...],
) -> None:
    """Fail unless current topology is the exact protected one-node/one-replica set."""

    if (
        len(rows) != 1
        or len(rows[0]) != 4
        or not isinstance(rows[0][0], str)
        or rows[0][0] not in authority.host_names
        or isinstance(rows[0][1], bool)
        or not isinstance(rows[0][1], int)
        or rows[0][1] <= 0
        or rows[0][2:] != (1, 1)
        or clickhouse_cluster_topology_sha256(rows) != authority.topology_sha256
    ):
        raise ValueError("ClickHouse cluster topology authority differs")


__all__ = [
    "ClickHouseClusterConnectionAuthority",
    "SemanticRefreshClickHouseHttpClientPort",
    "assert_clickhouse_cluster_topology",
    "clickhouse_cluster_topology_sha256",
    "normalize_clickhouse_endpoint_authority",
]
