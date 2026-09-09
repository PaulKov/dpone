"""Deployment-owned MSSQL Asset URI authority resolution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type

_MSSQL_ENGINE = "mssql"
_DEFAULT_MSSQL_PORT = 1433
MSSQL_ASSET_URI_INVALID = "DPONE_MSSQL_ASSET_URI_INVALID"


@dataclass(frozen=True, slots=True)
class MssqlAssetAuthority:
    """Stable deployment-owned MSSQL Asset URI authority."""

    host: str
    port: int = _DEFAULT_MSSQL_PORT
    instance: str | None = None

    def netloc(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True, slots=True)
class AssetUriIssue:
    """Structured build/check issue for Asset URI resolution."""

    code: str
    message: str
    path: str | None = None


@dataclass(frozen=True, slots=True)
class AssetUriResolution:
    """Structured URI resolution result (never silently drops invalid MSSQL)."""

    uri: str | None
    issues: tuple[AssetUriIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return self.uri is not None and not self.issues


def is_templated_value(value: str | None) -> bool:
    """Return True when a coordinate still contains Jinja/Airflow templating."""

    return value is not None and "{{" in value


def issue(message: str, *, path: str | None = None) -> AssetUriIssue:
    return AssetUriIssue(code=MSSQL_ASSET_URI_INVALID, message=message, path=path)


def normalize_hostname(host: str) -> str:
    """Normalize hostname for registry and explicit URI comparison."""

    text = host.strip().rstrip(".").lower()
    if not text:
        return text
    try:
        return text.encode("idna").decode("ascii")
    except UnicodeError:
        return text


def normalize_instance(instance: str | None) -> str | None:
    """Normalize named instance for authority identity (case-insensitive)."""

    if instance is None:
        return None
    text = instance.strip()
    return text.lower() if text else None


def validate_authority_host(host: str) -> str | None:
    text = host.strip()
    if not text or is_templated_value(text):
        return "mssql asset_authority.host is required and must not be templated"
    if any(ch in text for ch in ("/", "?", "#", "@", ":", " ")):
        return "mssql asset_authority.host must be a bare hostname (no port/user/path)"
    return None


def parse_mssql_asset_authority(
    raw: object,
    *,
    path: str | None = None,
) -> tuple[MssqlAssetAuthority | None, tuple[AssetUriIssue, ...]]:
    """Parse ``asset_authority`` mapping or reject invalid shapes."""

    if not isinstance(raw, Mapping):
        return None, (issue("mssql asset_authority must be a mapping with host/port", path=path),)
    host = str(raw.get("host") or "").strip()
    host_error = validate_authority_host(host)
    if host_error:
        return None, (issue(host_error, path=path),)
    port_raw = raw.get("port", _DEFAULT_MSSQL_PORT)
    try:
        port = int(port_raw) if port_raw is not None else _DEFAULT_MSSQL_PORT
    except (TypeError, ValueError):
        return None, (issue("mssql asset_authority.port must be an integer 1..65535", path=path),)
    if not 1 <= port <= 65535:
        return None, (issue("mssql asset_authority.port must be an integer 1..65535", path=path),)
    instance_raw = raw.get("instance")
    instance = str(instance_raw).strip() if instance_raw not in (None, "") else None
    if instance is not None and (is_templated_value(instance) or "/" in instance):
        return None, (issue("mssql asset_authority.instance is invalid", path=path),)
    return (
        MssqlAssetAuthority(
            host=normalize_hostname(host),
            port=port,
            instance=normalize_instance(instance),
        ),
        (),
    )


def authority_from_connection_metadata(
    connection: Mapping[str, Any] | None,
    *,
    path: str | None = None,
) -> tuple[MssqlAssetAuthority | None, tuple[AssetUriIssue, ...]]:
    """Resolve authority from registry ``connection`` metadata."""

    if not isinstance(connection, Mapping):
        return None, (
            issue(
                "mssql Asset URI requires registry connection.asset_authority "
                "(or connection.host/port); connection_ref is not a valid host",
                path=path,
            ),
        )
    nested = connection.get("asset_authority")
    if nested is not None:
        return parse_mssql_asset_authority(nested, path=path)
    host = str(connection.get("host") or "").strip()
    if not host:
        return None, (
            issue(
                "mssql Asset URI requires registry connection.asset_authority.host "
                "(or connection.host); connection_ref must not be used as URI host",
                path=path,
            ),
        )
    payload = {
        "host": host,
        "port": connection.get("port", _DEFAULT_MSSQL_PORT),
        "instance": connection.get("instance"),
    }
    return parse_mssql_asset_authority(payload, path=path)


def load_mssql_asset_authority_index(
    registry: Mapping[str, Any] | None,
) -> dict[str, MssqlAssetAuthority]:
    """Build ``connection_ref -> authority`` from a connection-registry document."""

    if not isinstance(registry, Mapping):
        return {}
    connections = registry.get("connections")
    if not isinstance(connections, Mapping):
        return {}
    index: dict[str, MssqlAssetAuthority] = {}
    for ref, entry in connections.items():
        if not isinstance(entry, Mapping):
            continue
        if canonical_endpoint_type(str(entry.get("type") or "")) != _MSSQL_ENGINE:
            continue
        authority, issues = authority_from_connection_metadata(
            entry.get("connection") if isinstance(entry.get("connection"), Mapping) else None,
            path=str(ref),
        )
        if authority is not None and not issues:
            index[str(ref)] = authority
    return index


def load_mssql_default_database_index(
    registry: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Build ``connection_ref -> default database`` from registry connection metadata."""

    if not isinstance(registry, Mapping):
        return {}
    connections = registry.get("connections")
    if not isinstance(connections, Mapping):
        return {}
    index: dict[str, str] = {}
    for ref, entry in connections.items():
        if not isinstance(entry, Mapping):
            continue
        if canonical_endpoint_type(str(entry.get("type") or "")) != _MSSQL_ENGINE:
            continue
        connection = entry.get("connection")
        if not isinstance(connection, Mapping):
            continue
        database = str(connection.get("database") or "").strip()
        if database and not is_templated_value(database):
            index[str(ref)] = database
    return index


def connection_registry_path_for_env(repo_root: Path, env: str) -> Path | None:
    """Resolve the connection-registry path for one environment (no cross-env scan)."""

    from dpone.gitops.airflow_mssql_registry_path import (
        connection_registry_path_for_env as _resolve,
    )

    return _resolve(repo_root, env)


def discover_connection_registry_path(repo_root: Path, *, env: str = "dev") -> Path | None:
    """Resolve registry path for ``env`` only (no best-effort fallback to another env)."""

    return connection_registry_path_for_env(repo_root, env)


def load_connection_registry_document(
    path: Path,
    *,
    raw: bytes | None = None,
) -> Mapping[str, Any] | None:
    """Load a connection-registry YAML document, or ``None`` when unavailable.

    Prefer passing ``raw`` from a single-read snapshot so digest and parse share
    the same bytes.
    """

    try:
        import yaml
    except ImportError:  # pragma: no cover
        return None
    try:
        text = raw.decode("utf-8") if raw is not None else path.read_text(encoding="utf-8")
        payload = yaml.safe_load(text)
    except (OSError, UnicodeError, yaml.YAMLError):
        return None
    return payload if isinstance(payload, Mapping) else None


def connection_ref_from_block(block: Mapping[str, Any]) -> str | None:
    """Logical connection alias from a source/sink block (not Asset URI host)."""

    for key in ("connection_ref", "connection_id"):
        value = str(block.get(key) or "").strip()
        if value and not is_templated_value(value):
            return value
    return None


def _authorities_match(left: MssqlAssetAuthority, right: MssqlAssetAuthority) -> bool:
    return (
        normalize_hostname(left.host) == normalize_hostname(right.host)
        and int(left.port) == int(right.port)
        and normalize_instance(left.instance) == normalize_instance(right.instance)
    )


def resolve_mssql_asset_authority(
    block: Mapping[str, Any],
    *,
    authority_index: Mapping[str, MssqlAssetAuthority] | None = None,
    path: str | None = None,
) -> tuple[MssqlAssetAuthority | None, tuple[AssetUriIssue, ...]]:
    """Resolve MSSQL authority for a lineage block.

    For managed connections (``connection_ref`` present), ``lineage.asset_authority``
    is only an assertion: it must exactly match the registry authority after
    hostname normalization. Authors cannot override deployment-owned identity.
    """

    declared: MssqlAssetAuthority | None = None
    lineage = block.get("lineage")
    if isinstance(lineage, Mapping) and lineage.get("asset_authority") is not None:
        declared, declared_issues = parse_mssql_asset_authority(lineage.get("asset_authority"), path=path)
        if declared_issues:
            return None, declared_issues

    ref = connection_ref_from_block(block)
    if ref and authority_index and ref in authority_index:
        registry_authority = authority_index[ref]
        if declared is not None and not _authorities_match(declared, registry_authority):
            return None, (
                issue(
                    "lineage.asset_authority must match registry connection.asset_authority "
                    f"for connection_ref {ref!r} (deployment-owned; author override forbidden)",
                    path=path or ref,
                ),
            )
        return registry_authority, ()
    if ref:
        return None, (
            issue(
                f"mssql connection_ref {ref!r} has no deployment-owned asset_authority "
                "in the connection registry; add connection.asset_authority.host/port "
                "(never use connection_ref as mssql:// host)",
                path=path or ref,
            ),
        )
    if declared is not None:
        return None, (
            issue(
                "mssql Asset URI requires connection_ref with registry asset_authority; "
                "lineage.asset_authority alone is not deployment-owned",
                path=path,
            ),
        )
    return None, (
        issue(
            "mssql Asset URI requires connection_ref with registry asset_authority",
            path=path,
        ),
    )


__all__ = [
    "MSSQL_ASSET_URI_INVALID",
    "AssetUriIssue",
    "AssetUriResolution",
    "MssqlAssetAuthority",
    "authority_from_connection_metadata",
    "connection_ref_from_block",
    "connection_registry_path_for_env",
    "discover_connection_registry_path",
    "is_templated_value",
    "issue",
    "load_connection_registry_document",
    "load_mssql_asset_authority_index",
    "load_mssql_default_database_index",
    "normalize_hostname",
    "normalize_instance",
    "parse_mssql_asset_authority",
    "resolve_mssql_asset_authority",
    "validate_authority_host",
]
