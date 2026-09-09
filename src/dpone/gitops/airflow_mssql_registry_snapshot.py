"""Immutable env-bound MSSQL connection-registry snapshots for pack builds."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from dpone.gitops.airflow_mssql_asset_authority import (
    AssetUriIssue,
    MssqlAssetAuthority,
    load_mssql_asset_authority_index,
    load_mssql_default_database_index,
    normalize_hostname,
)
from dpone.gitops.airflow_mssql_registry_path import (
    MSSQL_REGISTRY_ENVIRONMENT_MISMATCH,
    MSSQL_REGISTRY_PATH_INVALID,
    connection_registry_path_for_env,
    read_mssql_registry_bytes,
    resolve_connection_registry_path,
)

MSSQL_REGISTRY_SNAPSHOT_DRIFT = "DPONE_MSSQL_REGISTRY_SNAPSHOT_DRIFT"
MSSQL_REGISTRY_PARSE_INVALID = "DPONE_MSSQL_REGISTRY_PARSE_INVALID"


@dataclass(frozen=True, slots=True)
class ResolvedMssqlAssetRegistry:
    """Immutable MSSQL authority + database indexes from one env registry file."""

    env: str
    path: str | None
    authorities: Mapping[str, MssqlAssetAuthority]
    databases: Mapping[str, str]
    content_sha256: str | None = None
    issues: tuple[AssetUriIssue, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "authorities", MappingProxyType(dict(self.authorities)))
        object.__setattr__(self, "databases", MappingProxyType(dict(self.databases)))

    @property
    def ok(self) -> bool:
        return not self.issues

    @property
    def approved_hosts(self) -> frozenset[str]:
        return frozenset(normalize_hostname(auth.host) for auth in self.authorities.values())

    @property
    def approved_authorities(self) -> frozenset:
        from dpone.gitops.airflow_mssql_explicit_authority import MssqlAssetAuthorityKey

        return frozenset(MssqlAssetAuthorityKey.from_authority(auth) for auth in self.authorities.values())

    @property
    def connection_refs(self) -> frozenset[str]:
        return frozenset(self.authorities)

    def identity(self) -> dict[str, str | None]:
        """Stable registry identity for pack/reconcile/report binding."""

        digest = None if self.content_sha256 is None else f"sha256:{self.content_sha256}"
        return {
            "environment": self.env,
            "registry_path": self.path,
            "registry_sha256": digest,
            "authority_digest": self.authority_digest(),
        }

    def authority_digest(self) -> str | None:
        """Digest of projected connection_ref → host/port/instance/database."""

        if not self.authorities and not self.databases:
            return None if self.content_sha256 is None else f"sha256:{self.content_sha256}"
        payload = {
            ref: {
                "host": auth.host,
                "port": auth.port,
                "instance": auth.instance,
                "database": self.databases.get(ref),
            }
            for ref, auth in sorted(self.authorities.items())
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return "sha256:" + hashlib.sha256(raw).hexdigest()

    def verify_unchanged(self, repo_root: Path | None = None) -> AssetUriIssue | None:
        """Return a drift issue when the on-disk registry no longer matches this snapshot."""

        if self.path is None or self.content_sha256 is None:
            return None
        path = Path(self.path)
        if not path.is_absolute() and repo_root is not None:
            path = repo_root / path
        try:
            digest = hashlib.sha256(read_mssql_registry_bytes(path)).hexdigest()
        except OSError:
            return AssetUriIssue(
                code=MSSQL_REGISTRY_SNAPSHOT_DRIFT,
                message="MSSQL connection registry disappeared during the artifact-set build",
                path=self.path,
            )
        if digest != self.content_sha256:
            return AssetUriIssue(
                code=MSSQL_REGISTRY_SNAPSHOT_DRIFT,
                message="MSSQL connection registry changed during the artifact-set build",
                path=self.path,
            )
        return None


def resolve_mssql_asset_registry(repo_root: Path, *, env: str) -> ResolvedMssqlAssetRegistry:
    """Load authority and database indexes from one single-read env registry snapshot."""

    resolution = resolve_connection_registry_path(repo_root, env)
    if resolution.issues:
        return ResolvedMssqlAssetRegistry(
            env=env,
            path=resolution.relative_path,
            authorities={},
            databases={},
            issues=resolution.issues,
        )
    if resolution.path is None or resolution.relative_path is None:
        return ResolvedMssqlAssetRegistry(env=env, path=None, authorities={}, databases={})
    try:
        raw = read_mssql_registry_bytes(resolution.path)
    except OSError as exc:
        return ResolvedMssqlAssetRegistry(
            env=env,
            path=resolution.relative_path,
            authorities={},
            databases={},
            issues=(
                AssetUriIssue(
                    code=MSSQL_REGISTRY_PATH_INVALID,
                    message=f"MSSQL connection registry could not be read: {exc.strerror or exc}",
                    path=resolution.relative_path,
                ),
            ),
        )
    digest = hashlib.sha256(raw).hexdigest()
    document, parse_issue = _parse_registry_document(raw, path=resolution.relative_path)
    if parse_issue is not None:
        return ResolvedMssqlAssetRegistry(
            env=env,
            path=resolution.relative_path,
            authorities={},
            databases={},
            content_sha256=digest,
            issues=(parse_issue,),
        )
    env_issue = _environment_invariant(document, requested_env=env, path=resolution.relative_path)
    if env_issue is not None:
        return ResolvedMssqlAssetRegistry(
            env=env,
            path=resolution.relative_path,
            authorities={},
            databases={},
            content_sha256=digest,
            issues=(env_issue,),
        )
    return ResolvedMssqlAssetRegistry(
        env=env,
        path=resolution.relative_path,
        authorities=load_mssql_asset_authority_index(document),
        databases=load_mssql_default_database_index(document),
        content_sha256=digest,
    )


def require_mssql_registry_env_match(
    registry: ResolvedMssqlAssetRegistry,
    *,
    env: str,
) -> AssetUriIssue | None:
    """Fail closed when a caller-supplied snapshot does not match the requested env."""

    requested = (env or "").strip()
    if registry.env != requested:
        return AssetUriIssue(
            code=MSSQL_REGISTRY_ENVIRONMENT_MISMATCH,
            message=(f"MSSQL registry snapshot env {registry.env!r} does not match requested env {requested!r}"),
            path=registry.path,
        )
    return None


def load_mssql_registry_indexes(
    repo_root: Path,
    *,
    env: str = "dev",
    registry: ResolvedMssqlAssetRegistry | None = None,
) -> tuple[dict[str, MssqlAssetAuthority], dict[str, str]]:
    """Load MSSQL authority + default-database indexes from one resolved registry."""

    resolved = registry or resolve_mssql_asset_registry(repo_root, env=env)
    mismatch = require_mssql_registry_env_match(resolved, env=env)
    if mismatch is not None or not resolved.ok:
        return {}, {}
    return dict(resolved.authorities), dict(resolved.databases)


def _parse_registry_document(
    raw: bytes,
    *,
    path: str,
) -> tuple[Mapping[str, Any] | None, AssetUriIssue | None]:
    try:
        from dpone.manifest.bounded_yaml import BoundedYamlError, load_bounded_yaml
    except ImportError:  # pragma: no cover
        return None, AssetUriIssue(
            code=MSSQL_REGISTRY_PARSE_INVALID,
            message="bounded YAML loader is required to parse MSSQL connection registries",
            path=path,
        )
    try:
        payload = load_bounded_yaml(raw)
    except BoundedYamlError as exc:
        if exc.code == "invalid_utf8":
            message = "MSSQL connection registry must be valid UTF-8"
        elif exc.code == "duplicate_key":
            message = "MSSQL connection registry YAML contains duplicate keys"
        else:
            message = "MSSQL connection registry YAML is invalid"
        return None, AssetUriIssue(
            code=MSSQL_REGISTRY_PARSE_INVALID,
            message=message,
            path=path,
        )
    if not isinstance(payload, Mapping):
        return None, AssetUriIssue(
            code=MSSQL_REGISTRY_PARSE_INVALID,
            message="MSSQL connection registry must be one YAML mapping",
            path=path,
        )
    return payload, None


def _environment_invariant(
    document: Mapping[str, Any],
    *,
    requested_env: str,
    path: str,
) -> AssetUriIssue | None:
    if "environment" not in document:
        return None
    document_env = str(document.get("environment") or "").strip()
    if document_env != requested_env:
        return AssetUriIssue(
            code=MSSQL_REGISTRY_ENVIRONMENT_MISMATCH,
            message=(
                f"MSSQL registry document environment {document_env!r} does not match requested env {requested_env!r}"
            ),
            path=path,
        )
    return None


__all__ = [
    "MSSQL_REGISTRY_ENVIRONMENT_MISMATCH",
    "MSSQL_REGISTRY_PARSE_INVALID",
    "MSSQL_REGISTRY_SNAPSHOT_DRIFT",
    "ResolvedMssqlAssetRegistry",
    "connection_registry_path_for_env",
    "load_mssql_registry_indexes",
    "require_mssql_registry_env_match",
    "resolve_mssql_asset_registry",
]
