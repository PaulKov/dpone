"""Closed wire contract for deployment-owned MSSQL Asset outlet projections.

Contract id: ``dpone.mssql-asset-outlet-projection.v1``.

Self-service summary
--------------------
Immutable dbt release packs keep logical ``asset_ref`` values. At the
environment deployment boundary dpone materializes one closed projection that
binds each logical ref through the binding-set to a registry connection and a
canonical AIP-60 ``mssql://`` URI. The Airflow provider resolves outlets only
from this projection — never by guessing registry authorities from the logical
alias alone.

Stable activation codes
-----------------------
- ``DPONE_MSSQL_ASSET_OUTLET_PROJECTION_REQUIRED`` — logical asset_ref present
  but projection absent/incomplete
- ``DPONE_MSSQL_ASSET_OUTLET_PROJECTION_INVALID`` — projection shape/digest/URI
  fails closed validation
- ``DPONE_MSSQL_ASSET_OUTLET_PROJECTION_MISMATCH`` — projection disagrees with
  deployment identity (env, registry_ref, binding_set_ref, recomputed digest,
  or deployment↔index mirror)
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.mssql_asset_ref_codec import asset_ref_sha256
from dpone_airflow_pack.mssql_outlet_projection_codes import (
    MSSQL_ASSET_OUTLET_PROJECTION_SCHEMA,
    PROJECTION_INVALID,
    PROJECTION_MISMATCH,
    PROJECTION_REQUIRED,
)
from dpone_airflow_pack.mssql_outlet_projection_entries import (
    MAX_PROJECTION_ENTRIES,
    parse_projection_entry,
)

_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PROJECTION_KEYS = frozenset(
    {
        "schema",
        "environment",
        "binding_set_ref",
        "connection_registry_ref",
        "entries",
        "projection_sha256",
    }
)


def projection_digest_payload(projection: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical payload hashed into ``projection_sha256``."""

    return {
        "schema": projection.get("schema"),
        "environment": projection.get("environment"),
        "binding_set_ref": projection.get("binding_set_ref"),
        "connection_registry_ref": projection.get("connection_registry_ref"),
        "entries": projection.get("entries"),
    }


def compute_projection_sha256(projection: Mapping[str, Any]) -> str:
    """Content-address the projection body (excluding ``projection_sha256``)."""

    raw = json.dumps(
        projection_digest_payload(projection),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def uri_by_asset_ref_sha256(projection: Mapping[str, Any]) -> dict[str, str]:
    """Build the provider lookup map ``asset_ref_sha256 → uri``."""

    entries = projection.get("entries")
    if not isinstance(entries, list):
        return {}
    result: dict[str, str] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            continue
        digest = str(item.get("asset_ref_sha256") or "").strip()
        uri = str(item.get("uri") or "").strip()
        if digest and uri:
            result[digest] = uri
    return result


def parse_mssql_asset_outlet_projection(
    value: object,
    *,
    path: Path | None = None,
    expected_environment: str | None = None,
    expected_binding_set_ref: str | None = None,
    expected_connection_registry_ref: str | None = None,
    require_present: bool = False,
) -> Mapping[str, str] | None:
    """Validate the closed projection and return the provider URI map."""

    if value is None:
        if require_present:
            raise InitFetchProviderError(
                PROJECTION_REQUIRED,
                "mssql_asset_outlet_projection is required for this deployment",
                path=_path_text(path),
            )
        return None
    document = _require_projection_document(value, path=path)
    environment = str(document.get("environment") or "").strip()
    if not environment:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql_asset_outlet_projection.environment is required",
            path=_path_text(path),
        )
    if expected_environment is not None and environment != expected_environment:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            "mssql_asset_outlet_projection.environment does not match deployment",
            path=_path_text(path),
        )
    binding_set_ref = str(document.get("binding_set_ref") or "").strip()
    connection_registry_ref = str(document.get("connection_registry_ref") or "").strip()
    if not _SHA256_RE.fullmatch(binding_set_ref) or not _SHA256_RE.fullmatch(connection_registry_ref):
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql_asset_outlet_projection refs must be canonical sha256 digests",
            path=_path_text(path),
        )
    if expected_binding_set_ref is not None and binding_set_ref != expected_binding_set_ref:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            "mssql_asset_outlet_projection.binding_set_ref does not match index",
            path=_path_text(path),
        )
    if expected_connection_registry_ref is not None and connection_registry_ref != expected_connection_registry_ref:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            "mssql_asset_outlet_projection.connection_registry_ref does not match index",
            path=_path_text(path),
        )
    uri_map = _parse_entries(document.get("entries"), path=path)
    expected_digest = compute_projection_sha256(document)
    actual_digest = str(document.get("projection_sha256") or "").strip()
    if actual_digest != expected_digest:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            "mssql_asset_outlet_projection.projection_sha256 does not match body",
            path=_path_text(path),
        )
    return uri_map


def require_projection_covers_asset_refs(
    *,
    asset_refs: Sequence[Mapping[str, Any]],
    uri_by_ref: Mapping[str, str] | None,
    path: Path | str | None = None,
    exact: bool = False,
) -> None:
    """Fail closed when logical refs are missing from the projection map."""

    path_text = path.as_posix() if isinstance(path, Path) else path
    if not asset_refs:
        return
    if uri_by_ref is None:
        raise InitFetchProviderError(
            PROJECTION_REQUIRED,
            "mssql logical asset_ref outlets require mssql_asset_outlet_projection",
            path=path_text,
        )
    expected: set[str] = set()
    for asset_ref in asset_refs:
        digest = asset_ref_sha256(asset_ref)
        if digest is None:
            raise InitFetchProviderError(
                PROJECTION_INVALID,
                "mssql logical asset_ref is incomplete",
                path=path_text,
            )
        expected.add(digest)
        if digest not in uri_by_ref:
            raise InitFetchProviderError(
                PROJECTION_REQUIRED,
                "mssql_asset_outlet_projection is missing a logical asset_ref",
                path=path_text,
            )
    if exact:
        extras = set(uri_by_ref) - expected
        if extras:
            raise InitFetchProviderError(
                PROJECTION_MISMATCH,
                "mssql_asset_outlet_projection contains asset_ref digests not present in pack outlets",
                path=path_text,
            )


def mirror_projections_or_raise(
    *,
    deployment_projection: object,
    index_projection: object,
    expected_environment: str | None = None,
    expected_binding_set_ref: str | None = None,
    expected_connection_registry_ref: str | None = None,
    path: Path | str | None = None,
) -> Mapping[str, str] | None:
    """Parse deployment + index projections separately, then require equality."""

    path_obj = path if isinstance(path, Path) else (Path(path) if path else None)
    path_text = path_obj.as_posix() if path_obj is not None else None
    if deployment_projection is None and index_projection is None:
        return None
    if deployment_projection is None or index_projection is None:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            "mssql_asset_outlet_projection must mirror between deployment and airflow-index",
            path=path_text,
        )
    deployment_map = parse_mssql_asset_outlet_projection(
        deployment_projection,
        path=path_obj,
        expected_environment=expected_environment,
        expected_binding_set_ref=expected_binding_set_ref,
        expected_connection_registry_ref=expected_connection_registry_ref,
        require_present=True,
    )
    index_map = parse_mssql_asset_outlet_projection(
        index_projection,
        path=path_obj,
        expected_environment=expected_environment,
        expected_binding_set_ref=expected_binding_set_ref,
        expected_connection_registry_ref=expected_connection_registry_ref,
        require_present=True,
    )
    if deployment_projection != index_projection or deployment_map != index_map:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            "mssql_asset_outlet_projection must mirror between deployment and airflow-index",
            path=path_text,
        )
    return index_map


def _require_projection_document(value: object, *, path: Path | None) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql_asset_outlet_projection must be a mapping",
            path=_path_text(path),
        )
    unknown = set(value) - _PROJECTION_KEYS
    if unknown:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection has unknown fields: {sorted(unknown)}",
            path=_path_text(path),
        )
    for field in _PROJECTION_KEYS:
        if field not in value:
            raise InitFetchProviderError(
                PROJECTION_INVALID,
                f"mssql_asset_outlet_projection.{field} is required",
                path=_path_text(path),
            )
    if str(value.get("schema") or "") != MSSQL_ASSET_OUTLET_PROJECTION_SCHEMA:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql_asset_outlet_projection.schema is invalid",
            path=_path_text(path),
        )
    return value


def _parse_entries(entries: object, *, path: Path | None) -> dict[str, str]:
    if not isinstance(entries, list) or not entries:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            "mssql_asset_outlet_projection.entries must be a non-empty array",
            path=_path_text(path),
        )
    if len(entries) > MAX_PROJECTION_ENTRIES:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries exceeds {MAX_PROJECTION_ENTRIES} items",
            path=_path_text(path),
        )
    uri_map: dict[str, str] = {}
    seen: set[str] = set()
    for index, item in enumerate(entries):
        digest, uri = parse_projection_entry(item, index=index, path=path)
        if digest in seen:
            raise InitFetchProviderError(
                PROJECTION_INVALID,
                f"mssql_asset_outlet_projection.entries[{index}] duplicates asset_ref_sha256",
                path=_path_text(path),
            )
        seen.add(digest)
        uri_map[digest] = uri
    return uri_map


def _path_text(path: Path | None) -> str | None:
    return path.as_posix() if path is not None else None


__all__ = [
    "MSSQL_ASSET_OUTLET_PROJECTION_SCHEMA",
    "PROJECTION_INVALID",
    "PROJECTION_MISMATCH",
    "PROJECTION_REQUIRED",
    "compute_projection_sha256",
    "mirror_projections_or_raise",
    "parse_mssql_asset_outlet_projection",
    "projection_digest_payload",
    "require_projection_covers_asset_refs",
    "uri_by_asset_ref_sha256",
]
