"""Entry-level validation for closed MSSQL outlet projections."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.mssql_asset_ref_codec import require_asset_ref_sha256
from dpone_airflow_pack.mssql_asset_uri_codec import (
    MAX_MSSQL_ASSET_URI_CHARS,
    parse_mssql_asset_uri,
    require_canonical_mssql_asset_uri,
)
from dpone_airflow_pack.mssql_outlet_projection_codes import (
    PROJECTION_INVALID,
    PROJECTION_MISMATCH,
)

# Keep in sync with docs/schemas/gitops/mssql-asset-outlet-projection.schema.json
MAX_PROJECTION_ENTRIES = 4096
MAX_WORKLOAD_IDS_PER_ENTRY = 1024

_ENTRY_KEYS = frozenset(
    {
        "asset_ref",
        "asset_ref_sha256",
        "registry_connection_ref",
        "uri",
        "resolved_binding",
        "workload_ids",
    }
)
_ASSET_REF_KEYS = frozenset(
    {
        "engine",
        "connection_ref",
        "database",
        "schema",
        "table",
    }
)
_RESOLVED_BINDING_KEYS = frozenset({"registry_connection_ref"})


def parse_projection_entry(item: object, *, index: int, path: Path | None) -> tuple[str, str]:
    """Validate one projection entry and return ``(asset_ref_sha256, uri)``."""

    if not isinstance(item, Mapping):
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}] must be a mapping",
            path=_path_text(path),
        )
    unknown = set(item) - _ENTRY_KEYS
    if unknown:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}] has unknown fields: {sorted(unknown)}",
            path=_path_text(path),
        )
    asset_ref = item.get("asset_ref")
    if not isinstance(asset_ref, Mapping):
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].asset_ref is required",
            path=_path_text(path),
        )
    unknown_ref = set(asset_ref) - _ASSET_REF_KEYS
    if unknown_ref:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].asset_ref has unknown fields: {sorted(unknown_ref)}",
            path=_path_text(path),
        )
    try:
        expected = require_asset_ref_sha256(asset_ref)
    except ValueError as exc:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].asset_ref is invalid",
            path=_path_text(path),
        ) from exc
    digest = str(item.get("asset_ref_sha256") or "").strip()
    if digest != expected:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            f"mssql_asset_outlet_projection.entries[{index}].asset_ref_sha256 mismatches asset_ref",
            path=_path_text(path),
        )
    _require_resolved_binding(item, index=index, path=path)
    _require_workload_ids(item, index=index, path=path)
    uri = str(item.get("uri") or "").strip()
    if len(uri) > MAX_MSSQL_ASSET_URI_CHARS:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].uri exceeds {MAX_MSSQL_ASSET_URI_CHARS} characters",
            path=_path_text(path),
        )
    try:
        canonical = require_canonical_mssql_asset_uri(uri)
        parsed = parse_mssql_asset_uri(canonical)
    except ValueError as exc:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].uri is not a canonical AIP-60 URI",
            path=_path_text(path),
        ) from exc
    # Relation equality: host/port/instance are deployment-authority only.
    if (
        parsed.database != str(asset_ref.get("database") or "").strip()
        or parsed.schema != str(asset_ref.get("schema") or "").strip()
        or parsed.table != str(asset_ref.get("table") or "").strip()
    ):
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            f"mssql_asset_outlet_projection.entries[{index}] asset_ref relation mismatches uri",
            path=_path_text(path),
        )
    return digest, canonical


def _require_resolved_binding(item: Mapping[str, object], *, index: int, path: Path | None) -> None:
    resolved = item.get("resolved_binding")
    if not isinstance(resolved, Mapping):
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].resolved_binding is required",
            path=_path_text(path),
        )
    unknown_binding = set(resolved) - _RESOLVED_BINDING_KEYS
    if unknown_binding:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].resolved_binding has unknown fields: "
            f"{sorted(unknown_binding)}",
            path=_path_text(path),
        )
    registry_ref = str(item.get("registry_connection_ref") or "").strip()
    bound = str(resolved.get("registry_connection_ref") or "").strip()
    if not registry_ref:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].registry_connection_ref is required",
            path=_path_text(path),
        )
    if bound != registry_ref:
        raise InitFetchProviderError(
            PROJECTION_MISMATCH,
            f"mssql_asset_outlet_projection.entries[{index}] resolved_binding disagrees",
            path=_path_text(path),
        )


def _require_workload_ids(item: Mapping[str, object], *, index: int, path: Path | None) -> None:
    workload_ids = item.get("workload_ids")
    if not isinstance(workload_ids, list) or not workload_ids:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].workload_ids must be a non-empty array",
            path=_path_text(path),
        )
    if len(workload_ids) > MAX_WORKLOAD_IDS_PER_ENTRY:
        raise InitFetchProviderError(
            PROJECTION_INVALID,
            f"mssql_asset_outlet_projection.entries[{index}].workload_ids exceeds {MAX_WORKLOAD_IDS_PER_ENTRY} items",
            path=_path_text(path),
        )
    seen_workloads: set[str] = set()
    for workload_index, workload_id in enumerate(workload_ids):
        text = str(workload_id or "").strip() if isinstance(workload_id, str) else ""
        if not text:
            raise InitFetchProviderError(
                PROJECTION_INVALID,
                f"mssql_asset_outlet_projection.entries[{index}].workload_ids[{workload_index}] is invalid",
                path=_path_text(path),
            )
        if text in seen_workloads:
            raise InitFetchProviderError(
                PROJECTION_INVALID,
                f"mssql_asset_outlet_projection.entries[{index}].workload_ids duplicates {text!r}",
                path=_path_text(path),
            )
        seen_workloads.add(text)


def _path_text(path: Path | None) -> str | None:
    return path.as_posix() if path is not None else None


__all__ = [
    "MAX_PROJECTION_ENTRIES",
    "MAX_WORKLOAD_IDS_PER_ENTRY",
    "parse_projection_entry",
]
