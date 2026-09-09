"""Canonical identity codec for logical MSSQL Asset references.

Shared by the build-plane projection writer and the Airflow provider so outlet
lookup cannot drift between ``|``-joined composites and digest keys.

Identity payload (RFC 8785-inspired JCS: sorted keys, compact separators):

```text
{
  "schema": "dpone.mssql-logical-asset-ref.v1",
  "engine": "mssql",
  "connection_ref": "<logical>",
  "database": "...",
  "schema_name": "...",
  "table": "..."
}
```

``schema_name`` is used instead of ``schema`` so the contract ``schema`` field
never collides with the MSSQL schema segment.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

LOGICAL_ASSET_REF_SCHEMA = "dpone.mssql-logical-asset-ref.v1"
_MSSQL_ENGINE = "mssql"


def canonical_logical_asset_ref_payload(asset_ref: Mapping[str, Any]) -> dict[str, str]:
    """Normalize one logical asset_ref into the digest payload shape."""

    engine = str(asset_ref.get("engine") or "").strip().lower()
    connection_ref = str(asset_ref.get("connection_ref") or "").strip()
    database = str(asset_ref.get("database") or "").strip()
    schema_name = str(asset_ref.get("schema") or asset_ref.get("schema_name") or "").strip()
    table = str(asset_ref.get("table") or "").strip()
    return {
        "schema": LOGICAL_ASSET_REF_SCHEMA,
        "engine": engine,
        "connection_ref": connection_ref,
        "database": database,
        "schema_name": schema_name,
        "table": table,
    }


def asset_ref_sha256(asset_ref: Mapping[str, Any]) -> str | None:
    """Return ``sha256:<hex>`` for a complete logical asset_ref, else ``None``."""

    payload = canonical_logical_asset_ref_payload(asset_ref)
    if payload["engine"] != _MSSQL_ENGINE:
        return None
    if not all(
        (
            payload["connection_ref"],
            payload["database"],
            payload["schema_name"],
            payload["table"],
        )
    ):
        return None
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def require_asset_ref_sha256(asset_ref: Mapping[str, Any]) -> str:
    """Return the digest or raise ``ValueError`` when the ref is incomplete."""

    digest = asset_ref_sha256(asset_ref)
    if digest is None:
        raise ValueError("mssql logical asset_ref is incomplete or not engine=mssql")
    return digest


__all__ = [
    "LOGICAL_ASSET_REF_SCHEMA",
    "asset_ref_sha256",
    "canonical_logical_asset_ref_payload",
    "require_asset_ref_sha256",
]
