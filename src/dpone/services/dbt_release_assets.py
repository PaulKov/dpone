"""Deterministic non-executable assets included in one dbt release build."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.contracts.dbt_source_snapshot import LegacyDbtSourceSnapshot


def build_source_snapshot(
    *,
    project_bundle_sha256: str,
    manifest_sha256: str,
) -> dict[str, str]:
    """Bind the stable project and manifest snapshots without volatile provenance."""

    return LegacyDbtSourceSnapshot(project_bundle_sha256, manifest_sha256).to_dict()


def canonical_schema_files() -> dict[str, bytes]:
    """Return release-relative canonical dbt schema documents."""

    return {
        f"schemas/dbt/{contract_id}.schema.json": _json_bytes(schema)
        for contract_id, schema in sorted(dbt_schema_contracts().items())
    }


def canonical_schema_descriptors(files: Mapping[str, bytes]) -> list[dict[str, Any]]:
    """Describe canonical schema bytes for release-set v2."""

    return [
        {
            "id": path.removeprefix("schemas/dbt/").removesuffix(".schema.json"),
            "path": path,
            "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }
        for path, payload in sorted(files.items())
    ]


def source_snapshot_bytes(snapshot: Mapping[str, str]) -> bytes:
    return _json_bytes(snapshot)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n"
    ).encode()


__all__ = [
    "build_source_snapshot",
    "canonical_schema_descriptors",
    "canonical_schema_files",
    "source_snapshot_bytes",
]
