"""Confined JSON reader for scheduler-verified dbt execution packs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.contracts.dbt_runtime import DbtPublishingError
from dpone.manifest.confined_files import read_confined_file


def execution_pack_payload(
    root: Path,
    relative_pack_path: str,
    *,
    max_bytes: int,
) -> dict[str, Any]:
    """Read one confined, duplicate-key-free execution-pack object."""

    try:
        raw = read_confined_file(root, relative_pack_path, max_bytes=max_bytes)
        payload = json.loads(
            raw,
            object_pairs_hook=unique_object,
            parse_constant=reject_json_constant,
        )
    except DbtPublishingError:
        raise
    except Exception as exc:
        raise DbtPublishingError(
            "DPONE_DBT_PACK_INVALID",
            "dbt execution pack is unavailable or invalid",
        ) from exc
    if not isinstance(payload, dict):
        raise DbtPublishingError(
            "DPONE_DBT_PACK_INVALID",
            "dbt execution pack must be an object",
        )
    return payload


def unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON object keys."""

    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def reject_json_constant(value: str) -> None:
    """Reject NaN and Infinity spellings outside canonical JSON."""

    raise ValueError(f"unsupported JSON constant: {value}")
