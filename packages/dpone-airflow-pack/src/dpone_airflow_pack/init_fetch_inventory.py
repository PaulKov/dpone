"""Strict runtime-payload and workload-pack inventory parsing."""

from __future__ import annotations

from pathlib import Path

from dpone_airflow_pack.init_fetch_contract import ExactRuntimePayload, ExactWorkloadPack
from dpone_airflow_pack.init_fetch_validation import (
    cache_ref,
    digest,
    exact_mapping,
    execution_token,
    field_invalid,
    positive_integer,
    require_cache_prefix,
    required_text,
)

_RUNTIME_PAYLOAD_KINDS = frozenset(
    {
        "dbt_project_bundle",
        "dbt_manifest",
        "dbt_selection_lock",
    }
)
_MAX_RUNTIME_PAYLOADS = 64


def parse_workload_packs(
    value: object,
    *,
    release_id: str,
    runtime_payload_ids: frozenset[str],
    path: Path | None,
) -> tuple[ExactWorkloadPack, ...]:
    """Parse the exact workload inventory for one pinned release."""

    if not isinstance(value, list) or not value:
        raise field_invalid(
            "workload_packs must be a non-empty list for strict init_fetch",
            path,
        )
    result: list[ExactWorkloadPack] = []
    seen: set[str] = set()
    release_prefix = ("releases", release_id.replace(":", "-"))
    for raw in value:
        item = exact_mapping(
            raw,
            "workload_packs[]",
            frozenset(
                {
                    "id",
                    "artifact_ref",
                    "sha256",
                    "bytes",
                    "pack_fingerprint",
                    "runtime_payload_ids",
                }
            ),
            optional=frozenset({"runtime_payload_ids"}),
            path=path,
        )
        workload_id = execution_token(item["id"], "workload_packs[].id", path)
        if workload_id in seen:
            raise field_invalid("workload_packs contains a duplicate id", path)
        seen.add(workload_id)
        artifact_ref = cache_ref(item["artifact_ref"], "workload_packs[].artifact_ref", path)
        selected_payload_ids = _runtime_payload_id_list(
            item.get("runtime_payload_ids", []),
            known_ids=runtime_payload_ids,
            path=path,
        )
        require_cache_prefix(artifact_ref, release_prefix, "workload pack", path)
        result.append(
            ExactWorkloadPack(
                id=workload_id,
                artifact_ref=artifact_ref,
                sha256=digest(item["sha256"], "workload_packs[].sha256", path),
                bytes=positive_integer(item["bytes"], "workload_packs[].bytes", path),
                pack_fingerprint=digest(
                    item["pack_fingerprint"],
                    "workload_packs[].pack_fingerprint",
                    path,
                ),
                runtime_payload_ids=selected_payload_ids,
            )
        )
    return tuple(sorted(result, key=lambda item: item.id))


def parse_runtime_payloads(
    value: object,
    *,
    release_id: str,
    path: Path | None,
) -> tuple[ExactRuntimePayload, ...]:
    """Parse the bounded release-owned runtime payload inventory."""

    if not isinstance(value, list) or len(value) > _MAX_RUNTIME_PAYLOADS:
        raise field_invalid(
            "runtime_payloads must be an array with at most 64 entries",
            path,
        )
    release_prefix = ("releases", release_id.replace(":", "-"))
    result: list[ExactRuntimePayload] = []
    seen: set[str] = set()
    for raw in value:
        item = exact_mapping(
            raw,
            "runtime_payloads[]",
            frozenset(
                {
                    "id",
                    "kind",
                    "artifact_ref",
                    "sha256",
                    "bytes",
                    "media_type",
                }
            ),
            path=path,
        )
        payload_id = execution_token(item["id"], "runtime_payloads[].id", path)
        if payload_id in seen:
            raise field_invalid("runtime_payloads contains a duplicate id", path)
        seen.add(payload_id)
        kind = required_text(item, "kind", path)
        if kind not in _RUNTIME_PAYLOAD_KINDS:
            raise field_invalid("runtime_payloads[].kind is unsupported", path)
        artifact_ref = cache_ref(item["artifact_ref"], "runtime_payloads[].artifact_ref", path)
        require_cache_prefix(artifact_ref, release_prefix, "runtime payload", path)
        media_type = required_text(item, "media_type", path)
        if len(media_type) > 200:
            raise field_invalid("runtime_payloads[].media_type is too long", path)
        result.append(
            ExactRuntimePayload(
                id=payload_id,
                kind=kind,
                artifact_ref=artifact_ref,
                sha256=digest(item["sha256"], "runtime_payloads[].sha256", path),
                bytes=positive_integer(item["bytes"], "runtime_payloads[].bytes", path),
                media_type=media_type,
            )
        )
    return tuple(sorted(result, key=lambda item: item.id))


def _runtime_payload_id_list(
    value: object,
    *,
    known_ids: frozenset[str],
    path: Path | None,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise field_invalid("workload_packs[].runtime_payload_ids must be an array", path)
    result = tuple(execution_token(item, "workload_packs[].runtime_payload_ids[]", path) for item in value)
    if len(result) != len(set(result)):
        raise field_invalid("workload_packs[].runtime_payload_ids contains duplicates", path)
    if set(result) - known_ids:
        raise field_invalid("workload references an absent runtime payload", path)
    return tuple(sorted(result))


__all__ = ["parse_runtime_payloads", "parse_workload_packs"]
