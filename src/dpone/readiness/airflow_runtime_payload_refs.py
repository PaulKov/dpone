"""Fail-closed checks for workload runtime_payload_ids vs release/index payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


class RuntimePayloadRefError(ValueError):
    """Workload runtime_payload_ids reference a missing payload identity."""

    def __init__(self, code: str, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.path = path


def dangling_runtime_payload_ids(
    *,
    workload_packs: Sequence[Mapping[str, Any]],
    runtime_payloads: Sequence[Mapping[str, Any]],
) -> tuple[tuple[str, str], ...]:
    """Return (workload_id, payload_id) pairs that are not present in payloads."""

    known = {
        str(item["id"])
        for item in runtime_payloads
        if isinstance(item, Mapping) and isinstance(item.get("id"), str) and item["id"]
    }
    dangling: list[tuple[str, str]] = []
    for pack in workload_packs:
        if not isinstance(pack, Mapping):
            continue
        workload_id = str(pack.get("id") or "")
        refs = pack.get("runtime_payload_ids")
        if refs is None:
            continue
        if not isinstance(refs, list):
            dangling.append((workload_id or "<unknown>", "<invalid-runtime_payload_ids>"))
            continue
        for ref in refs:
            if not isinstance(ref, str) or not ref or ref not in known:
                dangling.append((workload_id or "<unknown>", str(ref)))
    return tuple(dangling)


def require_runtime_payload_refs(
    *,
    workload_packs: Sequence[Mapping[str, Any]],
    runtime_payloads: Sequence[Mapping[str, Any]],
    code: str = "DPONE_RUNTIME_PAYLOAD_REFS_DANGLING",
) -> None:
    """Raise when any workload runtime_payload_ids entry is absent from payloads."""

    dangling = dangling_runtime_payload_ids(
        workload_packs=workload_packs,
        runtime_payloads=runtime_payloads,
    )
    if not dangling:
        return
    sample = ", ".join(f"{workload}:{payload}" for workload, payload in dangling[:8])
    more = "" if len(dangling) <= 8 else f" (+{len(dangling) - 8} more)"
    raise RuntimePayloadRefError(
        code,
        f"workload runtime_payload_ids reference missing runtime payloads: {sample}{more}",
        path=dangling[0][1],
    )


__all__ = [
    "RuntimePayloadRefError",
    "dangling_runtime_payload_ids",
    "require_runtime_payload_refs",
]
