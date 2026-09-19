"""Shared validation and evidence helpers for the external runtime coordinator."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, NoReturn


def inventory_digest(
    service: Any,
    members: tuple[str, ...],
    fallback: Callable[[object], str],
) -> str:
    provider = getattr(service, "inventory_digest", None)
    return str(provider()) if callable(provider) else fallback({"member_ids": members})


def receipt(service: Any, state: dict[str, Any], factory: Callable[..., Any]) -> Any:
    return factory(
        state,
        evidence_scope=str(service.evidence_scope),
        evidence_status=str(service.evidence_status),
    )


def compare_and_swap(
    service: Any,
    current: dict[str, Any] | None,
    desired: Mapping[str, Any],
    fail: Callable[..., NoReturn],
    preserved_error_type: type[Exception],
) -> dict[str, Any]:
    target_key = str(desired["target_key"])
    version = None if current is None else int(current["version"])
    try:
        return dict(service.compare_and_swap_authority(target_key, version, desired))
    except preserved_error_type:
        raise
    except Exception:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT", state=current)


def fail(
    service: Any,
    code: str,
    *,
    state: Mapping[str, Any] | None = None,
    member_ids: tuple[str, ...] = (),
    error_factory: Callable[..., Exception],
) -> NoReturn:
    evidence: dict[str, object] = {
        "evidence_scope": str(getattr(service, "evidence_scope", "runtime")),
        "evidence_status": "FAIL",
        "member_ids": list(member_ids or tuple(state.get("member_ids", ())) if state else member_ids),
    }
    if state is not None:
        evidence.update(
            target_key=state.get("target_key"),
            operation_id=state.get("operation_id"),
            phase=state.get("phase"),
        )
    raise error_factory(code, evidence=evidence)


__all__ = ["compare_and_swap", "fail", "inventory_digest", "receipt"]
