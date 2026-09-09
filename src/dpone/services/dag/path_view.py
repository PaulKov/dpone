from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Any, TypeVar

T = TypeVar("T")
K = TypeVar("K")


def group_consecutive(items: Sequence[T], *, key_fn: Callable[[T], K]) -> list[tuple[K, list[T]]]:
    """Group consecutive items by a key.

    Unlike ``itertools.groupby`` this returns materialized lists and keeps
    stable keys, which is convenient for CLI rendering.
    """

    out: list[tuple[K, list[T]]] = []
    cur_key: Any = None
    cur: list[T] = []

    for item in items:
        k = key_fn(item)
        if cur and k != cur_key:
            out.append((cur_key, cur))
            cur = []
        cur_key = k
        cur.append(item)

    if cur:
        out.append((cur_key, cur))

    return out


def _get_reason_kind(reason: Any) -> str:
    if isinstance(reason, dict):
        return str(reason.get("kind") or "")
    return str(getattr(reason, "kind", ""))


def _get_reason_evidence(reason: Any) -> dict:
    if isinstance(reason, dict):
        ev = reason.get("evidence")
        return ev if isinstance(ev, dict) else {}
    ev = getattr(reason, "evidence", None)
    return ev if isinstance(ev, dict) else {}


def reason_signature(reason: Any) -> tuple[Any, ...]:
    """Stable signature for a single reason.

    Signature is designed for UX grouping of explanations along a path.

    Supported inputs:
    - EdgeReason dataclass
    - JSONable dict with keys {kind, evidence}
    """

    kind = _get_reason_kind(reason)
    ev = _get_reason_evidence(reason)

    if kind == "group_to_group":
        return (kind, str(ev.get("upstream_group") or ""), str(ev.get("downstream_group") or ""))
    if kind == "group_to_task":
        return (kind, str(ev.get("group") or ""))
    if kind == "depends_on_selector":
        return (kind, str(ev.get("file") or ""), str(ev.get("selector") or ""))
    if kind == "depends_on_file_all":
        return (kind, str(ev.get("file") or ""))
    if kind == "depends_on_task_id":
        return (kind, str(ev.get("task_id") or ""))
    if kind == "depends_on_group_string":
        return (kind, str(ev.get("group") or ""))
    if kind == "depends_on_stem_fallback":
        return (kind, str(ev.get("stem") or ""))

    return (kind,)


def reasons_signature(reasons: Iterable[Any]) -> tuple[tuple[Any, ...], ...]:
    """Signature for the full reasons list of an edge."""

    return tuple(reason_signature(r) for r in (reasons or ()))
