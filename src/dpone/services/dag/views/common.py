from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from dpone.services.dag.load_context import DagCommandContext
from dpone.services.dag.path_view import group_consecutive

T = TypeVar("T")
K = TypeVar("K")


@dataclass(frozen=True, slots=True)
class DagViewMeta:
    """Common metadata attached to all DAG CLI view-models."""

    kind: str
    root: str
    base_path: str
    task_count: int
    options: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "kind": self.kind,
            "root": self.root,
            "base_path": self.base_path,
            "task_count": self.task_count,
        }
        if self.options:
            data["options"] = dict(self.options)
        return data


@dataclass(frozen=True, slots=True)
class PathSegmentView:
    """Grouped consecutive path edges with the same reason signature."""

    signature: str
    edge_count: int
    nodes: tuple[str, ...]
    primary_reason: dict[str, Any] | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "signature": self.signature,
            "edge_count": self.edge_count,
            "nodes": list(self.nodes),
            "primary_reason": self.primary_reason,
        }


def build_meta(kind: str, dag: DagCommandContext, *, options: Mapping[str, Any] | None = None) -> DagViewMeta:
    """Create view metadata from the loaded DAG command context."""

    clean_options: dict[str, Any] = {}
    for key, value in dict(options or {}).items():
        if value is None:
            continue
        clean_options[str(key)] = value

    return DagViewMeta(
        kind=kind,
        root=str(dag.root_path),
        base_path=str(dag.base_path),
        task_count=len(dag.nodes),
        options=clean_options,
    )


def build_path_segments(
    items: Sequence[T],
    *,
    key_fn: Callable[[T], K],
    signature_fn: Callable[[K], str],
    nodes_fn: Callable[[Sequence[T]], Sequence[str]],
    primary_reason_fn: Callable[[Sequence[T]], dict[str, Any] | None],
) -> tuple[PathSegmentView, ...]:
    """Build grouped path segments for UX-friendly path explanations."""

    if not items:
        return ()

    out: list[PathSegmentView] = []
    for key, seg in group_consecutive(items, key_fn=key_fn):
        out.append(
            PathSegmentView(
                signature=signature_fn(key),
                edge_count=len(seg),
                nodes=tuple(nodes_fn(seg)),
                primary_reason=primary_reason_fn(seg),
            )
        )
    return tuple(out)
