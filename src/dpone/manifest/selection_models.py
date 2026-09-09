"""Immutable, orchestrator-neutral contracts for workload selection."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint

_SELECTION_STATE_SCHEMA = "dpone.selection-state.v1"
_SELECTION_METHODS = frozenset({"id", "tag", "domain", "owner", "source", "sink", "group", "state", "selector"})
_STATE_VALUES = frozenset({"modified", "new", "unmodified"})
_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@/-]{0,127}$")
_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_STATE_NODES = 1000
_STATE_FIELDS = frozenset({"schema", "state_fingerprint", "nodes"})
_STATE_NODE_FIELDS = frozenset({"id", "semantic_fingerprint", "graph_fingerprint"})


class SelectionError(ValueError):
    """A stable, data-safe selector contract violation."""

    def __init__(self, code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.context = dict(context or {})


@dataclass(frozen=True, slots=True)
class ParsedSelectionExpression:
    raw: str
    method: str
    value: str
    include_ancestors: bool
    include_descendants: bool


def parse_selection_expression(raw_expression: object) -> ParsedSelectionExpression:
    """Parse one bounded, non-executable workload selector expression."""

    if not isinstance(raw_expression, str):
        raise _invalid_expression("Selector expression must be a string.")
    raw = raw_expression.strip()
    if not raw or len(raw) > 256:
        raise _invalid_expression("Selector expression must contain 1 to 256 characters.")
    ancestors = raw.startswith("+")
    descendants = raw.endswith("+")
    body = raw[1:] if ancestors else raw
    body = body[:-1] if descendants else body
    if body.count(":") != 1:
        raise _invalid_expression("Selector expression must use method:value syntax.")
    method, value = body.split(":", 1)
    if method not in _SELECTION_METHODS:
        raise _invalid_expression(f"Unsupported selector method: {method or '<empty>'}.")
    if not _valid_selection_value(value):
        raise _invalid_expression("Selector value contains unsupported or unsafe characters.")
    if method == "state" and value not in _STATE_VALUES:
        raise _invalid_expression("State selector must be state:modified, state:new, or state:unmodified.")
    return ParsedSelectionExpression(raw, method, value, ancestors, descendants)


@dataclass(frozen=True, slots=True)
class SelectionNode:
    """One independently runnable workload in the normalized selection graph."""

    node_id: str
    source_path: str
    semantic_fingerprint: str
    domain: str | None = None
    owner: str | None = None
    tags: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    sinks: tuple[str, ...] = ()
    groups: tuple[str, ...] = ()
    graph_fingerprint: str = ""

    def with_graph_fingerprint(self, edges: tuple[tuple[str, str], ...]) -> SelectionNode:
        dependencies = tuple(upstream for upstream, downstream in edges if downstream == self.node_id)
        return replace(
            self,
            tags=tuple(sorted(set(self.tags))),
            sources=tuple(sorted(set(self.sources))),
            sinks=tuple(sorted(set(self.sinks))),
            groups=tuple(sorted(set(self.groups))),
            graph_fingerprint=canonical_fingerprint({"node_id": self.node_id, "direct_dependencies": dependencies}),
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "source": self.source_path,
            "semantic_fingerprint": self.semantic_fingerprint,
            "graph_fingerprint": self.graph_fingerprint,
            "domain": self.domain,
            "owner": self.owner,
            "tags": list(self.tags),
            "sources": list(self.sources),
            "sinks": list(self.sinks),
            "groups": list(self.groups),
        }


@dataclass(frozen=True, slots=True)
class SelectionGraph:
    """A validated DAG of workload nodes and explicit dependency edges."""

    nodes: tuple[SelectionNode, ...]
    edges: tuple[tuple[str, str], ...]
    catalog_fingerprint: str

    @classmethod
    def build(
        cls,
        *,
        nodes: tuple[SelectionNode, ...],
        edges: tuple[tuple[str, str], ...],
    ) -> SelectionGraph:
        ordered_edges = tuple(sorted(set(edges)))
        by_id: dict[str, SelectionNode] = {}
        for node in nodes:
            if node.node_id in by_id:
                raise SelectionError(
                    "DPONE_SELECTION_DUPLICATE_WORKLOAD",
                    f"Workload id is declared more than once: {node.node_id}",
                    context={"workload_id": node.node_id},
                )
            by_id[node.node_id] = node
        for upstream, downstream in ordered_edges:
            if upstream not in by_id or downstream not in by_id:
                raise SelectionError(
                    "DPONE_SELECTION_EDGE_UNKNOWN",
                    f"Selection edge references an unknown workload: {upstream} -> {downstream}",
                    context={"upstream": upstream, "downstream": downstream},
                )
        _require_acyclic(tuple(by_id), ordered_edges)
        normalized = tuple(by_id[node_id].with_graph_fingerprint(ordered_edges) for node_id in sorted(by_id))
        payload = {
            "nodes": [node.to_jsonable() for node in normalized],
            "edges": [{"upstream": up, "downstream": down} for up, down in ordered_edges],
        }
        return cls(normalized, ordered_edges, canonical_fingerprint(payload))

    def by_id(self) -> dict[str, SelectionNode]:
        return {node.node_id: node for node in self.nodes}


@dataclass(frozen=True, slots=True)
class NamedSelection:
    name: str
    select: tuple[str, ...]
    exclude: tuple[str, ...] = ()
    description: str | None = None


@dataclass(frozen=True, slots=True)
class SelectionStateNode:
    node_id: str
    semantic_fingerprint: str
    graph_fingerprint: str

    def to_jsonable(self) -> dict[str, str]:
        return {
            "id": self.node_id,
            "semantic_fingerprint": self.semantic_fingerprint,
            "graph_fingerprint": self.graph_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class SelectionState:
    nodes: tuple[SelectionStateNode, ...]
    state_fingerprint: str
    schema: str = _SELECTION_STATE_SCHEMA

    @classmethod
    def build(cls, nodes: Any) -> SelectionState:
        ordered = tuple(sorted(tuple(nodes), key=lambda item: item.node_id))
        if len({item.node_id for item in ordered}) != len(ordered):
            raise SelectionError("DPONE_SELECTION_STATE_INVALID", "Selection state contains duplicate node ids.")
        fingerprint = canonical_fingerprint(
            {"schema": _SELECTION_STATE_SCHEMA, "nodes": [item.to_jsonable() for item in ordered]}
        )
        return cls(ordered, fingerprint)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "state_fingerprint": self.state_fingerprint,
            "nodes": [node.to_jsonable() for node in self.nodes],
        }


@dataclass(frozen=True, slots=True)
class SelectionRequest:
    select: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    named: Mapping[str, NamedSelection] | None = None
    state: SelectionState | None = None
    max_selected: int = 1000


@dataclass(frozen=True, slots=True)
class SelectionReason:
    kind: str
    expression: str
    root: str
    path: tuple[str, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "expression": self.expression,
            "root": self.root,
            "path": list(self.path),
        }


@dataclass(frozen=True, slots=True)
class SelectionEntry:
    node: SelectionNode
    reasons: tuple[SelectionReason, ...]

    def to_jsonable(self) -> dict[str, Any]:
        payload = self.node.to_jsonable()
        payload["reasons"] = [reason.to_jsonable() for reason in self.reasons]
        return payload


@dataclass(frozen=True, slots=True)
class SelectionReport:
    selected: tuple[SelectionEntry, ...]
    excluded: tuple[SelectionEntry, ...]
    unmatched: tuple[str, ...]
    removed: tuple[str, ...]
    selection_fingerprint: str
    catalog_fingerprint: str
    state_fingerprint: str | None
    select_expressions: tuple[str, ...]
    exclude_expressions: tuple[str, ...]
    schema: str = "dpone.selection-report.v1"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "selection_fingerprint": self.selection_fingerprint,
            "catalog_fingerprint": self.catalog_fingerprint,
            "state_fingerprint": self.state_fingerprint,
            "expressions": {
                "select": list(self.select_expressions),
                "exclude": list(self.exclude_expressions),
            },
            "selected": [entry.to_jsonable() for entry in self.selected],
            "excluded": [entry.to_jsonable() for entry in self.excluded],
            "unmatched": list(self.unmatched),
            "removed": list(self.removed),
        }


def selection_report_fingerprint(
    *,
    catalog_fingerprint: str,
    state_fingerprint: str | None,
    select_expressions: tuple[str, ...],
    exclude_expressions: tuple[str, ...],
    selected: tuple[SelectionEntry, ...],
    excluded: tuple[SelectionEntry, ...],
) -> str:
    """Return the canonical identity for one fully explained selection."""

    payload = {
        "catalog_fingerprint": catalog_fingerprint,
        "state_fingerprint": state_fingerprint,
        "select": select_expressions,
        "exclude": exclude_expressions,
        "selected": [_selection_entry_identity(entry) for entry in selected],
        "excluded": [_selection_entry_identity(entry) for entry in excluded],
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def _selection_entry_identity(entry: SelectionEntry) -> dict[str, Any]:
    return {
        "id": entry.node.node_id,
        "reasons": [reason.to_jsonable() for reason in entry.reasons],
    }


def state_from_graph(graph: SelectionGraph) -> SelectionState:
    """Build canonical state from a normalized current graph."""

    return SelectionState.build(
        SelectionStateNode(
            node_id=node.node_id,
            semantic_fingerprint=node.semantic_fingerprint,
            graph_fingerprint=node.graph_fingerprint,
        )
        for node in graph.nodes
    )


def parse_selection_state(payload: object) -> SelectionState:
    """Validate and parse one externally supplied canonical state baseline."""

    if (
        not isinstance(payload, Mapping)
        or payload.get("schema") != _SELECTION_STATE_SCHEMA
        or set(payload) != _STATE_FIELDS
    ):
        raise SelectionError("DPONE_SELECTION_STATE_INVALID", "Selection state schema is invalid.")
    raw_nodes = payload.get("nodes")
    if not isinstance(raw_nodes, list) or len(raw_nodes) > _MAX_STATE_NODES:
        raise SelectionError("DPONE_SELECTION_STATE_INVALID", "Selection state nodes must be a list.")
    nodes: list[SelectionStateNode] = []
    for raw_node in raw_nodes:
        if not isinstance(raw_node, Mapping) or set(raw_node) != _STATE_NODE_FIELDS:
            raise SelectionError("DPONE_SELECTION_STATE_INVALID", "Selection state node must be an object.")
        nodes.append(
            SelectionStateNode(
                _required_state_id(raw_node, "id"),
                _required_state_fingerprint(raw_node, "semantic_fingerprint"),
                _required_state_fingerprint(raw_node, "graph_fingerprint"),
            )
        )
    state = SelectionState.build(nodes)
    if payload.get("state_fingerprint") != state.state_fingerprint:
        raise SelectionError("DPONE_SELECTION_STATE_INVALID", "Selection state fingerprint does not match content.")
    return state


def classify_state(graph: SelectionGraph, baseline: SelectionState) -> tuple[dict[str, str], tuple[str, ...]]:
    """Classify current nodes against one canonical baseline."""

    previous = {node.node_id: node for node in baseline.nodes}
    current_ids = {node.node_id for node in graph.nodes}
    statuses: dict[str, str] = {}
    for node in graph.nodes:
        old = previous.get(node.node_id)
        if old is None:
            statuses[node.node_id] = "new"
        elif old.semantic_fingerprint != node.semantic_fingerprint or old.graph_fingerprint != node.graph_fingerprint:
            statuses[node.node_id] = "modified"
        else:
            statuses[node.node_id] = "unmodified"
    return statuses, tuple(sorted(set(previous) - current_ids))


def _valid_selection_value(value: str) -> bool:
    if not _VALUE_PATTERN.fullmatch(value) or "://" in value or "\\" in value:
        return False
    return not any(part in {".", ".."} for part in PurePosixPath(value).parts)


def _invalid_expression(message: str) -> SelectionError:
    return SelectionError("DPONE_SELECTION_EXPRESSION_INVALID", message)


def _required_state_id(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value or len(value) > 256:
        raise SelectionError("DPONE_SELECTION_STATE_INVALID", f"Selection state node {key} is required.")
    return value


def _required_state_fingerprint(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value):
        raise SelectionError("DPONE_SELECTION_STATE_INVALID", f"Selection state node {key} is invalid.")
    return value


def _require_acyclic(node_ids: tuple[str, ...], edges: tuple[tuple[str, str], ...]) -> None:
    incoming = {node_id: 0 for node_id in node_ids}
    outgoing: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for upstream, downstream in edges:
        incoming[downstream] += 1
        outgoing[upstream].append(downstream)
    ready = sorted(node_id for node_id, count in incoming.items() if count == 0)
    visited: list[str] = []
    while ready:
        node_id = ready.pop(0)
        visited.append(node_id)
        for downstream in sorted(outgoing[node_id]):
            incoming[downstream] -= 1
            if incoming[downstream] == 0:
                ready.append(downstream)
                ready.sort()
    if len(visited) != len(node_ids):
        cycle_nodes = tuple(sorted(node_id for node_id, count in incoming.items() if count > 0))
        raise SelectionError(
            "DPONE_SELECTION_GRAPH_CYCLE",
            "Selection graph contains a dependency cycle.",
            context={"cycle_nodes": cycle_nodes[:20]},
        )


__all__ = [
    "NamedSelection",
    "ParsedSelectionExpression",
    "SelectionEntry",
    "SelectionError",
    "SelectionGraph",
    "SelectionNode",
    "SelectionReason",
    "SelectionReport",
    "SelectionRequest",
    "SelectionState",
    "SelectionStateNode",
    "classify_state",
    "parse_selection_expression",
    "parse_selection_state",
    "selection_report_fingerprint",
    "state_from_graph",
]
