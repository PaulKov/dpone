"""Canonical, explainable workload selection engine."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping

from dpone.manifest.selection_models import (
    NamedSelection,
    ParsedSelectionExpression,
    SelectionEntry,
    SelectionError,
    SelectionGraph,
    SelectionNode,
    SelectionReason,
    SelectionReport,
    SelectionRequest,
    SelectionState,
    SelectionStateNode,
    classify_state,
    parse_selection_expression,
    parse_selection_state,
    selection_report_fingerprint,
    state_from_graph,
)

_MAX_EXPRESSIONS = 1000
_MAX_NAMED_DEPTH = 8


class SelectionEngine:
    """Evaluate bounded selectors over a prevalidated workload graph."""

    def select(self, graph: SelectionGraph, request: SelectionRequest) -> SelectionReport:
        if request.max_selected <= 0:
            raise SelectionError("DPONE_SELECTION_LIMIT_EXCEEDED", "max_selected must be greater than zero.")
        if len(request.select) + len(request.exclude) > _MAX_EXPRESSIONS:
            raise SelectionError("DPONE_SELECTION_LIMIT_EXCEEDED", "Selector expression budget was exceeded.")
        nodes = graph.by_id()
        outgoing, incoming = _adjacency(graph)
        state_statuses, removed = self._state_context(graph, request)
        named = dict(request.named or {})
        include: dict[str, list[SelectionReason]] = {}
        unmatched: list[str] = []
        if request.select:
            for raw in _normalized_expressions(request.select):
                matched = self._evaluate_expression(
                    raw,
                    nodes=nodes,
                    outgoing=outgoing,
                    incoming=incoming,
                    named=named,
                    state_statuses=state_statuses,
                    trail=(),
                )
                if not matched:
                    unmatched.append(raw)
                _merge_reasons(include, matched)
        else:
            for node_id in sorted(nodes):
                include[node_id] = [SelectionReason("all_by_default", "*", node_id, (node_id,))]

        excluded: dict[str, list[SelectionReason]] = {}
        for raw in _normalized_expressions(request.exclude):
            matched = self._evaluate_expression(
                raw,
                nodes=nodes,
                outgoing=outgoing,
                incoming=incoming,
                named=named,
                state_statuses=state_statuses,
                trail=(),
            )
            if not matched:
                unmatched.append(raw)
            _merge_reasons(excluded, matched)

        selected_ids = tuple(sorted(set(include) - set(excluded)))
        excluded_ids = tuple(sorted(set(include).intersection(excluded)))
        if not selected_ids:
            raise SelectionError(
                "DPONE_SELECTION_EMPTY",
                "Selector expressions produced an empty workload set.",
                context={"unmatched": tuple(sorted(set(unmatched)))},
            )
        if len(selected_ids) > request.max_selected:
            raise SelectionError(
                "DPONE_SELECTION_LIMIT_EXCEEDED",
                f"Selected workload count {len(selected_ids)} exceeds limit {request.max_selected}.",
                context={"selected": len(selected_ids), "limit": request.max_selected},
            )
        selected_entries = tuple(
            SelectionEntry(nodes[node_id], _ordered_reasons(include[node_id])) for node_id in selected_ids
        )
        excluded_entries = tuple(
            SelectionEntry(
                nodes[node_id],
                _ordered_reasons((*include.get(node_id, ()), *excluded.get(node_id, ()))),
            )
            for node_id in excluded_ids
        )
        select_exprs = _normalized_expressions(request.select)
        exclude_exprs = _normalized_expressions(request.exclude)
        fingerprint = selection_report_fingerprint(
            catalog_fingerprint=graph.catalog_fingerprint,
            state_fingerprint=request.state.state_fingerprint if request.state else None,
            select_expressions=select_exprs,
            exclude_expressions=exclude_exprs,
            selected=selected_entries,
            excluded=excluded_entries,
        )
        return SelectionReport(
            selected=selected_entries,
            excluded=excluded_entries,
            unmatched=tuple(sorted(set(unmatched))),
            removed=removed,
            selection_fingerprint=fingerprint,
            catalog_fingerprint=graph.catalog_fingerprint,
            state_fingerprint=request.state.state_fingerprint if request.state else None,
            select_expressions=select_exprs,
            exclude_expressions=exclude_exprs,
        )

    def _evaluate_expression(
        self,
        raw: str,
        *,
        nodes: Mapping[str, SelectionNode],
        outgoing: Mapping[str, tuple[str, ...]],
        incoming: Mapping[str, tuple[str, ...]],
        named: Mapping[str, NamedSelection],
        state_statuses: Mapping[str, str],
        trail: tuple[str, ...],
    ) -> dict[str, list[SelectionReason]]:
        expression = parse_selection_expression(raw)
        roots = self._roots(
            expression,
            nodes=nodes,
            outgoing=outgoing,
            incoming=incoming,
            named=named,
            state_statuses=state_statuses,
            trail=trail,
        )
        result: dict[str, list[SelectionReason]] = {}
        direct_kind = (
            "named" if expression.method == "selector" else "state" if expression.method == "state" else "direct"
        )
        ordered_roots = tuple(sorted(roots))
        for root in ordered_roots:
            result.setdefault(root, []).append(SelectionReason(direct_kind, expression.raw, root, (root,)))
        if expression.include_ancestors:
            _expand(result, roots=ordered_roots, adjacency=incoming, kind="ancestor", expression=expression.raw)
        if expression.include_descendants:
            _expand(result, roots=ordered_roots, adjacency=outgoing, kind="descendant", expression=expression.raw)
        return result

    def _roots(
        self,
        expression: ParsedSelectionExpression,
        *,
        nodes: Mapping[str, SelectionNode],
        outgoing: Mapping[str, tuple[str, ...]],
        incoming: Mapping[str, tuple[str, ...]],
        named: Mapping[str, NamedSelection],
        state_statuses: Mapping[str, str],
        trail: tuple[str, ...],
    ) -> set[str]:
        if expression.method == "selector":
            return self._named_roots(
                expression.value,
                nodes=nodes,
                outgoing=outgoing,
                incoming=incoming,
                named=named,
                state_statuses=state_statuses,
                trail=trail,
            )
        if expression.method == "state":
            if not state_statuses:
                raise SelectionError("DPONE_SELECTION_STATE_REQUIRED", "A state selector requires a baseline.")
            return {node_id for node_id, status in state_statuses.items() if status == expression.value}
        return {node_id for node_id, node in nodes.items() if _matches(node, expression.method, expression.value)}

    def _named_roots(
        self,
        name: str,
        *,
        nodes: Mapping[str, SelectionNode],
        outgoing: Mapping[str, tuple[str, ...]],
        incoming: Mapping[str, tuple[str, ...]],
        named: Mapping[str, NamedSelection],
        state_statuses: Mapping[str, str],
        trail: tuple[str, ...],
    ) -> set[str]:
        if name in trail:
            raise SelectionError("DPONE_SELECTION_NAMED_CYCLE", f"Named selector cycle detected at {name}.")
        if len(trail) >= _MAX_NAMED_DEPTH:
            raise SelectionError("DPONE_SELECTION_LIMIT_EXCEEDED", "Named selector depth limit was exceeded.")
        definition = named.get(name)
        if definition is None:
            raise SelectionError("DPONE_SELECTION_NAMED_NOT_FOUND", f"Named selector was not found: {name}.")
        next_trail = (*trail, name)
        selected: set[str] = set()
        for raw in _normalized_expressions(definition.select):
            selected.update(
                self._evaluate_expression(
                    raw,
                    nodes=nodes,
                    outgoing=outgoing,
                    incoming=incoming,
                    named=named,
                    state_statuses=state_statuses,
                    trail=next_trail,
                )
            )
        excluded: set[str] = set()
        for raw in _normalized_expressions(definition.exclude):
            excluded.update(
                self._evaluate_expression(
                    raw,
                    nodes=nodes,
                    outgoing=outgoing,
                    incoming=incoming,
                    named=named,
                    state_statuses=state_statuses,
                    trail=next_trail,
                )
            )
        return selected - excluded

    @staticmethod
    def _state_context(
        graph: SelectionGraph,
        request: SelectionRequest,
    ) -> tuple[dict[str, str], tuple[str, ...]]:
        needs_state = any(
            parse_selection_expression(raw).method == "state" for raw in (*request.select, *request.exclude)
        )
        if request.state is None:
            if needs_state:
                raise SelectionError("DPONE_SELECTION_STATE_REQUIRED", "A state selector requires a baseline.")
            return {}, ()
        return classify_state(graph, request.state)


def _matches(node: SelectionNode, method: str, value: str) -> bool:
    if method == "id":
        return node.node_id == value
    if method == "domain":
        return node.domain == value
    if method == "owner":
        return node.owner == value
    values = {
        "tag": node.tags,
        "source": node.sources,
        "sink": node.sinks,
        "group": node.groups,
    }.get(method, ())
    return value in values


def _adjacency(graph: SelectionGraph) -> tuple[dict[str, tuple[str, ...]], dict[str, tuple[str, ...]]]:
    outgoing: dict[str, list[str]] = {node.node_id: [] for node in graph.nodes}
    incoming: dict[str, list[str]] = {node.node_id: [] for node in graph.nodes}
    for upstream, downstream in graph.edges:
        outgoing[upstream].append(downstream)
        incoming[downstream].append(upstream)
    return (
        {key: tuple(sorted(values)) for key, values in outgoing.items()},
        {key: tuple(sorted(values)) for key, values in incoming.items()},
    )


def _expand(
    result: dict[str, list[SelectionReason]],
    *,
    roots: tuple[str, ...],
    adjacency: Mapping[str, tuple[str, ...]],
    kind: str,
    expression: str,
) -> None:
    for root in roots:
        queue = deque((root,))
        paths: dict[str, tuple[str, ...]] = {root: (root,)}
        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, ()):
                if neighbor in paths:
                    continue
                path = (*paths[current], neighbor)
                paths[neighbor] = path
                result.setdefault(neighbor, []).append(SelectionReason(kind, expression, root, path))
                queue.append(neighbor)


def _merge_reasons(
    target: dict[str, list[SelectionReason]],
    source: Mapping[str, Iterable[SelectionReason]],
) -> None:
    for node_id, reasons in source.items():
        target.setdefault(node_id, []).extend(reasons)


def _ordered_reasons(reasons: Iterable[SelectionReason]) -> tuple[SelectionReason, ...]:
    unique = {(reason.kind, reason.expression, reason.root, reason.path): reason for reason in reasons}
    return tuple(unique[key] for key in sorted(unique))


def _normalized_expressions(expressions: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(str(expression).strip() for expression in expressions)))


__all__ = [
    "NamedSelection",
    "SelectionEngine",
    "SelectionError",
    "SelectionGraph",
    "SelectionNode",
    "SelectionReport",
    "SelectionRequest",
    "SelectionState",
    "SelectionStateNode",
    "parse_selection_expression",
    "parse_selection_state",
    "state_from_graph",
]
