"""Reachability, retry, and acknowledgement rules for REST delivery states."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .rest_delivery_design_contract_types import (
    DesignContractIssue,
    as_mapping,
    as_sequence,
    contract_issue,
)

_AXES = ("execution", "remote_effect", "acknowledgement", "verification", "checkpoint")


def validate_state_machine_semantics(contract: Mapping[str, Any]) -> Iterable[DesignContractIssue]:
    """Validate legal tuples, graph closure, safe retry, and failure mappings."""

    state_machine = as_mapping(contract["state_machine"])
    axes = as_mapping(state_machine["axes"])
    legal = {
        str(name): tuple(str(value) for value in as_sequence(values))
        for name, values in as_mapping(state_machine["legal_tuples"]).items()
    }
    automatic = tuple(as_mapping(value) for value in as_sequence(state_machine["automatic_transitions"]))
    operator = tuple(as_mapping(value) for value in as_sequence(state_machine["operator_only_transitions"]))
    yield from _tuple_issues(axes, legal)
    yield from _transition_issues(state_machine, legal, automatic, operator)
    yield from _retry_issues(legal, automatic)
    yield from _failure_scenario_issues(contract, legal, automatic)
    yield from _reconciliation_issues(legal, operator)


def _tuple_issues(axes: Mapping[str, Any], legal: Mapping[str, tuple[str, ...]]) -> Iterable[DesignContractIssue]:
    if tuple(axes) != _AXES:
        yield contract_issue("state.axes_order", "$.state_machine.axes", f"Axes must be ordered as {_AXES!r}.")
    known = {axis: {str(value) for value in as_sequence(axes[axis])} for axis in _AXES}
    seen: set[tuple[str, ...]] = set()
    for name, values in legal.items():
        if values in seen:
            yield contract_issue(
                "state.duplicate_tuple", f"$.state_machine.legal_tuples.{name}", "Legal tuple is duplicated."
            )
        seen.add(values)
        for index, axis in enumerate(_AXES):
            if values[index] not in known[axis]:
                yield contract_issue(
                    "state.axis_value",
                    f"$.state_machine.legal_tuples.{name}[{index}]",
                    f"Unknown {axis} value.",
                )
        if values[4] in {"ELIGIBLE", "PROMOTED"} and not (values[1] == "FULLY_APPLIED" and values[3] == "PASSED"):
            yield contract_issue(
                "state.checkpoint_without_verified_effect",
                f"$.state_machine.legal_tuples.{name}",
                "Checkpoint eligibility requires FULLY_APPLIED and PASSED.",
            )
        if values[0] == "WAITING" and values[1] == "UNKNOWN":
            yield contract_issue(
                "state.unknown_waiting",
                f"$.state_machine.legal_tuples.{name}",
                "UNKNOWN cannot be ordinary WAITING.",
            )


def _transition_issues(
    state_machine: Mapping[str, Any],
    legal: Mapping[str, tuple[str, ...]],
    automatic: Sequence[Mapping[str, Any]],
    operator: Sequence[Mapping[str, Any]],
) -> Iterable[DesignContractIssue]:
    names = set(legal)
    all_transitions = (*automatic, *operator)
    edges = {(str(item["from"]), str(item["to"])) for item in all_transitions}
    if len(edges) != len(all_transitions):
        yield contract_issue(
            "state.duplicate_transition", "$.state_machine", "Transitions must be unique by from/to pair."
        )
    for index, transition in enumerate(all_transitions):
        for endpoint in ("from", "to"):
            if str(transition[endpoint]) not in names:
                yield contract_issue(
                    "state.unknown_transition_tuple",
                    f"$.state_machine.transitions[{index}].{endpoint}",
                    "Transition references an unknown tuple.",
                )
    reachable = _reachable(str(state_machine["initial_tuple"]), all_transitions)
    for name in sorted(names - reachable):
        yield contract_issue("state.unreachable", f"$.state_machine.legal_tuples.{name}", "Legal tuple is unreachable.")
    automatic_sources = {str(item["from"]) for item in automatic}
    all_sources = {str(item["from"]) for item in all_transitions}
    terminal = {str(value) for value in as_sequence(state_machine["terminal_for_automatic_execution"])}
    for name in sorted(terminal & automatic_sources):
        yield contract_issue(
            "state.terminal_has_automatic_exit",
            f"$.state_machine.terminal_for_automatic_execution.{name}",
            "Terminal tuple has an automatic outgoing transition.",
        )
    for name in sorted(names - terminal - all_sources):
        yield contract_issue(
            "state.dead_end", f"$.state_machine.legal_tuples.{name}", "Non-terminal tuple has no outgoing transition."
        )
    for index, transition in enumerate(operator):
        source = legal.get(str(transition["from"]))
        if source and source[0] == "FINALIZED" and "operator_approval" not in str(transition["cas"]):
            yield contract_issue(
                "state.operator_guard",
                f"$.state_machine.operator_only_transitions[{index}].cas",
                "Reopening FINALIZED requires operator approval in CAS guards.",
            )


def _retry_issues(
    legal: Mapping[str, tuple[str, ...]], automatic: Sequence[Mapping[str, Any]]
) -> Iterable[DesignContractIssue]:
    retry_wait = legal.get("retry_wait")
    if retry_wait is None or retry_wait[:3] != ("RETRY_WAIT", "NONE_PROVEN", "NOT_ACCEPTED"):
        yield contract_issue(
            "retry.wait_tuple",
            "$.state_machine.legal_tuples.retry_wait",
            "RETRY_WAIT must preserve NONE_PROVEN and NOT_ACCEPTED.",
        )
    required = {
        ("submitting", "retry_wait"): {"transport_proof_request_not_started", "retry_budget_remaining"},
        ("retry_wait", "submitting"): {"next_attempt_after_reached", "generation_fence_passed"},
    }
    for edge, guards in required.items():
        transition = _find_transition(automatic, *edge)
        if transition is None:
            yield contract_issue(
                "retry.missing_transition", "$.state_machine.automatic_transitions", f"Missing {edge[0]} -> {edge[1]}."
            )
        elif not guards <= {str(value) for value in as_sequence(transition.get("guards", []))}:
            yield contract_issue(
                "retry.missing_guard",
                "$.state_machine.automatic_transitions",
                f"{edge[0]} -> {edge[1]} lacks required guards.",
            )
    due = _find_transition(automatic, "retry_wait", "submitting")
    if due and "attempt_ordinal_incremented" not in as_sequence(due.get("effects", [])):
        yield contract_issue(
            "retry.attempt_ordinal",
            "$.state_machine.automatic_transitions",
            "Due retry must increment attempt_ordinal.",
        )


def _failure_scenario_issues(
    contract: Mapping[str, Any],
    legal: Mapping[str, tuple[str, ...]],
    automatic: Sequence[Mapping[str, Any]],
) -> Iterable[DesignContractIssue]:
    required = {
        "request_not_started_retry",
        "success_count_mismatch_partial",
        "success_count_missing_unknown",
        "expired_effect_unknown",
    }
    scenarios = as_mapping(contract["failure_scenarios"])
    for missing in sorted(required - set(scenarios)):
        yield contract_issue(
            "failure.missing_scenario", f"$.failure_scenarios.{missing}", "Required failure scenario is missing."
        )
    for name, raw in scenarios.items():
        scenario = as_mapping(raw)
        source, target = str(scenario["from"]), str(scenario["to"])
        if _find_transition(automatic, source, target) is None:
            yield contract_issue(
                "failure.missing_transition",
                f"$.failure_scenarios.{name}",
                "Scenario does not map to an automatic transition.",
            )
            continue
        target_tuple = legal.get(target)
        expected_ack = str(scenario["acknowledgement_preserved"])
        if target_tuple is None or target_tuple[2] != expected_ack:
            yield contract_issue(
                "failure.acknowledgement_lost",
                f"$.failure_scenarios.{name}",
                f"Target must preserve acknowledgement {expected_ack}.",
            )


def _reconciliation_issues(
    legal: Mapping[str, tuple[str, ...]],
    operator: Sequence[Mapping[str, Any]],
) -> Iterable[DesignContractIssue]:
    outgoing: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for transition in operator:
        outgoing[str(transition["from"])].append(transition)

    for source_name, source_tuple in legal.items():
        if source_name.startswith("reconciling_unknown_"):
            required_effects = {"NONE_PROVEN", "FULLY_APPLIED", "PARTIALLY_APPLIED", "UNKNOWN"}
        elif source_name.startswith("reconciling_partial_"):
            required_effects = {"NONE_PROVEN", "FULLY_APPLIED", "PARTIALLY_APPLIED"}
        else:
            continue
        transitions = outgoing[source_name]
        target_effects = {legal[str(item["to"])][1] for item in transitions}
        if not required_effects <= target_effects:
            yield contract_issue(
                "state.reconciliation_resolution_gap",
                f"$.state_machine.operator_only_transitions.{source_name}",
                "Reconciliation cannot prove every required effect outcome.",
            )
        for transition in transitions:
            target_name = str(transition["to"])
            target_tuple = legal[target_name]
            if target_tuple[2] != source_tuple[2]:
                yield contract_issue(
                    "state.reconciliation_acknowledgement_drift",
                    f"$.state_machine.operator_only_transitions.{source_name}.{target_name}",
                    "Effect reconciliation must preserve the observed acknowledgement.",
                )
            cas = str(transition["cas"])
            required_cas = {"revision", "reconciliation_id", "operator_approval"}
            required_cas.add("evidence_digest" if target_tuple[1] == "UNKNOWN" else "effect_proof")
            if not all(guard in cas for guard in required_cas):
                yield contract_issue(
                    "state.reconciliation_proof_guard",
                    f"$.state_machine.operator_only_transitions.{source_name}.{target_name}.cas",
                    "Reconciliation resolution lacks revision, proof, identity, or approval guard.",
                )


def _reachable(initial: str, transitions: Sequence[Mapping[str, Any]]) -> set[str]:
    graph: dict[str, set[str]] = defaultdict(set)
    for transition in transitions:
        graph[str(transition["from"])].add(str(transition["to"]))
    visited: set[str] = set()
    queue = deque([initial])
    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)
        queue.extend(sorted(graph[current] - visited))
    return visited


def _find_transition(transitions: Sequence[Mapping[str, Any]], source: str, target: str) -> Mapping[str, Any] | None:
    return next(
        (item for item in transitions if item.get("from") == source and item.get("to") == target),
        None,
    )


__all__ = ["validate_state_machine_semantics"]
