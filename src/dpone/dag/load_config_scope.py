"""Portable and legacy relation-scope parsing for DAG load configs."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any

from dpone.contracts.portable_relation_scope import PortableScopeContractError, parse_portable_relation_scope
from dpone.contracts.rolling_window import RollingWindowSpec
from dpone.dag.errors import DagConfigurationError

if TYPE_CHECKING:
    from dpone.dag.parse_trace import ParseTracer


def resolve_load_scopes(
    *,
    strategy_config: Mapping[str, Any],
    source_options: Mapping[str, Any],
    options: MutableMapping[str, Any],
    parse_tracer: ParseTracer | None,
) -> tuple[Any | None, Any]:
    """Parse one portable AST and retain legacy source scope only as an explicit option."""

    source_custom_predicate = source_options.get("custom_predicate")
    sink_custom_predicate = strategy_config.get("custom_predicate")
    portable_scope_raw = strategy_config.get("portable_scope")
    if "window" in strategy_config:
        if (
            strategy_config.get("mode") != "replace"
            or strategy_config.get("atomicity") != "target_atomic"
            or source_custom_predicate
            or sink_custom_predicate
            or portable_scope_raw is not None
        ):
            raise DagConfigurationError(
                "rolling_window_requires_atomic_replace: no additional custom or portable predicates"
            )
        try:
            options["rolling_window"] = RollingWindowSpec.from_mapping(strategy_config["window"]).to_dict()
        except ValueError as exc:
            raise DagConfigurationError(str(exc)) from exc
    elif "atomicity" in strategy_config:
        raise DagConfigurationError("rolling_window_required_for_strategy_atomicity")
    try:
        portable_scope = parse_portable_relation_scope(portable_scope_raw) if portable_scope_raw is not None else None
    except PortableScopeContractError as exc:
        raise DagConfigurationError(str(exc)) from exc
    if parse_tracer and portable_scope is not None:
        parse_tracer.record(
            kind="load_config.field",
            target="load_config.portable_scope",
            value=portable_scope.to_contract(),
            sources=("sink.strategy.portable_scope",),
            operation="parse_portable_scope_ast",
        )
    if source_custom_predicate:
        options["source_custom_predicate"] = source_custom_predicate
        if parse_tracer:
            parse_tracer.record(
                kind="option.key",
                target="load_config.options.source_custom_predicate",
                value=source_custom_predicate,
                sources=("source.options.custom_predicate",),
                operation="inject",
                details={"note": "source custom_predicate is injected as source_custom_predicate"},
            )
    return portable_scope, sink_custom_predicate
