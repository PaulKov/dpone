"""Callable introspection helpers for governance coordinator dispatch."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.governance.quality_execution import QualityGateExecution


from inspect import Parameter, signature
from typing import Any


def accepts_keyword(callable_value: Any, keyword: str) -> bool:
    try:
        parameters = signature(callable_value).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(parameter.name == keyword or parameter.kind is Parameter.VAR_KEYWORD for parameter in parameters)


def invoke_with_quality_scope(
    callable_value: Any,
    *,
    quality_scope: Any | None,
    quality_execution: QualityGateExecution | None,
    **kwargs: Any,
) -> Any:
    if accepts_keyword(callable_value, "quality_scope"):
        kwargs["quality_scope"] = quality_scope
    if accepts_keyword(callable_value, "quality_execution"):
        kwargs["quality_execution"] = quality_execution
    return callable_value(**kwargs)


def invoke_with_target_guard(
    callable_value: Any,
    *,
    before_target_mutation: Any | None,
    quality_scope: Any | None,
    quality_execution: QualityGateExecution | None,
    **kwargs: Any,
) -> Any:
    if accepts_keyword(callable_value, "before_target_mutation"):
        kwargs["before_target_mutation"] = before_target_mutation
    elif before_target_mutation is not None:
        before_target_mutation()
    if accepts_keyword(callable_value, "quality_scope"):
        kwargs["quality_scope"] = quality_scope
    if accepts_keyword(callable_value, "quality_execution"):
        kwargs["quality_execution"] = quality_execution
    return callable_value(**kwargs)
