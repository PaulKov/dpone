"""Isolated staged-validation snapshots with identity-bound runtime clients."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

_RUNTIME_REFERENCE_OPTION_KEYS = frozenset({"_source_connector"})
_STAGED_CONFIG_ATTRIBUTES = (
    "staging_config",
    "finalization_config",
    "decoded_config",
)


def snapshot_staged_validation_values(*values: Any) -> tuple[Any, ...]:
    """Deep-copy validation semantics without copying live connector clients.

    ``LoadConfig.options["_source_connector"]`` is an injected runtime
    dependency, not declarative load policy. Its identity must remain stable
    while every surrounding config, handle, option, and metadata value is
    isolated from mutations after validation.
    """

    memo: dict[int, Any] = {}
    for value in values:
        _register_runtime_references(value, memo)
    return deepcopy(values, memo)


def _register_runtime_references(value: Any, memo: dict[int, Any]) -> None:
    _register_config_references(value, memo)
    for attribute in _STAGED_CONFIG_ATTRIBUTES:
        _register_config_references(getattr(value, attribute, None), memo)


def _register_config_references(config: Any, memo: dict[int, Any]) -> None:
    options = getattr(config, "options", None)
    if not isinstance(options, Mapping):
        return
    for key in _RUNTIME_REFERENCE_OPTION_KEYS:
        reference = options.get(key)
        if reference is not None:
            memo[id(reference)] = reference


__all__ = ["snapshot_staged_validation_values"]
