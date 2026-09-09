"""Fail closed when the opt-in window path cannot honor configured policies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.process_errors import ETLProcessError
from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot

_UNSUPPORTED_FIELDS = (
    "with_dedup",
    "dedup_expression",
    "reconciliation",
    "reconciliation_policy",
    "repair_authority_ref",
    "custom_predicate",
    "portable_scope",
    "micro_batch_commit",
    "only_new_rows",
    "partition",
    "overwrite_type",
)
_UNSUPPORTED_OPTIONS = (
    "backfill",
    "cdc",
    "diff",
    "scd2",
    "state",
    "schema_contract",
    "reconciliation",
    "source_custom_predicate",
    "custom_predicate",
    "portable_scope",
    "pre_sql",
    "post_sql",
    "hooks",
    "pre_hooks",
    "post_hooks",
    "with_dedup",
    "dedup_expression",
    "micro_batch_commit",
    "incremental_column",
    "lookback",
    "source_materialization",
    "strategy_intelligence",
)


def validate_window_admission(load_config: Any) -> None:
    """Validate before factories or source I/O; preserve legacy policy authority.

    The first window implementation supplies intrinsic staging reconciliation and
    injected fenced evidence/state callbacks. It cannot execute the legacy ETL
    processor's configurable quality, hook, repair or change-capture pipeline.
    Those combinations therefore fail explicitly instead of losing checks.
    """
    strategy = getattr(load_config, "load_strategy", "replace")
    if getattr(strategy, "value", strategy) != "replace":
        _unsupported("load_strategy")
    for field in _UNSUPPORTED_FIELDS:
        if _configured(getattr(load_config, field, None)):
            _unsupported(field)
    options = getattr(load_config, "options", None) or {}
    if not isinstance(options, Mapping):
        _unsupported("options")
    for scope in (options, options.get("source_options", {}), options.get("sink_options", {})):
        if not isinstance(scope, Mapping):
            _unsupported("endpoint_options")
        for key in _UNSUPPORTED_OPTIONS:
            if _configured(scope.get(key)):
                _unsupported(key)
    if not QualityExecutionSnapshot.from_load_config(load_config).is_inert():
        _unsupported("quality_or_acceptance")


def _configured(value: Any) -> bool:
    return value is not None and value is not False and value != {} and value != [] and value != ""


def reject_orphan_window_chunking(load_config: Any) -> None:
    """A chunking declaration cannot silently fall through to a legacy runner."""
    options = getattr(load_config, "options", None) or {}
    native = options.get("native_transfer") if isinstance(options, Mapping) else None
    execution = native.get("execution") if isinstance(native, Mapping) else None
    if isinstance(execution, Mapping) and "chunking" in execution:
        raise ETLProcessError("rolling_window_required_for_bounded_chunking")


def _unsupported(field: str) -> None:
    raise ETLProcessError(f"rolling_window_unsupported_policy: {field}")
