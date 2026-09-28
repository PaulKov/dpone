"""Pure, strict selection of durable quality replay before runtime hydration.

Selection changes composition, not dataset semantics. Validate before excluding
this single option from the versioned semantic identity; never exclude its parent
options map. Route admission remains owned by the existing cluster policy.
"""

from __future__ import annotations

from collections.abc import Mapping
from difflib import SequenceMatcher
from typing import Any

from dpone.contracts.clickhouse_cluster_admission import (
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)
from dpone.contracts.connector_declarations import canonical_endpoint_type

REPLAY_OPTION = "durable_quality_replay"


class ReplaySelectionError(ValueError):
    """Safe configuration failure with no credential or input-value disclosure."""

    code = "DPONE_REPLAY_QUALITY_EVIDENCE_UNSUPPORTED"
    blocks_committed_success = True

    def __init__(self, field: str) -> None:
        super().__init__(f"{self.code}: invalid or unsupported {field}")


def require_replay_boolean(value: object, *, sink_type: str) -> bool:
    """Protect direct factory callers before resolving their credentials."""
    if type(value) is not bool or (value and canonical_endpoint_type(sink_type) != "clickhouse"):
        raise ReplaySelectionError(f"sink.options.{REPLAY_OPTION}")
    return value


def replay_selection(options: object, *, sink_type: str) -> bool:
    """Read the canonical sink option without coercion or invented defaults."""
    values = _mapping(options)
    _reject_aliases(values)
    return require_replay_boolean(values.get(REPLAY_OPTION, False), sink_type=sink_type)


def validate_replay_configuration(config: Mapping[str, Any]) -> bool:
    """Validate authoring and static route before clients or state are built."""
    source, sink = _mapping(config.get("source")), _mapping(config.get("sink"))
    for misplaced in (config, _mapping(config.get("options")), source, _mapping(source.get("options")), sink):
        _reject_aliases(misplaced)
        if REPLAY_OPTION in misplaced:
            raise ReplaySelectionError(f"sink.options.{REPLAY_OPTION} placement")
    options = _mapping(sink.get("options"))
    selected = replay_selection(options, sink_type=str(sink.get("type") or ""))
    if not selected:
        return False
    strategy, table, staging = (_mapping(sink.get(name)) for name in ("strategy", "table", "staging"))
    decision = evaluate_clickhouse_cluster_admission(
        clickhouse_cluster_admission_input(
            sink_type=canonical_endpoint_type(str(sink.get("type") or "")),
            strategy_mode=str(strategy.get("mode") or ""),
            max_source_bytes=strategy.get("max_source_bytes"),
            physical_design=_mapping(options.get("physical_design")),
            target_database=str(table.get("schema") or ""),
            staging_database=str(staging.get("schema") or ""),
        )
    )
    if not decision.selected or decision.mode != "cluster":
        raise ReplaySelectionError("internal replicated cluster full_refresh route")
    return True


def validate_normalized_replay_selection(options: Mapping[str, Any]) -> None:
    """Reject forged/misplaced normalized copies before identity projection."""
    source, sink = _mapping(options.get("source_options")), _mapping(options.get("sink_options"))
    if REPLAY_OPTION in source:
        raise ReplaySelectionError("source_options")
    for values in (options, source, sink):
        _reject_aliases(values)
        if REPLAY_OPTION in values and type(values[REPLAY_OPTION]) is not bool:
            raise ReplaySelectionError(REPLAY_OPTION)
    if REPLAY_OPTION in options and REPLAY_OPTION in sink and options[REPLAY_OPTION] != sink[REPLAY_OPTION]:
        raise ReplaySelectionError("conflicting normalized replay selection")


def _reject_aliases(values: Mapping[str, Any]) -> None:
    # Unknown near-spellings must not silently leave replay disabled, including
    # open flow-option objects. Existing unrelated extension options stay intact.
    if any(
        isinstance(key, str) and key != REPLAY_OPTION and SequenceMatcher(None, key, REPLAY_OPTION).ratio() >= 0.85
        for key in values
    ):
        raise ReplaySelectionError(f"sink.options.{REPLAY_OPTION} spelling")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
