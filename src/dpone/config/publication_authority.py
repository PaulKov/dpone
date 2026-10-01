"""Pure configuration selection and normalized parity for publication authority.

These checks do not authenticate an endpoint, establish ownership, or provision
SQL objects. The runtime must separately admit the resolved deployment binding.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.clickhouse_cluster_admission import (
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)
from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding

PUBLICATION_OPTION = "publication_authority"


class PublicationSelectionError(ValueError):
    """Invalid public selection, without echoing input values or credentials."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"publication_authority: {reason}")


def select_publication_authority(config: Mapping[str, Any]) -> PublicationAuthorityBinding | None:
    """Parse the single canonical placement, including explicit null rejection."""
    source, sink = _mapping(config.get("source")), _mapping(config.get("sink"))
    for misplaced in (config, _mapping(config.get("options")), source, _mapping(source.get("options")), sink):
        if PUBLICATION_OPTION in misplaced:
            raise PublicationSelectionError("requires sink.options placement")
    return _selection(_mapping(sink.get("options")))


def validate_publication_configuration(config: Mapping[str, Any]) -> PublicationAuthorityBinding | None:
    """Reject unsupported authoring before packing or constructing clients."""
    binding = select_publication_authority(config)
    if binding is None:
        return None
    sink = _mapping(config.get("sink"))
    options, strategy, table, staging = (
        _mapping(sink.get(name)) for name in ("options", "strategy", "table", "staging")
    )
    decision = evaluate_clickhouse_cluster_admission(
        clickhouse_cluster_admission_input(
            sink_type=canonical_endpoint_type(str(sink.get("type") or "")),
            strategy_mode=str(strategy.get("mode") or "full_refresh"),
            max_source_bytes=strategy.get("max_source_bytes"),
            physical_design=_mapping(options.get("physical_design")),
            target_database=str(table.get("schema") or ""),
            staging_database=str(staging.get("schema") or "staging"),
        )
    )
    if not decision.selected or decision.mode != "cluster":
        raise PublicationSelectionError("bounded internal replicated full_refresh required")
    return binding


def normalized_publication_selection(options: Mapping[str, Any]) -> PublicationAuthorityBinding | None:
    """Require the two compiled copies to agree; source placement is forbidden."""
    source, sink = _mapping(options.get("source_options")), _mapping(options.get("sink_options"))
    if PUBLICATION_OPTION in source:
        raise PublicationSelectionError("source_options placement is forbidden")
    root_binding, sink_binding = _selection(options), _selection(sink)
    if root_binding != sink_binding:
        raise PublicationSelectionError("conflicting normalized selection")
    return root_binding


def _selection(options: Mapping[str, Any]) -> PublicationAuthorityBinding | None:
    if PUBLICATION_OPTION not in options:
        return None
    try:
        return PublicationAuthorityBinding.from_mapping(options[PUBLICATION_OPTION])
    except ValueError as exc:
        raise PublicationSelectionError("invalid binding fields") from exc


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
