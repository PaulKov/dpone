from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_route_planner import NativeTransferRoutePlanner


class NativeTransferRouteRunGuard:
    """Evaluates route-level native-transfer blockers before runtime hydration."""

    def blockers(self, *, raw_config: Mapping[str, Any], manifest_dir: Path) -> tuple[str, ...]:
        source = raw_config.get("source", {}) if isinstance(raw_config.get("source"), Mapping) else {}
        sink = raw_config.get("sink", {}) if isinstance(raw_config.get("sink"), Mapping) else {}
        source_options = source.get("options", {}) if isinstance(source.get("options"), Mapping) else {}
        sink_options = sink.get("options", {}) if isinstance(sink.get("options"), Mapping) else {}
        execution = _native_execution(source_options if isinstance(source_options, Mapping) else {})
        policy = NativeTransferExecutionPolicy.from_mapping(dict(execution))
        certification = policy.certification
        if certification.artifact and not Path(certification.artifact).is_absolute():
            certification = type(certification)(
                mode=certification.mode,
                artifact=str((manifest_dir / certification.artifact).resolve()),
            )
        strategy = sink.get("strategy", {}) if isinstance(sink, Mapping) else {}
        decision = NativeTransferRoutePlanner().plan(
            source_type=canonical_endpoint_type(
                str(source.get("type", "postgres")) if isinstance(source, Mapping) else "postgres"
            ),
            sink_type=canonical_endpoint_type(
                str(sink.get("type", "bigquery")) if isinstance(sink, Mapping) else "bigquery"
            ),
            strategy=str(strategy.get("mode", "full_refresh")) if isinstance(strategy, Mapping) else "full_refresh",
            source_options=source_options if isinstance(source_options, Mapping) else {},
            sink_options=sink_options if isinstance(sink_options, Mapping) else {},
            execution_policy=policy,
            certification_policy=certification,
        )
        return decision.blockers


def _native_execution(source_options: Mapping[str, Any]) -> Mapping[str, Any]:
    native_transfer = source_options.get("native_transfer") if isinstance(source_options, Mapping) else {}
    execution = native_transfer.get("execution") if isinstance(native_transfer, Mapping) else {}
    return execution if isinstance(execution, Mapping) else {}


__all__ = ["NativeTransferRouteRunGuard"]
